/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package drf

import (
	"math"

	"github.com/golang/glog"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api/helpers"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
)

var shareDelta = 0.000001

type drfAttr struct {
	share            float64
	dominantResource string
	allocated        *api.Resource
}

type drfPlugin struct {
	totalResource *api.Resource

	// Key is Job ID
	jobOpts map[api.JobID]*drfAttr

	// map[namespaceName]->attr
	namespaceOpts map[string]*drfAttr

	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

// New return drf plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &drfPlugin{
		totalResource:   api.EmptyResource(),
		jobOpts:         map[api.JobID]*drfAttr{},
		namespaceOpts:   map[string]*drfAttr{},
		pluginArguments: arguments,
	}
}

func (drf *drfPlugin) Name() string {
	return "drf"
}

func (drf *drfPlugin) OnSessionOpen(ssn *framework.Session) {
	// Prepare scheduling data for this session.
	for _, n := range ssn.Nodes {
		drf.totalResource.Add(n.Allocatable)
	}

	namespaceOrderEnabled := ssn.NamespaceOrderEnabledInPlugin(drf.Name())

	for _, job := range ssn.Jobs {
		attr := &drfAttr{
			allocated: api.EmptyResource(),
		}

		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
				}
			}
		}

		// Calculate the init share of Job
		drf.updateShare(attr)

		drf.jobOpts[job.UID] = attr

		if namespaceOrderEnabled {
			nsOpts, found := drf.namespaceOpts[job.Namespace]
			if !found {
				nsOpts = &drfAttr{
					allocated: api.EmptyResource(),
				}
				drf.namespaceOpts[job.Namespace] = nsOpts
			}
			// all task in job should have the same namespace with job
			nsOpts.allocated.Add(attr.allocated)
			drf.updateShare(nsOpts)
		}
	}

	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) []*api.TaskInfo {
		var victims []*api.TaskInfo

		addVictim := func(candidate *api.TaskInfo) {
			victims = append(victims, candidate)
		}

		latt := drf.jobOpts[preemptor.Job]
		lalloc := latt.allocated.Clone().Add(preemptor.Resreq)
		ls := drf.calculateShare(lalloc, drf.totalResource)

		var lNsShare float64
		if namespaceOrderEnabled {
			lWeight := ssn.NamespaceInfo[preemptor.Namespace].GetWeight()
			lNsAtt := drf.namespaceOpts[preemptor.Namespace]
			lNsAlloc := lNsAtt.allocated.Clone().Add(preemptor.Resreq)
			lNsShare = drf.calculateShare(lNsAlloc, drf.totalResource) / float64(lWeight)
		}

		allocations := map[api.JobID]*api.Resource{}
		namespaceAllocation := map[string]*api.Resource{}

		for _, preemptee := range preemptees {
			if namespaceOrderEnabled && preemptor.Namespace != preemptee.Namespace {
				nsAllocation, found := namespaceAllocation[preemptee.Namespace]
				if !found {
					rNsAtt := drf.namespaceOpts[preemptee.Namespace]
					nsAllocation = rNsAtt.allocated.Clone()
					namespaceAllocation[preemptee.Namespace] = nsAllocation
				}
				rWeight := ssn.NamespaceInfo[preemptee.Namespace].GetWeight()
				rNsAlloc := nsAllocation.Sub(preemptee.Resreq)
				rNsShare := drf.calculateShare(rNsAlloc, drf.totalResource) / float64(rWeight)

				// equal namespace order leads to judgement of jobOrder
				if lNsShare < rNsShare {
					addVictim(preemptee)
				}
				if lNsShare-rNsShare > shareDelta {
					continue
				}
			}

			if _, found := allocations[preemptee.Job]; !found {
				ratt := drf.jobOpts[preemptee.Job]
				allocations[preemptee.Job] = ratt.allocated.Clone()
			}
			ralloc := allocations[preemptee.Job].Sub(preemptee.Resreq)
			rs := drf.calculateShare(ralloc, drf.totalResource)

			if ls < rs || math.Abs(ls-rs) <= shareDelta {
				addVictim(preemptee)
			}
		}

		glog.V(4).Infof("Victims from DRF plugins are %+v", victims)

		return victims
	}

	ssn.AddPreemptableFn(drf.Name(), preemptableFn)

	jobOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		glog.V(4).Infof("DRF JobOrderFn: <%v/%v> share state: %f, <%v/%v> share state: %f",
			lv.Namespace, lv.Name, drf.jobOpts[lv.UID].share, rv.Namespace, rv.Name, drf.jobOpts[rv.UID].share)

		if drf.jobOpts[lv.UID].share == drf.jobOpts[rv.UID].share {
			return 0
		}

		if drf.jobOpts[lv.UID].share < drf.jobOpts[rv.UID].share {
			return -1
		}

		return 1
	}

	ssn.AddJobOrderFn(drf.Name(), jobOrderFn)

	namespaceOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(string)
		rv := r.(string)

		lOpt := drf.namespaceOpts[lv]
		rOpt := drf.namespaceOpts[rv]

		lWeight := ssn.NamespaceInfo[lv].GetWeight()
		rWeight := ssn.NamespaceInfo[rv].GetWeight()

		glog.V(4).Infof("DRF NamespaceOrderFn: <%v> share state: %f, weight %v, <%v> share state: %f, weight %v",
			lv, lOpt.share, lWeight, rv, rOpt.share, rWeight)

		lWeightedShare := lOpt.share / float64(lWeight)
		rWeightedShare := rOpt.share / float64(rWeight)

		if lWeightedShare == rWeightedShare {
			return 0
		}

		if lWeightedShare < rWeightedShare {
			return -1
		}

		return 1
	}

	if namespaceOrderEnabled {
		ssn.AddNamespaceOrderFn(drf.Name(), namespaceOrderFn)
	}

	// Register event handlers.
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			attr := drf.jobOpts[event.Task.Job]
			attr.allocated.Add(event.Task.Resreq)

			drf.updateShare(attr)

			nsShare := -1.0
			if namespaceOrderEnabled {
				nsOpt := drf.namespaceOpts[event.Task.Namespace]
				nsOpt.allocated.Add(event.Task.Resreq)

				drf.updateShare(nsOpt)
				nsShare = nsOpt.share
			}

			glog.V(4).Infof("DRF AllocateFunc: task <%v/%v>, resreq <%v>,  share <%v>, namespace share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share, nsShare)
		},
		DeallocateFunc: func(event *framework.Event) {
			attr := drf.jobOpts[event.Task.Job]
			attr.allocated.Sub(event.Task.Resreq)

			drf.updateShare(attr)

			nsShare := -1.0
			if namespaceOrderEnabled {
				nsOpt := drf.namespaceOpts[event.Task.Namespace]
				nsOpt.allocated.Sub(event.Task.Resreq)

				drf.updateShare(nsOpt)
				nsShare = nsOpt.share
			}

			glog.V(4).Infof("DRF EvictFunc: task <%v/%v>, resreq <%v>,  share <%v>, namespace share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share, nsShare)
		},
	})
}

func (drf *drfPlugin) updateShare(attr *drfAttr) {
	attr.share = drf.calculateShare(attr.allocated, drf.totalResource)
}

func (drf *drfPlugin) calculateShare(allocated, totalResource *api.Resource) float64 {
	res := float64(0)
	for _, rn := range totalResource.ResourceNames() {
		share := helpers.Share(allocated.Get(rn), totalResource.Get(rn))
		if share > res {
			res = share
		}
	}

	return res
}

func (drf *drfPlugin) OnSessionClose(session *framework.Session) {
	// Clean schedule data.
	drf.totalResource = api.EmptyResource()
	drf.jobOpts = map[api.JobID]*drfAttr{}
}
