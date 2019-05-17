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

package backfill

import (
	"sync"

	"github.com/kubernetes-sigs/kube-batch/pkg/apis/scheduling/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"

	"github.com/golang/glog"

	"k8s.io/client-go/util/workqueue"
)

type backfillAction struct {
	ssn *framework.Session

	arguments framework.Arguments
}

type victimsOfBackFill struct {
	Tasks []*api.TaskInfo
}

func New(args framework.Arguments) framework.Action {
	return &backfillAction{
		arguments: args,
	}
}

func (bf *backfillAction) Name() string {
	return "backfill"
}

func (bf *backfillAction) Initialize() {}

func (bf *backfillAction) Execute(ssn *framework.Session) {
	glog.V(3).Infof("Enter BackFill ...")
	defer glog.V(3).Infof("Leaving BackFill ...")

	enableBackFillNonBestEffortPods := false
	bf.arguments.GetBool(&enableBackFillNonBestEffortPods, conf.EnableBackFillNonBestEffortPods)
	if enableBackFillNonBestEffortPods {
		glog.V(3).Infof("BackFill for non-BestEffort pods is enabled")
	} else {
		glog.V(3).Infof("BackFill for non-BestEffort pods is disabled")
	}

	// jobRobberMap is used to store queue ID to potential jobs who can backFill resources of other jobs.
	jobRobberMap := map[api.QueueID]*util.PriorityQueue{}
	queues := map[api.QueueID]*api.QueueInfo{}
	for _, job := range ssn.Jobs {
		if job.PodGroup.Status.Phase == v1alpha1.PodGroupPending {
			continue
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			glog.V(4).Infof("Job <%s/%s> Queue <%s> skip backfill, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		queue, found := ssn.Queues[job.Queue]
		if !found {
			glog.Warningf("Queue of job <%s:%s> not found in cache, skip it", job.Namespace, job.Name)
			continue
		}

		if _, found := queues[queue.UID]; !found {
			glog.V(3).Infof("Add queue <%s:%s> into queue map", queue.Name, queue.UID)
			queues[queue.UID] = queue
		}

		if _, found := jobRobberMap[job.Queue]; !found {
			glog.V(3).Infof("Add queue <%s> into job map", job.Queue)
			jobRobberMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
		}

		if len(job.TaskStatusIndex[api.Pending]) > 0 {
			glog.V(3).Infof("Add job <%s:%s> into job map", job.Namespace, job.Name)
			jobRobberMap[job.Queue].Push(job)
		}
	}

	for _, q := range queues {
		for {
			jobRobbers := jobRobberMap[q.UID]
			if jobRobbers == nil || jobRobbers.Empty() {
				break
			}

			jr := jobRobbers.Pop().(*api.JobInfo)

			// taskRobberMap is used to store ID of job with potential backFill ability to its' non-bestEffortPendingTasks.
			taskRobberMap := map[api.JobID]*util.PriorityQueue{}

			// As long as the job contains a task with pending status
			// it needs to be judged whether it has the potential to backFill other jobs.
			if tasks := jr.TaskStatusIndex[api.Pending]; len(tasks) > 0 {
				taskRobberMap[jr.UID] = util.NewPriorityQueue(ssn.TaskOrderFn)
				for _, task := range tasks {
					// If task is best effort, directly assign node to it.
					if task.InitResreq.IsEmpty() {
						allocateBestEffortPod(ssn, task, jr)
					} else {
						glog.V(3).Infof("Add task <%s:%s> into task map", task.Namespace, task.Name)
						taskRobberMap[jr.UID].Push(task)
					}
				}
			} else {
				continue
			}

			if !enableBackFillNonBestEffortPods {
				continue
			}

			taskRobbers := taskRobberMap[jr.UID]
			if taskRobbers.Empty() {
				continue
			}

			if !ssn.JobEligibleToBackFillOthers(jr) {
				glog.V(3).Infof("Job <%s:%s> can not backFill resources of other jobs, skip it", jr.Namespace, jr.Name)
				continue
			}

			glog.V(3).Infof("Process backFill for job <%s:%s>", jr.Namespace, jr.Name)

			stmt := ssn.Statement()
			// jobNeedRePush as a label to determine whether the job should be pushed to the queue again.
			jobNeedRePush := false
			for {
				if taskRobbers.Empty() {
					break
				}

				tr := taskRobbers.Pop().(*api.TaskInfo)
				glog.V(3).Infof("Process backFill for task <%s:%s>", tr.Namespace, tr.Name)

				succeed := processBackFill(ssn, stmt, jr, tr)
				if ssn.JobReady(jr) {
					if succeed {
						jobNeedRePush = succeed
					}

					glog.V(3).Infof("Job <%s:%s> is ready to patch after backFill, process allocate for it",
						jr.Namespace, jr.Name)
					stmt.Commit()
					break
				}
			}

			if !ssn.JobReady(jr) {
				glog.V(3).Infof("Job <%s:%s> still can not be ready after backFill, "+
					"rollback associated backFill operation", jr.Namespace, jr.Name)
				stmt.Discard()
			}

			// As long as there is a task backFill succeed while job was ready, the job should be pushed to the sequence
			// again to try to perform the backFill operation. For the condition whether stopping the backFill is if the
			// job is ready, when a job is ready, it may still have pods with status of pending that need to be scheduled
			if jobNeedRePush {
				jobRobbers.Push(jr)
			}
		}
	}
}

func allocateBestEffortPod(ssn *framework.Session, task *api.TaskInfo, job *api.JobInfo) {
	nodes := util.GetNodeList(ssn.Nodes)

	candidateNodes, fitErrors := util.PredicateNodes(task, nodes, ssn.PredicateFn)
	if len(candidateNodes) == 0 {
		glog.V(3).Infof("Predicates failed for task <%s/%s> on all nodes",
			task.Namespace, task.Name)
		job.NodesFitErrors[task.UID] = fitErrors
		return
	}

	nodeScores := util.PrioritizeNodes(task, candidateNodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)

	nominatedNode := util.SelectBestNode(nodeScores)

	glog.V(3).Infof("Binding Task <%s/%s> to node <%s>", task.Namespace, task.Name, nominatedNode.Name)
	if err := ssn.Allocate(task, nominatedNode.Name); err != nil {
		glog.Errorf("Failed to bind Task %v on %s in Session %v", task.UID, nominatedNode.Name, ssn.UID)
		f := api.NewFitErrors()
		f.SetNodeError(nominatedNode.Name, err)
		job.NodesFitErrors[task.UID] = f
	}

	return
}

func processBackFill(ssn *framework.Session, stmt *framework.Statement, jr *api.JobInfo, tr *api.TaskInfo) (succeed bool) {
	nodeToVictims := selectNodesForBackFill(ssn, jr, tr)
	if len(nodeToVictims) <= 0 {
		glog.V(3).Infof("Task <%s:%s> of job <%s:%s> backFill failed for can not find appropriate node for it",
			tr.Namespace, tr.Name, jr.Namespace, jr.Name)
		return
	}

	nominatedNode := selectHost(ssn, nodeToVictims, tr)
	if nominatedNode == nil {
		glog.V(3).Infof("Task <%s:%s> of job <%s:%s> backFill failed for can not find appropriate node for it",
			tr.Namespace, tr.Name, jr.Namespace, jr.Name)
		return
	}
	nominatedTasksToDelete := nodeToVictims[nominatedNode]

	var nodeCache *api.NodeInfo
	if node, found := ssn.Nodes[nominatedNode.Name]; found {
		nodeCache = node
	} else {
		return
	}

	for _, task := range nominatedTasksToDelete.Tasks {
		glog.V(3).Infof("Try to backFill resource of task <%s:%s> for task <%s:%s>",
			task.Namespace, task.Name, tr.Namespace, tr.Name)
		if err := stmt.BackFill(task); err != nil {
			glog.Errorf("BackFill task <%s:%s> for task <%s:%s> failed for: %v",
				task.Namespace, task.Name, tr.Namespace, tr.Name, err)
			return
		}
	}

	// check if idle resource of nominated node is larger than request resource of task after backFill.
	if !tr.InitResreq.LessEqual(nodeCache.Idle.Clone()) {
		glog.Errorf("Idle resource of node <%s> can not satisfy the scheduling of task <%s:%s>, skip it",
			nodeCache.Name, tr.Namespace, tr.Name)
		return
	}

	glog.V(3).Infof("BackFill succeed for task <%s:%s>, try to allocate it on node <%s>",
		tr.Namespace, tr.Name, nodeCache.Name)
	if err := stmt.Allocate(tr, nodeCache.Name); err != nil {
		glog.Errorf("Allocate task <%s:%s> failed for: %v", tr.Namespace, tr.Name, err)
		return
	}

	return true
}

// selectNodesForBackFill finds all the potential nodes with potential victims for backFill in parallel.
func selectNodesForBackFill(ssn *framework.Session, jr *api.JobInfo, tr *api.TaskInfo) map[*api.NodeInfo]*victimsOfBackFill {
	nodes := []*api.NodeInfo{}
	for _, node := range ssn.Nodes {
		nodes = append(nodes, node.Snapshot())
	}

	nodeToPotentialVictims := map[*api.NodeInfo]*victimsOfBackFill{}
	var lock sync.Mutex
	checkNode := func(i int) {
		node := nodes[i]
		var potentialVictims []*api.TaskInfo
		if tr.InitResreq.LessEqual(node.Idle.Clone()) {
			glog.V(3).Infof("Idle resource of node <%s>  already meet the needs of task <%s:%s> scheduling, "+
				"do not need to release resource for it any more", node.Name, tr.Namespace, tr.Name)
		} else {
			potentialVictims = selectPotentialVictims(ssn, node, jr, tr)
			if len(potentialVictims) <= 0 {
				glog.V(3).Infof("Even releasing some of the resources that allocated, node %s still can not "+
					"satisfy the scheduling of task <%s:%s>", node.Name, tr.Namespace, tr.Name)
				return
			}
		}

		if err := ssn.PredicateFn(tr, node); err == nil {
			glog.V(3).Infof("After the backFill, resource of node %s meet the scheduling of task <%s:%s>",
				node.Name, tr.Namespace, tr.Name)
			lock.Lock()
			nodeToPotentialVictims[node] = &victimsOfBackFill{
				Tasks: potentialVictims,
			}
			lock.Unlock()
		} else {
			glog.Errorf("Predicate node %s for task <%s:%s> failed for %v",
				node.Name, tr.Namespace, tr.Name, err)
		}
	}

	workqueue.Parallelize(16, len(nodes), checkNode)
	return nodeToPotentialVictims
}

// selectPotentialVictims finds all potential victims for backFill in the node.
func selectPotentialVictims(ssn *framework.Session, node *api.NodeInfo, jr *api.JobInfo, tr *api.TaskInfo) []*api.TaskInfo {
	robbery := api.EmptyResource()
	potentialVictims := []*api.TaskInfo{}

	for _, task := range node.Tasks {
		if !ssn.CanBeBackFilled(tr, task) {
			continue
		}

		if err := node.RemoveTask(task); err != nil {
			glog.Errorf("Remove task <%s:%s> failed for: %v", task.Namespace, task.Name, err)
			continue
		}
		potentialVictims = append(potentialVictims, task)

		robbery.Add(task.Resreq)
		if tr.InitResreq.LessEqual(robbery) {
			return potentialVictims
		}
	}

	return []*api.TaskInfo{}
}

// selectHost finds nominated node with nominated victims for backFill.
// Preferred to choose node that satisfy tasks's scheduling without releasing any resource.
func selectHost(ssn *framework.Session, nodeToVictimMap map[*api.NodeInfo]*victimsOfBackFill, tr *api.TaskInfo) *api.NodeInfo {
	for node, victims := range nodeToVictimMap {
		if len(victims.Tasks) == 0 {
			return node
		}
	}

	nodes := []*api.NodeInfo{}
	for node := range nodeToVictimMap {
		nodes = append(nodes, node)
	}

	nodeScores := util.PrioritizeNodes(tr, nodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)

	return util.SelectBestNode(nodeScores)
}

func (bf *backfillAction) UnInitialize() {}
