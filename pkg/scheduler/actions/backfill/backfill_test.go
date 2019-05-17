package backfill

import (
	"reflect"
	"testing"
	"time"

	"github.com/kubernetes-sigs/kube-batch/pkg/apis/scheduling/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/actions/allocate"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/cache"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/drf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/gang"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/nodeorder"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/predicates"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/priority"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func TestBackFill(t *testing.T) {
	framework.RegisterPluginBuilder("gang", gang.New)
	framework.RegisterPluginBuilder("predicates", predicates.New)
	framework.RegisterPluginBuilder("drf", drf.New)
	framework.RegisterPluginBuilder("priority", priority.New)
	framework.RegisterPluginBuilder("nodeorder", nodeorder.New)

	n1 := util.BuildNode("n1", util.BuildResourceList("3", "4G"), make(map[string]string))
	n1.Status.Allocatable["pods"] = resource.MustParse("100")

	n2 := util.BuildNode("n2", util.BuildResourceList("7", "7G"), make(map[string]string))
	n2.Status.Allocatable["pods"] = resource.MustParse("100")

	tests := []struct {
		name         string
		podGroups    []*v1alpha1.PodGroup
		pods         []*v1.Pod
		nodes        []*v1.Node
		queues       []*v1alpha1.Queue
		expectedNode string
		expected     map[string]string
	}{
		{
			name: "allocate resource for non-bestEffort pods",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					util.BuildResourceList("2.5", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p2", "", v1.PodPending,
					util.BuildResourceList("2.5", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p3", "", v1.PodPending,
					v1.ResourceList{}, "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p4", "", v1.PodPending,
					v1.ResourceList{}, "pg2",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"ns1/p3": "n1",
				"ns1/p4": "n1",
			},
		},
		{
			name: "basic backFill logic",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					util.BuildResourceList("2.5", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p2", "", v1.PodPending,
					util.BuildResourceList("2.5", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p3", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p4", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"ns1/p3": "n1",
				"ns1/p4": "n1",
			},
		},
		{
			name: "no enough resources to make backFill succeed",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					util.BuildResourceList("2.5", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p2", "", v1.PodPending,
					util.BuildResourceList("2.5", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p3", "", v1.PodPending,
					util.BuildResourceList("1.6", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p4", "", v1.PodPending,
					util.BuildResourceList("1.6", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{},
		},
		{
			name: "multiple candidate nodes meet backFill condition",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 3,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg3",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p2", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p3", "", v1.PodPending,
					util.BuildResourceList("3", "3G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p4", "", v1.PodPending,
					util.BuildResourceList("3", "3G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p5", "", v1.PodPending,
					util.BuildResourceList("3", "3G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p6", "", v1.PodPending,
					util.BuildResourceList("3", "3G"), "pg3",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p7", "", v1.PodPending,
					util.BuildResourceList("3", "3G"), "pg3",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1, n2,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"ns1/p1": "n2",
				"ns1/p2": "n2",
				"ns1/p6": "n2",
				"ns1/p7": "n1",
			},
		},
	}

	backFillAction := New(framework.Arguments{"enableBackFillNonBestEffortPods": "true"})
	allocateAction := allocate.New(framework.Arguments{})
	for i, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}

		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},
			Recorder:      record.NewFakeRecorder(100),
		}

		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}

		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, pg := range test.podGroups {
			schedulerCache.AddPodGroup(pg)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:                  "gang",
						EnabledJobReady:       &trueValue,
						EnabledJobOrder:       &trueValue,
						EnabledJobBackFill:    &trueValue,
						EnabledTaskBackFilled: &trueValue,
					},
					{
						Name:             "predicates",
						EnabledPredicate: &trueValue,
					},
					{
						Name:            "drf",
						EnabledJobOrder: &trueValue,
					},
					{
						Name:             "priority",
						EnabledJobOrder:  &trueValue,
						EnabledTaskOrder: &trueValue,
					},
					{
						Name:             "nodeorder",
						EnabledNodeOrder: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		allocateAction.Execute(ssn)
		backFillAction.Execute(ssn)

		for i := 0; i < len(test.expected); i++ {
			select {
			case <-binder.Channel:
			case <-time.After(3 * time.Second):
				t.Errorf("Failed to get backFill request")
			}
		}

		if !reflect.DeepEqual(test.expected, binder.Binds) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, binder.Binds)
		}
	}
}

func TestAllocateBestEffortPod(t *testing.T) {
	framework.RegisterPluginBuilder("predicates", predicates.New)

	n1 := util.BuildNode("n1", util.BuildResourceList("3", "4G"), make(map[string]string))
	n1.Status.Allocatable["pods"] = resource.MustParse("100")

	tests := []struct {
		name         string
		podGroups    []*v1alpha1.PodGroup
		pods         []*v1.Pod
		nodes        []*v1.Node
		queues       []*v1alpha1.Queue
		expectedNode string
		expected     map[string]string
	}{
		{
			name: "function of allocating resources for non-bestEffort task",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 1,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					v1.ResourceList{}, "pg1",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"ns1/p1": "n1",
			},
		},
	}

	for i, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}

		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},
			Recorder:      record.NewFakeRecorder(100),
		}

		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}

		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, pg := range test.podGroups {
			schedulerCache.AddPodGroup(pg)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:             "predicates",
						EnabledPredicate: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		for _, job := range ssn.Jobs {
			for _, task := range job.TaskStatusIndex[api.Pending] {
				allocateBestEffortPod(ssn, task, job)
			}
		}

		for i := 0; i < len(test.expected); i++ {
			select {
			case <-binder.Channel:
			case <-time.After(3 * time.Second):
				t.Errorf("Failed to get backFill request")
			}
		}

		if !reflect.DeepEqual(test.expected, binder.Binds) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, binder.Binds)
		}
	}
}

func TestSelectNodesForBackFill(t *testing.T) {
	framework.RegisterPluginBuilder("gang", gang.New)
	framework.RegisterPluginBuilder("predicates", predicates.New)
	framework.RegisterPluginBuilder("drf", drf.New)
	framework.RegisterPluginBuilder("priority", priority.New)
	framework.RegisterPluginBuilder("nodeorder", nodeorder.New)

	n1 := util.BuildNode("n1", util.BuildResourceList("3", "4G"), make(map[string]string))
	n1.Status.Allocatable["pods"] = resource.MustParse("100")

	n2 := util.BuildNode("n2", util.BuildResourceList("7", "7G"), make(map[string]string))
	n2.Status.Allocatable["pods"] = resource.MustParse("100")

	tests := []struct {
		name         string
		podGroups    []*v1alpha1.PodGroup
		pods         []*v1.Pod
		nodes        []*v1.Node
		queues       []*v1alpha1.Queue
		expectedNode string
		expected     map[string]map[string][]string
	}{
		{
			name: "function of select nodes for backFill",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 3,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					util.BuildResourceList("7", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p2", "", v1.PodPending,
					util.BuildResourceList("3", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p3", "", v1.PodPending,
					util.BuildResourceList("7", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p4", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p5", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1, n2,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]map[string][]string{
				"p4": {
					"n1": []string{"p2"},
					"n2": []string{"p1"},
				},
				"p5": {
					"n1": []string{"p2"},
					"n2": []string{"p1"},
				},
			},
		},
	}

	allocateAction := allocate.New(framework.Arguments{})
	for i, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}

		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},
			Recorder:      record.NewFakeRecorder(100),
		}

		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}

		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, pg := range test.podGroups {
			schedulerCache.AddPodGroup(pg)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:                  "gang",
						EnabledJobReady:       &trueValue,
						EnabledJobOrder:       &trueValue,
						EnabledJobBackFill:    &trueValue,
						EnabledTaskBackFilled: &trueValue,
					},
					{
						Name:             "predicates",
						EnabledPredicate: &trueValue,
					},
					{
						Name:            "drf",
						EnabledJobOrder: &trueValue,
					},
					{
						Name:             "priority",
						EnabledJobOrder:  &trueValue,
						EnabledTaskOrder: &trueValue,
					},
					{
						Name:             "nodeorder",
						EnabledNodeOrder: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		allocateAction.Execute(ssn)

		result := make(map[string]map[string][]string)
		for _, job := range ssn.Jobs {
			for _, task := range job.TaskStatusIndex[api.Pending] {
				nodeToVictims := selectNodesForBackFill(ssn, job, task)
				nodeVictims := make(map[string][]string)
				for node, victimOfBackFill := range nodeToVictims {
					if len(victimOfBackFill.Tasks) >= 0 {
						victims := []string{}
						for _, v := range victimOfBackFill.Tasks {
							victims = append(victims, v.Name)
						}
						nodeVictims[node.Name] = victims
					}
				}
				if len(nodeVictims) > 0 {
					result[task.Name] = nodeVictims
				}
			}
		}

		if !reflect.DeepEqual(test.expected, result) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, result)
		}
	}
}

func TestSelectHost(t *testing.T) {
	framework.RegisterPluginBuilder("gang", gang.New)
	framework.RegisterPluginBuilder("predicates", predicates.New)
	framework.RegisterPluginBuilder("drf", drf.New)
	framework.RegisterPluginBuilder("priority", priority.New)
	framework.RegisterPluginBuilder("nodeorder", nodeorder.New)

	n1 := util.BuildNode("n1", util.BuildResourceList("3", "4G"), make(map[string]string))
	n1.Status.Allocatable["pods"] = resource.MustParse("100")

	n2 := util.BuildNode("n2", util.BuildResourceList("7", "7G"), make(map[string]string))
	n2.Status.Allocatable["pods"] = resource.MustParse("100")

	tests := []struct {
		name         string
		podGroups    []*v1alpha1.PodGroup
		pods         []*v1.Pod
		nodes        []*v1.Node
		queues       []*v1alpha1.Queue
		expectedNode string
		expected     map[string]string
	}{
		{
			name: "function of select host",
			podGroups: []*v1alpha1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 3,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "ns1",
					},
					Spec: v1alpha1.PodGroupSpec{
						Queue:     "q1",
						MinMember: 2,
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("ns1", "p1", "", v1.PodPending,
					util.BuildResourceList("7", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p2", "", v1.PodPending,
					util.BuildResourceList("3", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p3", "", v1.PodPending,
					util.BuildResourceList("7", "1G"), "pg1",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p4", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
				util.BuildPod("ns1", "p5", "", v1.PodPending,
					util.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				n1, n2,
			},
			queues: []*v1alpha1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: v1alpha1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"p4": "n2",
				"p5": "n2",
			},
		},
	}

	allocateAction := allocate.New(framework.Arguments{})
	for i, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}

		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},
			Recorder:      record.NewFakeRecorder(100),
		}

		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}

		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, pg := range test.podGroups {
			schedulerCache.AddPodGroup(pg)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:                  "gang",
						EnabledJobReady:       &trueValue,
						EnabledJobOrder:       &trueValue,
						EnabledJobBackFill:    &trueValue,
						EnabledTaskBackFilled: &trueValue,
					},
					{
						Name:             "predicates",
						EnabledPredicate: &trueValue,
					},
					{
						Name:            "drf",
						EnabledJobOrder: &trueValue,
					},
					{
						Name:             "priority",
						EnabledJobOrder:  &trueValue,
						EnabledTaskOrder: &trueValue,
					},
					{
						Name:             "nodeorder",
						EnabledNodeOrder: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		allocateAction.Execute(ssn)

		result := make(map[string]string)
		for _, job := range ssn.Jobs {
			for _, task := range job.TaskStatusIndex[api.Pending] {
				nodeToVictims := selectNodesForBackFill(ssn, job, task)
				if len(nodeToVictims) <= 0 {
					continue
				}

				node := selectHost(ssn, nodeToVictims, task)
				if node != nil {
					result[task.Name] = node.Name
				}
			}
		}

		if !reflect.DeepEqual(test.expected, result) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, result)
		}
	}
}
