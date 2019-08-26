package driver

import (
	"sync"
	"time"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	// ExpectationsTTLNanosec is the default expectations time-to-live,
	// for cases where we expect a deletion that never happens.
	//
	// Set to 5 minutes similar to https://github.com/kubernetes/kubernetes/blob/v1.13.2/pkg/controller/controller_utils.go
	ExpectationsTTLNanosec = 5 * time.Minute // time is internally represented as int64 nanoseconds
)

func NewDeleteExpectations() *DeleteExpectations {
	return &DeleteExpectations{
		mutex:        sync.RWMutex{},
		expectations: map[types.NamespacedName]*expectedPods{},
		ttl:          ExpectationsTTLNanosec,
	}
}

type DeleteExpectations struct {
	mutex sync.RWMutex
	// expectations holds all the expected deletion for all the ES clusters.
	expectations map[types.NamespacedName]*expectedPods
	// ttl
	ttl time.Duration
}

type podWithTTL struct {
	*v1.Pod
	ttl int64
}

type podsWithTTL map[types.NamespacedName]*podWithTTL

func (p podsWithTTL) toPods() []*v1.Pod {
	result := make([]*v1.Pod, len(p))
	i := 0
	for _, pod := range p {
		result[i] = pod.Pod
		i++
	}
	return result
}

type expectedPods struct {
	sync.Mutex
	// podsWithTTL holds the pod expected to be deleted for a specific ES cluster.
	podsWithTTL
}

func newExpectedPods() *expectedPods {
	return &expectedPods{
		Mutex:       sync.Mutex{},
		podsWithTTL: map[types.NamespacedName]*podWithTTL{},
	}
}

func clusterFromPod(meta metav1.Object) *types.NamespacedName {
	labels := meta.GetLabels()
	clusterName, isSet := labels[label.ClusterNameLabelName]
	if isSet {
		return &types.NamespacedName{
			Namespace: meta.GetNamespace(),
			Name:      clusterName,
		}
	}
	return nil
}

func (d *DeleteExpectations) GetExpectedDeletions(cluster types.NamespacedName) []*v1.Pod {
	return d.getOrCreateExpectedPods(cluster).toPods()
}

// ExpectDeletion marks a deletion for the given resource as expected.
func (d *DeleteExpectations) ExpectDeletion(pod *v1.Pod) {
	cluster := clusterFromPod(pod.GetObjectMeta())
	if cluster == nil {
		return // Should not happen as all Pods should have the correct labels
	}
	d.getOrCreateExpectedPods(*cluster).addExpectation(pod, d.ttl)
}

func (d *DeleteExpectations) RemoveExpectation(pod *v1.Pod) {
	cluster := clusterFromPod(pod.GetObjectMeta())
	if cluster == nil {
		return // Should not happen as all Pods should have the correct labels
	}
	d.getOrCreateExpectedPods(*cluster).removeExpectation(pod)
}

func (e *expectedPods) addExpectation(pod *v1.Pod, ttl time.Duration) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	e.podsWithTTL[k8s.ExtractNamespacedName(pod)] = &podWithTTL{
		Pod: pod,
		ttl: ttl.Nanoseconds(),
	}
}

func (e *expectedPods) removeExpectation(pod *v1.Pod) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	delete(e.podsWithTTL, k8s.ExtractNamespacedName(pod))
}

func (d *DeleteExpectations) getOrCreateExpectedPods(namespacedName types.NamespacedName) *expectedPods {
	d.mutex.RLock()
	expectedPods, exists := d.expectations[namespacedName]
	d.mutex.RUnlock()
	if !exists {
		expectedPods = d.createExpectedPods(namespacedName)
	}
	return expectedPods
}

func (d *DeleteExpectations) createExpectedPods(namespacedName types.NamespacedName) *expectedPods {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	// if this method is called, counters probably don't exist yet
	// still re-check with lock acquired in case they would be created
	// in-between 2 concurrent calls to e.getOrCreateCounters
	expectedPods, exists := d.expectations[namespacedName]
	if exists {
		return expectedPods
	}
	expectedPods = newExpectedPods()
	d.expectations[namespacedName] = expectedPods
	return expectedPods
}

// Watch
type PodDeletionWatch struct {
	expectations *DeleteExpectations
}

func NewPodDeletionWatch(expectations *DeleteExpectations) *PodDeletionWatch {
	return &PodDeletionWatch{expectations: expectations}
}

func (e *PodDeletionWatch) Create(event.CreateEvent, workqueue.RateLimitingInterface) {}
func (e *PodDeletionWatch) Update(event.UpdateEvent, workqueue.RateLimitingInterface) {}
func (e *PodDeletionWatch) Delete(evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	pod, ok := evt.Object.(*v1.Pod)
	if ok {
		e.expectations.RemoveExpectation(pod)
	}
}
func (e *PodDeletionWatch) Generic(event.GenericEvent, workqueue.RateLimitingInterface) {}
