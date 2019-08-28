package driver

import (
	"sync"
	"time"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ExpectationsTTLNanosec is the default expectations time-to-live,
	// used to unlock some situations if a Pod never restart.
	//
	// Set to 5 minutes similar to https://github.com/kubernetes/kubernetes/blob/v1.13.2/pkg/controller/controller_utils.go
	ExpectationsTTLNanosec = 5 * time.Minute // time is internally represented as int64 nanoseconds
)

// TODO: Do we really need to be thread safe ? No watches, it will be only called by the ES controller...
func NewRestartExpectations() *RestartExpectations {
	return &RestartExpectations{
		mutex:        sync.RWMutex{},
		expectations: map[types.NamespacedName]*expectedPods{},
		ttl:          ExpectationsTTLNanosec,
	}
}

type RestartExpectations struct {
	mutex sync.RWMutex
	// expectations holds all the expected deletion for all the ES clusters.
	expectations map[types.NamespacedName]*expectedPods
	// ttl
	ttl time.Duration
}

type PodWithTTL struct {
	*v1.Pod
	time.Time
}

type podsWithTTL map[types.NamespacedName]*PodWithTTL

func (p podsWithTTL) toPods() []*v1.Pod {
	result := make([]*v1.Pod, len(p))
	i := 0
	for _, pod := range p {
		result[i] = pod.Pod
		i++
	}
	return result
}

func (p podsWithTTL) clear() {
	// TODO: We can do something smarter
	for k := range p {
		delete(p, k)
	}
}

type expectedPods struct {
	sync.Mutex
	// podsWithTTL holds the pod expected to be deleted for a specific ES cluster.
	podsWithTTL
}

func newExpectedPods() *expectedPods {
	return &expectedPods{
		Mutex:       sync.Mutex{},
		podsWithTTL: map[types.NamespacedName]*PodWithTTL{},
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

type ExpectationController interface {
	MayBeRemoved(pod *PodWithTTL, ttl time.Duration) (bool, error)
}

// ClearExpectations goes through all the Pod and check if they have been restarted.
// If yes, expectations are cleared.
func (d *RestartExpectations) MayBeClearExpectations(
	cluster types.NamespacedName,
	controller ExpectationController,
) (bool, error) {
	pods := d.getOrCreateRestartExpectations(cluster)
	for _, pod := range pods.podsWithTTL {
		mayBeRemoved, err := controller.MayBeRemoved(pod, d.ttl)
		if err != nil {
			return false, err
		}
		if !mayBeRemoved {
			return false, nil
		}
	}
	// All expectations are cleared !
	pods.clear()
	return true, nil
}

func (d *RestartExpectations) GetExpectedRestarts(cluster types.NamespacedName) []*v1.Pod {
	return d.getOrCreateRestartExpectations(cluster).toPods()
}

// ExpectRestart marks a deletion for the given resource as expected.
func (d *RestartExpectations) ExpectRestart(pod *v1.Pod) {
	cluster := clusterFromPod(pod.GetObjectMeta())
	if cluster == nil {
		return // Should not happen as all Pods should have the correct labels
	}
	d.getOrCreateRestartExpectations(*cluster).addExpectation(pod, d.ttl)
}

func (d *RestartExpectations) RemoveExpectation(pod *v1.Pod) {
	cluster := clusterFromPod(pod.GetObjectMeta())
	if cluster == nil {
		return // Should not happen as all Pods should have the correct labels
	}
	d.getOrCreateRestartExpectations(*cluster).removeExpectation(pod)
}

func (e *expectedPods) addExpectation(pod *v1.Pod, ttl time.Duration) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	e.podsWithTTL[k8s.ExtractNamespacedName(pod)] = &PodWithTTL{
		Pod:  pod,
		Time: time.Now(),
	}
}

func (e *expectedPods) removeExpectation(pod *v1.Pod) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	delete(e.podsWithTTL, k8s.ExtractNamespacedName(pod))
}

func (d *RestartExpectations) getOrCreateRestartExpectations(namespacedName types.NamespacedName) *expectedPods {
	d.mutex.RLock()
	expectedPods, exists := d.expectations[namespacedName]
	d.mutex.RUnlock()
	if !exists {
		expectedPods = d.createRestartExpectations(namespacedName)
	}
	return expectedPods
}

func (d *RestartExpectations) createRestartExpectations(namespacedName types.NamespacedName) *expectedPods {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	expectedPods, exists := d.expectations[namespacedName]
	if exists {
		return expectedPods
	}
	expectedPods = newExpectedPods()
	d.expectations[namespacedName] = expectedPods
	return expectedPods
}
