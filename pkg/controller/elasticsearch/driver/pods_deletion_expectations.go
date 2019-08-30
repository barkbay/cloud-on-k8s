// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

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
	// used to unlock some situations if a Pod is never terminated.
	//
	// Set to 5 minutes similar to https://github.com/kubernetes/kubernetes/blob/v1.13.2/pkg/controller/controller_utils.go
	ExpectationsTTLNanosec = 5 * time.Minute // time is internally represented as int64 nanoseconds
)

// NewDeleteExpectations creates a some new expectations.
func NewDeleteExpectations() *DeleteExpectations {
	return &DeleteExpectations{
		mutex:        sync.RWMutex{},
		expectations: map[types.NamespacedName]*expectedPods{},
		ttl:          ExpectationsTTLNanosec,
	}
}

// DeleteExpectations holds the expectations for all the namespaces.
type DeleteExpectations struct {
	mutex sync.RWMutex
	// expectations holds all the expected deletion for all the ES clusters.
	expectations map[types.NamespacedName]*expectedPods
	// ttl
	ttl time.Duration
}

// PodWithTTL is a Pod that, if not deleted within the expectation TTL, will be removed from the expectations.
type PodWithTTL struct {
	*v1.Pod
	time.Time
}

type podsWithTTL map[types.NamespacedName]*PodWithTTL

func (p podsWithTTL) toPods() []v1.Pod {
	result := make([]v1.Pod, len(p))
	i := 0
	for _, pod := range p {
		result[i] = *pod.Pod
		i++
	}
	return result
}

// TODO: this one is not thread safe
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

type expectationCheck interface {
	// canBeRemoved is the function used to check if a Pod can be removed.
	canBeRemoved(pod *PodWithTTL, ttl time.Duration) (bool, error)
}

// MayBeClearExpectations goes through all the Pod and check if they have been replaced.
// If yes, expectations are cleared.
func (d *DeleteExpectations) MayBeClearExpectations(
	cluster types.NamespacedName,
	controller expectationCheck,
) (bool, error) {
	pods := d.getOrCreateDeleteExpectations(cluster)
	for _, pod := range pods.podsWithTTL {
		mayBeRemoved, err := controller.canBeRemoved(pod, d.ttl)
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

// GetDeleteExpectations returns the expectation for a given cluster.
func (d *DeleteExpectations) GetDeleteExpectations(cluster types.NamespacedName) []v1.Pod {
	return d.getOrCreateDeleteExpectations(cluster).toPods()
}

// ExpectDelete marks a deletion for the given resource as expected.
func (d *DeleteExpectations) ExpectDelete(pod *v1.Pod) {
	cluster := clusterFromPod(pod.GetObjectMeta())
	if cluster == nil {
		return // Should not happen as all Pods should have the correct labels
	}
	d.getOrCreateDeleteExpectations(*cluster).addExpectation(pod, d.ttl)
}

// RemoveExpectation removes the expectation for a given pod
func (d *DeleteExpectations) RemoveExpectation(pod *v1.Pod) {
	cluster := clusterFromPod(pod.GetObjectMeta())
	if cluster == nil {
		return // Should not happen as all Pods should have the correct labels
	}
	d.getOrCreateDeleteExpectations(*cluster).removeExpectation(pod)
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

func (d *DeleteExpectations) getOrCreateDeleteExpectations(namespacedName types.NamespacedName) *expectedPods {
	d.mutex.RLock()
	expectedPods, exists := d.expectations[namespacedName]
	d.mutex.RUnlock()
	if !exists {
		expectedPods = d.createDeleteExpectations(namespacedName)
	}
	return expectedPods
}

func (d *DeleteExpectations) createDeleteExpectations(namespacedName types.NamespacedName) *expectedPods {
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
