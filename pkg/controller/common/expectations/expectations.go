// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package expectations

import (
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type deleteExpectations map[types.NamespacedName]map[types.NamespacedName]metav1.ObjectMeta

// Note: expectations are NOT thread-safe.
// TODO: garbage collect/finalize deprecated expectation
type Expectations struct {
	// StatefulSet -> generation
	generations map[types.UID]int64
	// Cluster -> Pod
	// We could only keep UID in memory, purpose of ObjectMeta is logging and debugging
	deletions deleteExpectations
}

func NewExpectations() *Expectations {
	return &Expectations{
		generations: make(map[types.UID]int64),
		deletions:   make(deleteExpectations),
	}
}

// Deletions

// ExpectDelete registers an expected deletion for the given Pod.
func (e *Expectations) ExpectDeletion(pod v1.Pod) {
	cluster, exists := label.ClusterFromResourceLabels(pod.GetObjectMeta())
	if !exists {
		return // Should not happen as all Pods should have the correct labels
	}
	var expectedPods map[types.NamespacedName]metav1.ObjectMeta
	expectedPods, exists = e.deletions[cluster]
	if !exists {
		expectedPods = map[types.NamespacedName]metav1.ObjectMeta{}
		e.deletions[cluster] = expectedPods
	}
	expectedPods[k8s.ExtractNamespacedName(&pod)] = pod.ObjectMeta
}

// DeletionChecker is used to check if a Pod can be remove from the deletions expectations.
type DeletionChecker interface {
	CanRemoveExpectation(meta metav1.ObjectMeta) (bool, error)
}

// SatisfiedDeletions uses the provided DeletionChecker to check of deletions expectations are satisfied.
func (e *Expectations) SatisfiedDeletions(cluster types.NamespacedName, checker DeletionChecker) (bool, error) {
	// Get all the deletions expected for this cluster
	deletions, ok := e.deletions[cluster]
	if !ok {
		return true, nil
	}
	for pod, meta := range deletions {
		canRemove, err := checker.CanRemoveExpectation(meta)
		if err != nil {
			return false, err
		}
		if canRemove {
			delete(deletions, pod)
		} else {
			return false, nil
		}
	}
	return len(deletions) == 0, nil
}

// Generations

func (e *Expectations) ExpectGeneration(meta metav1.ObjectMeta) {
	e.generations[meta.UID] = meta.Generation
}

func (e *Expectations) GenerationExpected(metaObjs ...metav1.ObjectMeta) bool {
	for _, meta := range metaObjs {
		if expectedGen, exists := e.generations[meta.UID]; exists && meta.Generation < expectedGen {
			return false
		}
	}
	return true
}

// GetGenerations returns the map of generations, for testing purpose mostly.
func (e *Expectations) GetGenerations() map[types.UID]int64 {
	return e.generations
}
