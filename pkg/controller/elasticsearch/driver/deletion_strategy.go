package driver

import (
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/settings"
	v1 "k8s.io/api/core/v1"
)

// Predicate is a function that indicates if a Pod can be deleted.
type Predicate func(candidate *v1.Pod, expectedDeletions []*v1.Pod) (bool, error)

// Sort is a function that sorts the remaining candidates
type Sort func(allPods []*v1.Pod, state *ESState) ([]*v1.Pod, error)

// DeletionStrategy defines the strategy when some Pods must be deleted.
// 1. Pods are sorted
// 2. Apply some predicates
type DeletionStrategy interface {
	Predicates() map[string]Predicate
	SortFunction() Sort
}

func GetDeletionStrategy(state *ESState) *defaultDeletionStrategy {
	return &defaultDeletionStrategy{
		allPods:     nil,
		healthyPods: nil,
		state:       state,
	}
}

type defaultDeletionStrategy struct {
	allPods, healthyPods []*v1.Pod
	state                *ESState
}

func (d *defaultDeletionStrategy) SortFunction() Sort {
	return func(allPods []*v1.Pod, state *ESState) (pods []*v1.Pod, e error) {
		return allPods, nil
	}
}

func (d *defaultDeletionStrategy) Predicates() map[string]Predicate {
	return map[string]Predicate{
		// One master at a time
		"One_Master_At_A_Time": func(candidate *v1.Pod, expectedDeletions []*v1.Pod) (b bool, e error) {
			// If candidate is not a master then we don't care
			if label.IsMasterNode(*candidate) {
				return true, nil
			}
			for _, pod := range expectedDeletions {
				if label.IsMasterNode(*pod) {
					return false, nil
				}
			}
			return true, nil
		},
		// Ensure that a master can be removed without breaking the quorum.
		// If quorum is already broken then only delete a Pod if:
		// 1. All Pods are Pending
		// 2. All Pods have failed several times during the last minutes
		"Do_Not_Degrade_Quorum": func(candidate *v1.Pod, expectedDeletions []*v1.Pod) (b bool, e error) {
			// If candidate is not a master then we don't care
			if label.IsMasterNode(*candidate) {
				return true, nil
			}
			// Get the expected masters
			var expectedMasters []*v1.Pod
			for _, pod := range d.allPods {
				if label.IsMasterNode(*pod) {
					pod := pod
					expectedMasters = append(expectedMasters, pod)
				}
			}
			// Get the healthy masters
			var healthyMasters []*v1.Pod
			for _, pod := range d.healthyPods {
				if label.IsMasterNode(*pod) {
					pod := pod
					healthyMasters = append(healthyMasters, pod)
				}
			}
			minimumMasterNodes := settings.Quorum(len(expectedMasters))
			return len(healthyMasters)-1 > minimumMasterNodes, nil
		},
	}
}
