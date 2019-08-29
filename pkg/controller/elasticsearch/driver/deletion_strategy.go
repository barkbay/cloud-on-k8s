package driver

import (
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Predicate is a function that indicates if a Pod can be (or not) deleted.
type Predicate func(candidate *v1.Pod, expectedDeletions []*v1.Pod, maxUnavailableReached bool) (bool, error)

// Sort is a function that sorts the remaining candidates
type Sort func(allPods []*v1.Pod, state *ESState) error

// DeletionStrategy defines the strategy when some Pods must be deleted.
// 1. Pods are sorted
// 2. Apply some predicates
type DeletionStrategy interface {
	Predicates() map[string]Predicate
	SortFunction() Sort
}

func GetDeletionStrategy(state *ESState, healthyPods map[types.NamespacedName]*v1.Pod, masterNodesNames []string) *defaultDeletionStrategy {
	return &defaultDeletionStrategy{
		masterNodesNames: masterNodesNames,
		healthyPods:      healthyPods,
		state:            state,
	}
}

type defaultDeletionStrategy struct {
	masterNodesNames []string
	healthyPods      map[types.NamespacedName]*v1.Pod
	state            *ESState
}

// SortFunction is the default sort function, masters have lower priority as
// we want to update the nodes first.
// If 2 Pods are of the same type then use the reverse ordinal order
func (d *defaultDeletionStrategy) SortFunction() Sort {
	return func(allPods []*v1.Pod, state *ESState) (err error) {
		sort.Slice(allPods[:], func(i, j int) bool {
			pod1 := allPods[i]
			pod2 := allPods[j]
			if (label.IsMasterNode(*pod1) && label.IsMasterNode(*pod2)) ||
				(!label.IsMasterNode(*pod1) && !label.IsMasterNode(*pod2)) { // same type, use the reverse name function
				ssetName1, ord1, err := sset.StatefulSetName(pod1.Name)
				if err != nil {
					return false
				}
				ssetName2, ord2, err := sset.StatefulSetName(pod2.Name)
				if err != nil {
					return false
				}
				if strings.Compare(ssetName1, ssetName2) == 0 {
					// same name, compare ordinal, higher first
					return ord1 > ord2
				}
				return strings.Compare(ssetName1, ssetName2) == -1
			}
			if label.IsMasterNode(*pod1) && !label.IsMasterNode(*pod2) {
				// pod2 has higher priority since it is a node
				return false
			}
			return true
		})
		return err
	}
}

// Predicates returns a list of Predicates that will prevent a Pod from being deleted.
func (d *defaultDeletionStrategy) Predicates() map[string]Predicate {
	return map[string]Predicate{
		// One master at a time
		"Do_Not_Restart_Healthy_Node_If_MaxAvailable_Reached": func(
			candidate *v1.Pod,
			expectedDeletions []*v1.Pod,
			maxUnavailableReached bool,
		) (b bool, e error) {
			if maxUnavailableReached && k8s.IsPodReady(*candidate) {
				return false, nil
			}
			return true, nil
		},
		// One master at a time
		"One_Master_At_A_Time": func(
			candidate *v1.Pod,
			expectedDeletions []*v1.Pod,
			maxUnavailableReached bool,
		) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(*candidate) {
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
		// TODO: Deal with already broken quorum, only delete a Pod if:
		// 1. All Pods are Pending
		// 2. All Pods have failed several times during the last minutes
		"Do_Not_Degrade_Quorum": func(candidate *v1.Pod, expectedDeletions []*v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(*candidate) {
				return true, nil
			}
			// Get the expected masters
			expectedMasters := len(d.masterNodesNames)

			// Special case
			if expectedMasters < 3 {
				// I the cluster is configured with 1 or 2 nodes it is not H.A.
				return true, nil
			}

			// Get the healthy masters
			var healthyMasters []*v1.Pod
			for _, pod := range d.healthyPods {
				if label.IsMasterNode(*pod) {
					pod := pod
					healthyMasters = append(healthyMasters, pod)
				}
			}
			minimumMasterNodes := settings.Quorum(expectedMasters)
			return len(healthyMasters) > minimumMasterNodes, nil
		},
		"Do_Not_Delete_Last_Healthy_Master": func(candidate *v1.Pod, expectedDeletions []*v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(*candidate) {
				return true, nil
			}

			// If only one master node is expected this cluster is not H.A.
			if len(d.masterNodesNames) < 2 {
				// I the cluster is configured with 1 or 2 nodes it is not H.A.
				return true, nil
			}

			var healthyMasters []*v1.Pod
			candidateIsHealthy := false
			for _, expectedMaster := range d.masterNodesNames {
				for healthyPodName, healthyPod := range d.healthyPods {
					if !label.IsMasterNode(*healthyPod) {
						continue
					}
					master := types.NamespacedName{
						Name:      expectedMaster,
						Namespace: candidate.Namespace,
					}
					if candidate.Name == healthyPodName.Name {
						candidateIsHealthy = true
					}
					if healthyPodName == master {
						healthyMasters = append(healthyMasters, healthyPod)
					}
				}
			}

			if candidateIsHealthy && (len(healthyMasters) == 1) {
				// Last healthy one, don't delete
				return false, nil
			}
			return true, nil
		},
		"Skip_Unknown_LongTerminating_Pods": func(candidate *v1.Pod, expectedDeletions []*v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			// TODO: We should not try to delete an Unknown Pod or a Pod stuck in a Terminating state
			return true, nil
		},
	}
}