// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"sort"
	"strings"
	"time"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// NodeUnreachablePodReason is the reason on a pod when its state cannot be confirmed as kubelet is unresponsive
	// on the node it is (was) running.
	NodeUnreachablePodReason = "NodeLost"
)

// Predicate is a function that indicates if a Pod can be (or not) deleted.
type Predicate func(candidate v1.Pod, expectedDeletions []v1.Pod, maxUnavailableReached bool) (bool, error)

// Sort is a function that sorts the remaining candidates
type Sort func(allPods []v1.Pod, state ESState) error

// DeletionStrategy defines the strategy when some Pods must be deleted.
// 1. Pods are sorted
// 2. Apply some predicates
type DeletionStrategy interface {
	Predicates() map[string]Predicate
	SortFunction() Sort
}

// NewDefaultDeletionStrategy returns the default deletion strategy
func NewDefaultDeletionStrategy(
	state ESState,
	podsStatus PodsStatus,
	masterNodesNames []string,
) *DefaultDeletionStrategy {
	return &DefaultDeletionStrategy{
		masterNodesNames: masterNodesNames,
		podsStatus:       podsStatus,
		esState:          state,
	}
}

// DefaultDeletionStrategy holds the context used by the default strategy.
type DefaultDeletionStrategy struct {
	masterNodesNames []string
	podsStatus       PodsStatus
	esState          ESState
}

// SortFunction is the default sort function, masters have lower priority as
// we want to update the nodes first.
// If 2 Pods are of the same type then use the reverse ordinal order.
// TODO: Add some priority to unhealthy (bootlooping) Pods
func (d *DefaultDeletionStrategy) SortFunction() Sort {
	return func(allPods []v1.Pod, state ESState) (err error) {
		sort.Slice(allPods[:], func(i, j int) bool {
			pod1 := allPods[i]
			pod2 := allPods[j]
			if (label.IsMasterNode(pod1) && label.IsMasterNode(pod2)) ||
				(!label.IsMasterNode(pod1) && !label.IsMasterNode(pod2)) { // same type, use the reverse name function
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
			if label.IsMasterNode(pod1) && !label.IsMasterNode(pod2) {
				// pod2 has higher priority since it is a node
				return false
			}
			return true
		})
		return err
	}
}

// Predicates returns a list of Predicates that will prevent a Pod from being deleted.
func (d *DefaultDeletionStrategy) Predicates() map[string]Predicate {
	return map[string]Predicate{
		// If MaxUnavailable is reached, allow for an unhealthy Pod to be deleted.
		// This is to prevent a situation where MaxUnavailable is reached and we
		// can't make some progress even if the user has updated the spec.
		"do_not_restart_healthy_node_if_MaxUnavailable_reached": func(
			candidate v1.Pod,
			expectedDeletions []v1.Pod,
			maxUnavailableReached bool,
		) (b bool, e error) {
			if maxUnavailableReached && k8s.IsPodReady(candidate) {
				return false, nil
			}
			return true, nil
		},
		// One master at a time
		"one_master_at_a_time": func(
			candidate v1.Pod,
			expectedDeletions []v1.Pod,
			maxUnavailableReached bool,
		) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(candidate) {
				return true, nil
			}
			for _, pod := range expectedDeletions {
				if label.IsMasterNode(pod) {
					return false, nil
				}
			}
			return true, nil
		},
		// Force an upgrade of all the data nodes before upgrading the last master
		"do_not_delete_last_master_if_datanodes_are_not_upgraded": func(
			candidate v1.Pod,
			expectedDeletions []v1.Pod,
			maxUnavailableReached bool,
		) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(candidate) {
				return true, nil
			}
			for _, pod := range d.podsStatus.toUpdate {
				if label.IsMasterNode(pod) {
					// There's some others master alive, allow this one to be deleted
					return true, nil
				}
			}
			// This is the last master, check if all data nodes are up to date
			for _, pod := range d.podsStatus.toUpdate {
				if label.IsDataNode(pod) {
					// There's still a data node to update
					return false, nil
				}
			}
			return true, nil
		},
		// Ensure that a master can be removed without breaking the quorum.
		// TODO: Deal with already broken quorum, only delete a Pod if:
		// 1. All Pods are Pending
		// 2. All Pods have failed several times during the last minutes
		// TODO: Mostly to do a test, not sure if we should keep this one
		"do_not_degrade_quorum": func(candidate v1.Pod, expectedDeletions []v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(candidate) {
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
			var healthyMasters []v1.Pod
			for _, pod := range d.podsStatus.healthy {
				if label.IsMasterNode(pod) {
					pod := pod
					healthyMasters = append(healthyMasters, pod)
				}
			}
			minimumMasterNodes := settings.Quorum(expectedMasters)
			return len(healthyMasters) > minimumMasterNodes, nil
		},
		"do_not_delete_last_healthy_master": func(candidate v1.Pod, expectedDeletions []v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			// If candidate is not a master then we don't care
			if !label.IsMasterNode(candidate) {
				return true, nil
			}

			// If only one master node is expected this cluster is not H.A.
			if len(d.masterNodesNames) < 2 {
				// I the cluster is configured with 1 or 2 nodes it is not H.A.
				return true, nil
			}

			var healthyMasters []v1.Pod
			candidateIsHealthy := false
			for _, expectedMaster := range d.masterNodesNames {
				for healthyPodName, healthyPod := range d.podsStatus.healthy {
					if !label.IsMasterNode(healthyPod) {
						continue
					}
					master := types.NamespacedName{
						Name:      expectedMaster,
						Namespace: candidate.Namespace,
					}
					if candidate.Name == healthyPodName {
						candidateIsHealthy = true
					}
					if healthyPodName == master.Name {
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
		"Skip_Unknown_LongTerminating_Pods": func(candidate v1.Pod, expectedDeletions []v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			if candidate.DeletionTimestamp != nil && candidate.Status.Reason == NodeUnreachablePodReason {
				// kubelet is unresponsive, Unknown Pod, do not try to delete it
				return false, nil
			}
			if candidate.DeletionTimestamp != nil && time.Now().After(candidate.DeletionTimestamp.Add(ExpectationsTTLNanosec)) {
				return false, nil
			}
			return true, nil
		},
		"do_not_Delete_Pods_With_Same_Shards": func(candidate v1.Pod, expectedDeletions []v1.Pod, maxUnavailableReached bool) (b bool, e error) {
			// TODO: We should not delete 2 Pods with the same shards
			return true, nil
		},
		// In Yellow or Red status only allow unhealthy Pods to be restarted.
		// This is intended to unlock some situations where the cluster is not green and
		// a Pod has to be restarted a second time.
		"do_not_Restart_Healthy_Node_if_Not_Green": func(
			candidate v1.Pod,
			expectedDeletions []v1.Pod,
			maxUnavailableReached bool,
		) (b bool, e error) {
			green, err := d.esState.GreenHealth()
			if err != nil {
				return false, err
			}
			if green {
				return true, nil
			}
			if !k8s.IsPodReady(candidate) {
				return true, nil
			}
			return false, nil
		},
	}
}