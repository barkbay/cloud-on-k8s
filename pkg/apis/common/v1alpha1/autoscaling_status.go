// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	"fmt"
	"github.com/elastic/cloud-on-k8s/v2/pkg/utils/set"
	"github.com/elastic/cloud-on-k8s/v2/pkg/utils/stringsutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const (
	CPURequired                   AutoscalingEventType = "CPURequired"
	EmptyResponse                 AutoscalingEventType = "EmptyResponse"
	HorizontalScalingLimitReached AutoscalingEventType = "HorizontalScalingLimitReached"
	MemoryRequired                AutoscalingEventType = "MemoryRequired"
	NoNodeSet                     AutoscalingEventType = "NoNodeSet"
	OverlappingPolicies           AutoscalingEventType = "OverlappingPolicies"
	StorageRequired               AutoscalingEventType = "StorageRequired"
	UnexpectedNodeStorageCapacity AutoscalingEventType = "UnexpectedNodeStorageCapacity"
	VerticalScalingLimitReached   AutoscalingEventType = "VerticalScalingLimitReached"

	NotOnlineErrorMessage string = "An error prevented the use of the autoscaling API"
)

const (
	// ElasticsearchAutoscalerActive status is True when compute (CPU and memory) and storage resources can be calculated.
	ElasticsearchAutoscalerActive ConditionType = "Active"

	// ElasticsearchAutoscalerHealthy status is true if resources have been calculated for all the resources and
	// no error has been encountered during the reconciliation process.
	ElasticsearchAutoscalerHealthy ConditionType = "Healthy"

	// ElasticsearchAutoscalerLimited status is true when a resource limit is reached.
	ElasticsearchAutoscalerLimited ConditionType = "Limited"

	// ElasticsearchAutoscalerOnline status is true if the Elasticsearch API was available
	// For example, it is expected for this condition to be False if the cluster is being bootstrapped, it should however
	// become True when operator is able to connect to Elasticsearch.
	ElasticsearchAutoscalerOnline ConditionType = "Online"
)

type ElasticsearchAutoscalerStatus struct {
	// ObservedGeneration is the last observed generation by the controller.
	ObservedGeneration *int64 `json:"observedGeneration,omitempty"`
	// Conditions holds the current service state of the autoscaling controller.
	// +kubebuilder:validation:Optional
	Conditions Conditions `json:"conditions,omitempty"`
	// AutoscalingPolicyStatuses is used to expose state messages to user or external system.
	// +kubebuilder:validation:Optional
	AutoscalingPolicyStatuses []AutoscalingPolicyStatus `json:"policies"`
}

type AutoscalingPolicyStatus struct {
	// Name is the name of the autoscaling policy
	Name string `json:"name"`
	// NodeSetNodeCount holds the number of nodes for each nodeSet.
	// +kubebuilder:validation:Optional
	NodeSetNodeCount NodeSetNodeCountList `json:"nodeSets"`
	// ResourcesSpecification holds the resource values common to all the nodeSets managed by a same autoscaling policy.
	// Only the resources managed by the autoscaling controller are saved in the Status.
	// +kubebuilder:validation:Optional
	ResourcesSpecification NodeResources `json:"resources"`
	// PolicyStates may contain various messages regarding the current state of this autoscaling policy.
	// +kubebuilder:validation:Optional
	PolicyStates []PolicyState `json:"state"`
	// LastModificationTime is the last time the resources have been updated, used by the cooldown algorithm.
	// +kubebuilder:validation:Optional
	LastModificationTime metav1.Time `json:"lastModificationTime"`
}

func (s *ElasticsearchAutoscalerStatus) CurrentResourcesForPolicy(policyName string) (NodeSetsResources, bool) {
	for _, policyStatus := range s.AutoscalingPolicyStatuses {
		if policyStatus.Name == policyName {
			return NodeSetsResources{
				Name:             policyStatus.Name,
				NodeSetNodeCount: policyStatus.NodeSetNodeCount,
				NodeResources:    policyStatus.ResourcesSpecification,
			}, true
		}
	}
	return NodeSetsResources{}, false
}

func (s *ElasticsearchAutoscalerStatus) LastModificationTime(policyName string) (metav1.Time, bool) {
	for _, policyState := range s.AutoscalingPolicyStatuses {
		if policyState.Name == policyName {
			return policyState.LastModificationTime, true
		}
	}
	return metav1.Time{}, false
}

type AutoscalingPolicyStatusBuilder struct {
	policyName           string
	nodeSetsResources    NodeSetsResources
	lastModificationTime metav1.Time
	states               map[AutoscalingEventType]PolicyState
}

func NewAutoscalingPolicyStatusBuilder(name string) *AutoscalingPolicyStatusBuilder {
	return &AutoscalingPolicyStatusBuilder{
		policyName: name,
		states:     make(map[AutoscalingEventType]PolicyState),
	}
}

func (psb *AutoscalingPolicyStatusBuilder) Build() AutoscalingPolicyStatus {
	policyStates := make([]PolicyState, len(psb.states))
	i := 0
	for _, v := range psb.states {
		policyStates[i] = PolicyState{
			Type:     v.Type,
			Messages: v.Messages,
		}
		i++
	}
	return AutoscalingPolicyStatus{
		Name:                   psb.policyName,
		NodeSetNodeCount:       psb.nodeSetsResources.NodeSetNodeCount,
		ResourcesSpecification: psb.nodeSetsResources.NodeResources,
		LastModificationTime:   psb.lastModificationTime,
		PolicyStates:           policyStates,
	}
}

// SetNodeSetsResources sets the compute resources associated to a tier.
func (psb *AutoscalingPolicyStatusBuilder) SetNodeSetsResources(nodeSetsResources NodeSetsResources) *AutoscalingPolicyStatusBuilder {
	psb.nodeSetsResources = nodeSetsResources
	return psb
}

func (psb *AutoscalingPolicyStatusBuilder) SetLastModificationTime(lastModificationTime metav1.Time) *AutoscalingPolicyStatusBuilder {
	psb.lastModificationTime = lastModificationTime
	return psb
}

// RecordEvent records a new event (type + message) for the tier.
func (psb *AutoscalingPolicyStatusBuilder) RecordEvent(stateType AutoscalingEventType, message string) *AutoscalingPolicyStatusBuilder {
	if policyState, ok := psb.states[stateType]; ok {
		policyState.Messages = append(policyState.Messages, message)
		psb.states[stateType] = policyState
		return psb
	}
	psb.states[stateType] = PolicyState{
		Type:     stateType,
		Messages: []string{message},
	}
	return psb
}

type AutoscalingEventType string

type PolicyState struct {
	Type     AutoscalingEventType `json:"type"`
	Messages []string             `json:"messages"`
}

type AutoscalingStatusBuilder struct {
	// Surface specific autoscaling events
	ScalingLimitEvents set.StringSet
	OtherEvents        set.StringSet

	// Online/Offline status
	online        *bool
	onlineMessage string

	// Policies statuses
	policyStatusBuilder map[string]*AutoscalingPolicyStatusBuilder
}

// NewAutoscalingStatusBuilder creates a new autoscaling status builder.
func NewAutoscalingStatusBuilder() *AutoscalingStatusBuilder {
	return &AutoscalingStatusBuilder{
		ScalingLimitEvents:  set.Make(),
		OtherEvents:         set.Make(),
		policyStatusBuilder: make(map[string]*AutoscalingPolicyStatusBuilder),
	}
}

func (asb *AutoscalingStatusBuilder) SetOnline(isOnline bool, message string) *AutoscalingStatusBuilder {
	asb.online = &isOnline
	if len(message) < 256 {
		asb.onlineMessage = message
		return asb
	}
	// arbitrarily truncate the message to avoid a full stacktrace in the resource status
	asb.onlineMessage = stringsutil.Truncate(message, 256) + "[...]"
	return asb
}

// UpdateResources sets or updates compute resources associated to all the tiers.
func (asb *AutoscalingStatusBuilder) UpdateResources(
	nextClusterResources ClusterResources,
	currentAutoscalingStatus ElasticsearchAutoscalerStatus,
) *AutoscalingStatusBuilder {
	// Update the timestamp on tiers resources
	now := metav1.Now()
	for _, nextNodeSetResources := range nextClusterResources {
		// Save the resources in the status
		asb.ForPolicy(nextNodeSetResources.Name).SetNodeSetsResources(nextNodeSetResources)

		// Restore the previous timestamp
		previousTimestamp, ok := currentAutoscalingStatus.LastModificationTime(nextNodeSetResources.Name)
		if ok {
			asb.ForPolicy(nextNodeSetResources.Name).SetLastModificationTime(previousTimestamp)
		}

		currentNodeSetResources, ok := currentAutoscalingStatus.CurrentResourcesForPolicy(nextNodeSetResources.Name)
		if !ok || !currentNodeSetResources.SameResources(nextNodeSetResources) {
			asb.ForPolicy(nextNodeSetResources.Name).SetLastModificationTime(now)
		}
	}
	return asb
}

func (asb *AutoscalingStatusBuilder) ForPolicy(policyName string) *AutoscalingPolicyStatusBuilder {
	if value, ok := asb.policyStatusBuilder[policyName]; ok {
		return value
	}
	policyStatusBuilder := NewAutoscalingPolicyStatusBuilder(policyName)
	asb.policyStatusBuilder[policyName] = policyStatusBuilder
	return policyStatusBuilder
}

func (asb *AutoscalingStatusBuilder) Build() ElasticsearchAutoscalerStatus {
	policyStates := make([]AutoscalingPolicyStatus, len(asb.policyStatusBuilder))
	i := 0
	for _, policyStateBuilder := range asb.policyStatusBuilder {
		for eventType := range policyStateBuilder.states {
			if eventType == VerticalScalingLimitReached || eventType == HorizontalScalingLimitReached {
				asb.ScalingLimitEvents.Add(policyStateBuilder.policyName)
			} else {
				asb.OtherEvents.Add(policyStateBuilder.policyName)
			}
		}
		policyStates[i] = policyStateBuilder.Build()
		i++
	}

	now := metav1.Now()
	var conditions Conditions
	if asb.ScalingLimitEvents.Count() > 0 {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerLimited,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: now,
				Message:            fmt.Sprintf("Limit reached for policies %s", strings.Join(asb.ScalingLimitEvents.AsSlice(), ",")),
			})
	} else {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerLimited,
				Status:             corev1.ConditionFalse,
				LastTransitionTime: now,
			})
	}
	if asb.OtherEvents.Count() > 0 {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerHealthy,
				Status:             corev1.ConditionFalse,
				LastTransitionTime: now,
				Message: fmt.Sprintf(
					"Issues reported for the following policies: [%s]. Check operator logs, Kubernetes events, and policies status for more details",
					strings.Join(asb.OtherEvents.AsSlice(), ","),
				),
			})
	} else {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerHealthy,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: now,
			})
	}

	// Set active status
	conditions = conditions.MergeWith(
		Condition{
			Type:               ElasticsearchAutoscalerActive,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		},
	)

	// Set online status
	if asb.online == nil {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerOnline,
				Status:             corev1.ConditionUnknown,
				LastTransitionTime: now,
			},
		)
	} else if *asb.online {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerOnline,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: now,
				Message:            asb.onlineMessage,
			},
		)
	} else {
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerOnline,
				Status:             corev1.ConditionFalse,
				LastTransitionTime: now,
				Message:            asb.onlineMessage,
			},
		)
	}

	if asb.online == nil || !*asb.online {
		// Also update the healthy condition if not online
		var healthyCondition Condition
		healthyConditionIdx := conditions.Index(ElasticsearchAutoscalerHealthy)
		if healthyConditionIdx > 0 {
			healthyCondition = conditions[healthyConditionIdx]
		}
		conditions = conditions.MergeWith(
			Condition{
				Type:               ElasticsearchAutoscalerHealthy,
				Status:             corev1.ConditionFalse,
				LastTransitionTime: now,
				Message:            strings.TrimSpace(fmt.Sprintf("%s. %s", NotOnlineErrorMessage, healthyCondition.Message)),
			},
		)
	}

	return ElasticsearchAutoscalerStatus{
		Conditions:                conditions,
		AutoscalingPolicyStatuses: policyStates,
	}
}
