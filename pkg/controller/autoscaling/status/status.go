// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package status

import (
	"encoding/json"
	"fmt"
	"hash/adler32"
	"sort"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ElasticsearchAutoscalingStatusAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-status"

	VerticalScalingLimitReached   PolicyStateType = "VerticalScalingLimitReached"
	HorizontalScalingLimitReached PolicyStateType = "HorizontalScalingLimitReached"
	OverlappingPolicies           PolicyStateType = "OverlappingPolicies"
	InvalidMinimumNodeCount       PolicyStateType = "InvalidMinimumNodeCount"
	MemoryRequired                PolicyStateType = "MemoryRequired"
	StorageRequired               PolicyStateType = "StorageRequired"
	NoNodeSet                     PolicyStateType = "NoNodeSet"
)

type Status struct {
	// PolicyStatus is used to expose state messages to user or external system
	PolicyStates []PolicyStateItem `json:"policies"`

	// NodeSetsResourcesWithMeta is used to expose the last computed resources per nodeSet
	NodeSetResources NodeSetsResourcesWithMeta `json:"resources"`
}

type PolicyStateItem struct {
	Name         string        `json:"name"`
	NodeSets     []string      `json:"nodeSets"`
	PolicyStates []PolicyState `json:"state"`
}

type PolicyStateBuilder struct {
	policyName string
	nodeSets   []string
	states     map[PolicyStateType]PolicyState
}

func NewPolicyStateBuilder(name string) *PolicyStateBuilder {
	return &PolicyStateBuilder{
		policyName: name,
		states:     make(map[PolicyStateType]PolicyState),
	}
}

func (psb *PolicyStateBuilder) Build() PolicyStateItem {
	policyStates := make([]PolicyState, len(psb.states))
	i := 0
	for _, v := range psb.states {
		policyStates[i] = PolicyState{
			Type:     v.Type,
			Messages: v.Messages,
		}
		i++
	}
	return PolicyStateItem{
		Name:         psb.policyName,
		NodeSets:     psb.nodeSets,
		PolicyStates: policyStates,
	}
}

func (psb *PolicyStateBuilder) WithPolicyState(stateType PolicyStateType, message string) *PolicyStateBuilder {
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

func (psb *PolicyStateBuilder) SetNodeSets(nodeSets []esv1.NodeSet) *PolicyStateBuilder {
	psb.nodeSets = make([]string, len(nodeSets))
	for i := range nodeSets {
		psb.nodeSets[i] = nodeSets[i].Name
	}
	return psb
}

type PolicyStateType string

type PolicyState struct {
	Type     PolicyStateType `json:"type"`
	Messages []string        `json:"messages"`
}

type PolicyStatesBuilder struct {
	policyStatesBuilder map[string]*PolicyStateBuilder
}

func NewPolicyStatesBuilder() *PolicyStatesBuilder {
	return &PolicyStatesBuilder{
		policyStatesBuilder: make(map[string]*PolicyStateBuilder),
	}
}

func (psb *PolicyStatesBuilder) ForPolicy(policyName string) *PolicyStateBuilder {
	if value, ok := psb.policyStatesBuilder[policyName]; ok {
		return value
	}
	policyStatusBuilder := NewPolicyStateBuilder(policyName)
	psb.policyStatesBuilder[policyName] = policyStatusBuilder
	return policyStatusBuilder
}

func (psb *PolicyStatesBuilder) Build() []PolicyStateItem {
	policyStates := make([]PolicyStateItem, len(psb.policyStatesBuilder))
	i := 0
	for _, policyStateBuilder := range psb.policyStatesBuilder {
		policyStates[i] = policyStateBuilder.Build()
		i++
	}

	return policyStates
}

type NodeSetResourcesWithMeta struct {
	Hash                 string      `json:"hash,omitempty"`
	LastModificationTime metav1.Time `json:"lastModificationTime"`
	nodesets.NodeSetResources
}

type NodeSetsResourcesWithMeta []NodeSetResourcesWithMeta

func (s NodeSetsResourcesWithMeta) ByNodeSet() map[string]NodeSetResourcesWithMeta {
	byNodeSet := make(map[string]NodeSetResourcesWithMeta)
	for _, nodeSetResources := range s {
		nodeSetResources := nodeSetResources
		byNodeSet[nodeSetResources.Name] = nodeSetResources
	}
	return byNodeSet
}

func GetAutoscalingStatus(es esv1.Elasticsearch) (NodeSetsResourcesWithMeta, error) {
	status := Status{}
	if es.Annotations == nil {
		return status.NodeSetResources, nil
	}
	serializedStatus, ok := es.Annotations[ElasticsearchAutoscalingStatusAnnotationName]
	if !ok {
		return NodeSetsResourcesWithMeta{}, nil
	}
	err := json.Unmarshal([]byte(serializedStatus), &status)
	return status.NodeSetResources, err
}

func UpdateAutoscalingStatus(
	es *esv1.Elasticsearch,
	statusBuilder *PolicyStatesBuilder,
	nextNodeSetsResources nodesets.NodeSetsResources,
	actualNodeSetsStatus NodeSetsResourcesWithMeta,
) error {
	status := Status{
		PolicyStates: statusBuilder.Build(),
	}
	byNodeSetsNextResources := nextNodeSetsResources.ByNodeSet()
	byNodeSetsActualResources := actualNodeSetsStatus.ByNodeSet()
	now := metav1.Now()
	for _, nodeSet := range es.Spec.NodeSets {
		nodeSetResource, ok := byNodeSetsNextResources[nodeSet.Name]
		if !ok {
			// nodeSet not managed by the autoscaler
			continue
		}
		nextHash := ResourcesHash(nodeSet)
		if previousResources, exists := byNodeSetsActualResources[nodeSet.Name]; exists &&
			previousResources.Hash == nextHash {
			// resources have not been updated, reuse previous resources
			status.NodeSetResources = append(status.NodeSetResources, previousResources)
			continue
		}

		status.NodeSetResources = append(status.NodeSetResources,
			NodeSetResourcesWithMeta{
				Hash:                 ResourcesHash(nodeSet),
				LastModificationTime: now,
				NodeSetResources:     nodeSetResource,
			})

	}
	if es.Annotations == nil {
		es.Annotations = make(map[string]string)
	}
	serializedStatus, err := json.Marshal(&status)
	if err != nil {
		return err
	}
	es.Annotations[ElasticsearchAutoscalingStatusAnnotationName] = string(serializedStatus)
	return nil
}

// ResourcesHash computes a reliable hash of the resources spec in a nodeSet.
// The lack of reliability may come from the fact that Quantity.String() has a side effect on the
// struct, and hence any hash computation must ensure that Quantity.String() is called in a consistent
// when the hash is computed.
func ResourcesHash(nodeSet esv1.NodeSet) string {
	configChecksum := adler32.New()
	_, _ = configChecksum.Write([]byte{byte(nodeSet.Count)})
	for _, container := range nodeSet.PodTemplate.Spec.Containers {
		if container.Name != esv1.ElasticsearchContainerName {
			continue
		}
		for _, resourceName := range orderedResourceNames(container.Resources.Requests) {
			_, _ = configChecksum.Write([]byte(resourceName))
			resourceQuantity := container.Resources.Requests[resourceName]
			_, _ = configChecksum.Write([]byte(resourceQuantity.String()))
		}
		for _, resourceName := range orderedResourceNames(container.Resources.Limits) {
			_, _ = configChecksum.Write([]byte(resourceName))
			resourceQuantity := container.Resources.Requests[resourceName]
			_, _ = configChecksum.Write([]byte(resourceQuantity.String()))
		}
	}
	for _, volumeClaim := range orderVolumeClaimTemplates(nodeSet.VolumeClaimTemplates) {
		_, _ = configChecksum.Write([]byte(volumeClaim.Name))
		for _, storageResourceName := range orderedResourceNames(volumeClaim.Spec.Resources.Requests) {
			_, _ = configChecksum.Write([]byte(storageResourceName))
			resourceQuantity := volumeClaim.Spec.Resources.Requests[storageResourceName]
			_, _ = configChecksum.Write([]byte(resourceQuantity.String()))
		}

	}
	return fmt.Sprintf("%x", configChecksum.Sum(nil))
}

func orderVolumeClaimTemplates(pvc []corev1.PersistentVolumeClaim) []corev1.PersistentVolumeClaim {
	sort.Slice(pvc, func(i, j int) bool {
		return pvc[i].String() < pvc[j].String()
	})
	return pvc
}

func orderedResourceNames(m v1.ResourceList) []v1.ResourceName {
	keys := make([]v1.ResourceName, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	return keys
}
