// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package resources

import (
	"fmt"

	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
)

// NamedTierResources models for all the nodeSets managed by a same autoscaling policy:
// * the resources (cpu, memory, storage) expected the nodeSet specifications
// * the individual number of nodes (count) in each nodeSet
type NamedTierResources struct {
	Name string `json:"name"`
	// NodeSetNodeCount holds the number of nodes for each nodeSet.
	NodeSetNodeCount NodeSetNodeCountList `json:"nodeSets"`
	// ResourcesSpecification holds the resource values common to all the nodeSet managed by a same autoscaling policy.
	ResourcesSpecification
}

func NewNamedTierResources(name string, nodeSetNames []string) NamedTierResources {
	return NamedTierResources{
		Name:             name,
		NodeSetNodeCount: newNodeSetNodeCountList(nodeSetNames),
	}
}

type ClusterResources []NamedTierResources

// IsUsedBy returns true if the nodeSet resources matches the one specified in the NodeSetResources
func (ntr NamedTierResources) IsUsedBy(containerName string, nodeSet v1.NodeSet) (bool, error) {
	for _, nodeCount := range ntr.NodeSetNodeCount {
		if nodeCount.Name != nodeSet.Name {
			continue
		}
		if nodeCount.NodeCount != nodeSet.Count {
			return false, nil
		}

		// Compare volume request
		switch len(nodeSet.VolumeClaimTemplates) {
		case 0:
			if ntr.HasRequest(corev1.ResourceStorage) {
				return false, nil
			}
		case 1:
			volumeClaim := nodeSet.VolumeClaimTemplates[0]
			if !ResourcesEqual(corev1.ResourceStorage, ntr.ResourcesSpecification.Requests, volumeClaim.Spec.Resources.Requests) {
				return false, nil
			}
		default:
			return false, fmt.Errorf("only 1 volume claim template is allowed when autoscaling is enabled, got %d in nodeSet %s", len(nodeSet.VolumeClaimTemplates), nodeSet.Name)
		}

		// Compare CPU and Memory requests
		container := getContainer(containerName, nodeSet.PodTemplate.Spec.Containers)
		if container == nil {
			return false, nil
		}
		return ResourcesEqual(corev1.ResourceMemory, ntr.ResourcesSpecification.Requests, container.Resources.Requests) &&
			ResourcesEqual(corev1.ResourceCPU, ntr.ResourcesSpecification.Requests, container.Resources.Requests), nil
	}
	return false, nil
}

func ResourcesEqual(resourceName corev1.ResourceName, expected, current corev1.ResourceList) bool {
	if len(expected) == 0 {
		// No value expected, return true
		return true
	}
	expectedValue, hasExpectedValue := expected[resourceName]
	if !hasExpectedValue {
		// Expected values does not contain the resource
		return true
	}
	if len(current) == 0 {
		// Value is expected but current is nil or empty
		return false
	}
	currentValue, hasCurrentValue := current[resourceName]
	if !hasCurrentValue {
		// Current values does not contain the resource
		return false
	}
	return expectedValue.Equal(currentValue)
}

func getContainer(name string, containers []corev1.Container) *corev1.Container {
	for i := range containers {
		container := containers[i]
		if container.Name == name {
			// Remove the container
			return &container
		}
	}
	return nil
}

type NodeSetNodeCount struct {
	// NodeSet name.
	Name string `json:"name"`
	// NodeCount is the number of nodes, as computed by the autoscaler, expected in this NodeSet.
	NodeCount int32 `json:"nodeCount"`
}
type NodeSetNodeCountList []NodeSetNodeCount

func (n NodeSetNodeCountList) TotalNodeCount() int32 {
	var totalNodeCount int32
	for _, nodeSet := range n {
		totalNodeCount += nodeSet.NodeCount
	}
	return totalNodeCount
}

func (n NodeSetNodeCountList) ByNodeSet() map[string]int32 {
	byNodeSet := make(map[string]int32)
	for _, nodeSet := range n {
		byNodeSet[nodeSet.Name] = nodeSet.NodeCount
	}
	return byNodeSet
}

func newNodeSetNodeCountList(nodeSetNames []string) NodeSetNodeCountList {
	nodeSetNodeCount := make([]NodeSetNodeCount, len(nodeSetNames))
	for i := range nodeSetNames {
		nodeSetNodeCount[i] = NodeSetNodeCount{Name: nodeSetNames[i]}
	}
	return nodeSetNodeCount
}

// ResourcesSpecification holds the result of the autoscaling algorithm.
type ResourcesSpecification struct {
	Limits   corev1.ResourceList `json:"limits,omitempty"`
	Requests corev1.ResourceList `json:"requests,omitempty"`
}

// MaxMerge merge the specified resource into the ResourcesSpecification only if its quantity is greater
// than the existing one.
func (rs *ResourcesSpecification) MaxMerge(
	other corev1.ResourceRequirements,
	resourceName corev1.ResourceName,
) {
	// Requests
	otherResourceRequestValue, otherHasResourceRequest := other.Requests[resourceName]
	if otherHasResourceRequest {
		if rs.Requests == nil {
			rs.Requests = make(corev1.ResourceList)
		}
		receiverValue, receiverHasResource := rs.Requests[resourceName]
		if !receiverHasResource {
			rs.Requests[resourceName] = otherResourceRequestValue
		} else if otherResourceRequestValue.Cmp(receiverValue) > 0 {
			rs.Requests[resourceName] = otherResourceRequestValue
		}
	}

	// Limits
	otherResourceLimitValue, otherHasResourceLimit := other.Limits[resourceName]
	if otherHasResourceLimit {
		if rs.Limits == nil {
			rs.Limits = make(corev1.ResourceList)
		}
		receiverValue, receiverHasResource := rs.Limits[resourceName]
		if !receiverHasResource {
			rs.Limits[resourceName] = otherResourceLimitValue
		} else if otherResourceLimitValue.Cmp(receiverValue) > 0 {
			rs.Limits[resourceName] = otherResourceLimitValue
		}
	}
}

func (rs *ResourcesSpecification) SetRequest(resourceName corev1.ResourceName, quantity resource.Quantity) {
	if rs.Requests == nil {
		rs.Requests = make(corev1.ResourceList)
	}
	rs.Requests[resourceName] = quantity
}

func (rs *ResourcesSpecification) SetLimit(resourceName corev1.ResourceName, quantity resource.Quantity) {
	if rs.Limits == nil {
		rs.Limits = make(corev1.ResourceList)
	}
	rs.Limits[resourceName] = quantity
}

func (rs *ResourcesSpecification) HasRequest(resourceName corev1.ResourceName) bool {
	if rs.Requests == nil {
		return false
	}
	_, hasRequest := rs.Requests[resourceName]
	return hasRequest
}

func (rs *ResourcesSpecification) GetRequest(resourceName corev1.ResourceName) resource.Quantity {
	return rs.Requests[resourceName]
}

// ResourceList is a set of (resource name, quantity) pairs.
type ResourceListInt64 map[corev1.ResourceName]int64

// ResourcesSpecificationInt64 is mostly use in logs to print comparable values.
type ResourcesSpecificationInt64 struct {
	Requests ResourceListInt64 `json:"requests,omitempty"`
	Limits   ResourceListInt64 `json:"limits,omitempty"`
}

// ToInt64 converts all the resource quantities to int64, mostly to be logged and build dashboard.
func (rs ResourcesSpecification) ToInt64() ResourcesSpecificationInt64 {
	rs64 := ResourcesSpecificationInt64{
		Requests: make(ResourceListInt64),
		Limits:   make(ResourceListInt64),
	}
	for resource, value := range rs.Requests {
		switch resource {
		case corev1.ResourceCPU:
			rs64.Requests[resource] = value.MilliValue()
		default:
			rs64.Requests[resource] = value.Value()
		}
	}
	for resource, value := range rs.Limits {
		switch resource {
		case corev1.ResourceCPU:
			rs64.Requests[resource] = value.MilliValue()
		default:
			rs64.Requests[resource] = value.Value()
		}
	}
	return rs64
}

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

type NodeSetResources struct {
	NodeCount int32
	*NamedTierResources
}

func (ntr NamedTierResources) SameResources(other NamedTierResources) bool {
	thisByName := ntr.NodeSetNodeCount.ByNodeSet()
	otherByName := other.NodeSetNodeCount.ByNodeSet()
	if len(thisByName) != len(otherByName) {
		return false
	}
	for nodeSet, nodeCount := range thisByName {
		otherNodeCount, ok := otherByName[nodeSet]
		if !ok || nodeCount != otherNodeCount {
			return false
		}
	}
	return equality.Semantic.DeepEqual(ntr.ResourcesSpecification, other.ResourcesSpecification)
}

func (cr ClusterResources) ByNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources)
	for i := range cr {
		nodeSetsResource := cr[i]
		for j := range nodeSetsResource.NodeSetNodeCount {
			nodeSetNodeCount := nodeSetsResource.NodeSetNodeCount[j]
			nodeSetResources := NodeSetResources{
				NodeCount:          nodeSetNodeCount.NodeCount,
				NamedTierResources: &nodeSetsResource,
			}
			byNodeSet[nodeSetNodeCount.Name] = nodeSetResources
		}
	}
	return byNodeSet
}

func (cr ClusterResources) ByAutoscalingPolicy() map[string]NamedTierResources {
	byNamedTier := make(map[string]NamedTierResources)
	for _, namedTierResources := range cr {
		byNamedTier[namedTierResources.Name] = namedTierResources
	}
	return byNamedTier
}
