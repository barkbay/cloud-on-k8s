// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

func applyScaleDecision(
	nodeSets []v1.NodeSet,
	container string,
	requiredCapacity client.RequiredCapacity,
	policy commonv1.ScalePolicy,
) ([]v1.NodeSet, error) {
	updatedNodeSets := make([]v1.NodeSet, len(nodeSets))
	for i, nodeSet := range nodeSets {
		updatedNodeSets[i] = *nodeSet.DeepCopy()
	}

	// 1. Scale vertically
	nodeCapacity := scaleVertically(updatedNodeSets, container, requiredCapacity.Node, policy)
	// 2. Scale horizontally
	if err := scaleHorizontally(updatedNodeSets, requiredCapacity.Tier, nodeCapacity, policy); err != nil {
		return updatedNodeSets, err
	}

	return updatedNodeSets, nil
}

// scaleVertically vertically scales nodeSet to match the per node requirements
func scaleVertically(
	nodeSets []v1.NodeSet,
	containerName string,
	requestedCapacity client.Capacity,
	policy commonv1.ScalePolicy,
) client.Capacity {
	nodeCapacity := client.Capacity{
		Storage: nil,
		Memory:  requestedCapacity.Memory,
	}
	if requestedCapacity.Memory.Cmp(*policy.MinAllowed.Memory) == -1 {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		nodeCapacity.Memory = policy.MinAllowed.Memory
	}
	if requestedCapacity.Memory.Cmp(*policy.MaxAllowed.Memory) == 1 {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		nodeCapacity.Memory = policy.MaxAllowed.Memory
	}
	for i := range nodeSets {
		container, containers := getContainer(containerName, nodeSets[i].PodTemplate.Spec.Containers)
		if container == nil {
			container = &corev1.Container{
				Name: containerName,
			}
		}
		container.Resources.Requests = corev1.ResourceList{
			corev1.ResourceMemory: *nodeCapacity.Memory,
		}
		nodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)
	}
	return nodeCapacity
}

var zero = *resource.NewQuantity(0, resource.DecimalSI)

// scaleHorizontally
// nodeCapacity is the node capacity as computed by scaleVertically
func scaleHorizontally(
	nodeSets []v1.NodeSet,
	requestedCapacity client.Capacity,
	nodeCapacity client.Capacity,
	policy commonv1.ScalePolicy,
) error {
	// Compute current memory for all the nodeSets and scale horizontally accordingly
	currentMemory := zero.DeepCopy()
	currentCount := 0
	nodeMemoryCapacity := nodeCapacity.Memory.DeepCopy()
	for _, nodeSet := range nodeSets {
		for i := int32(0); i < nodeSet.Count; i++ {
			currentMemory.Add(nodeMemoryCapacity)
			currentCount++
		}
	}

	// memoryDelta holds the memory variation, it can be:
	// * a positive value if some memory needs to be added
	// * a negative value if some memory can be reclaimed
	memoryDelta := requestedCapacity.Memory.DeepCopy()
	// Substract existing (or planned) currentMemory
	memoryDelta.Sub(currentMemory)

	log.V(1).Info(
		"Memory status",
		"tier", policy.Roles,
		"current", currentMemory,
		"target", requestedCapacity.Memory,
		"missing", memoryDelta,
	)

	nodeToAdd := getNodeDelta(memoryDelta, nodeMemoryCapacity, currentMemory, *requestedCapacity.Memory)

	switch {
	case nodeToAdd > 0:
		nodeToAdd = min(int(*policy.MaxAllowed.Count)-currentCount, nodeToAdd)
		log.V(1).Info("Need to add nodes", "to_add", nodeToAdd)
		fnm := NewFairNodesManager(nodeSets)
		for nodeToAdd > 0 {
			fnm.AddNode()
			nodeToAdd--
		}
	case nodeToAdd < 0:
		nodeToAdd = max(int(*policy.MinAllowed.Count)-currentCount, nodeToAdd)
		log.V(1).Info("Need to remove nodes", "to_remove", nodeToAdd)
		fnm := NewFairNodesManager(nodeSets)
		for nodeToAdd < 0 {
			fnm.RemoveNode()
			nodeToAdd++
		}
	}

	return nil
}

func getNodeDelta(memoryDelta, nodeMemoryCapacity, currentMemory, target resource.Quantity) int {
	nodeToAdd := 0
	switch sign := memoryDelta.Sign(); {
	case sign > 0:
		for memoryDelta.Sign() > 0 {
			memoryDelta.Sub(nodeMemoryCapacity)
			// Compute how many nodes should be added
			nodeToAdd++
		}
	case sign < 0:
		currentMemory.Sub(nodeMemoryCapacity)
		for currentMemory.Cmp(target) > 0 {
			nodeToAdd--
			currentMemory.Sub(nodeMemoryCapacity)
		}
	}
	return nodeToAdd
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func getContainer(name string, containers []corev1.Container) (*corev1.Container, []corev1.Container) {
	for i, container := range containers {
		if container.Name == name {
			// Remove the container
			return &container, append(containers[:i], containers[i+1:]...)
		}
	}
	return nil, containers
}
