// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

// applyScaleDecision implements a "scale vertically" first scaling strategy.
func applyScaleDecision(
	nodeSets []v1.NodeSet,
	container string,
	requiredCapacity client.RequiredCapacity,
	policy commonv1.ResourcePolicy,
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

// scaleVertically vertically scales nodeSet to match the per node requirements.
func scaleVertically(
	nodeSets []v1.NodeSet,
	containerName string,
	requestedCapacity client.Capacity,
	policy commonv1.ResourcePolicy,
) client.Capacity {
	nodeCapacity := client.Capacity{
		Storage: nil,
		Memory:  requestedCapacity.Memory,
	}
	if *requestedCapacity.Memory < policy.MinAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		*nodeCapacity.Memory = policy.MinAllowed.Memory.Value()
	}
	if *requestedCapacity.Memory > policy.MaxAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		*nodeCapacity.Memory = policy.MaxAllowed.Memory.Value()
	}
	for i := range nodeSets {
		container, containers := getContainer(containerName, nodeSets[i].PodTemplate.Spec.Containers)
		if container == nil {
			container = &corev1.Container{
				Name: containerName,
			}
		}
		resourceMemory := resource.NewQuantity(*nodeCapacity.Memory, resource.DecimalSI)
		container.Resources.Requests = corev1.ResourceList{
			corev1.ResourceMemory: *resourceMemory,
		}
		nodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)
	}
	return nodeCapacity
}

var zero = *resource.NewQuantity(0, resource.DecimalSI)

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	nodeSets []v1.NodeSet,
	requestedCapacity client.Capacity,
	nodeCapacity client.Capacity,
	policy commonv1.ResourcePolicy,
) error {
	// Compute current memory for all the currentNodeSets and scale horizontally accordingly

	var currentMemory int64
	currentCount := 0
	for _, nodeSet := range nodeSets {
		currentMemory += *nodeCapacity.Memory * int64(nodeSet.Count)
		currentCount += int(nodeSet.Count)
	}

	// memoryDelta holds the memory variation, it can be:
	// * a positive value if some memory needs to be added
	// * a negative value if some memory can be reclaimed
	memoryDelta := *requestedCapacity.Memory - currentMemory

	log.V(1).Info(
		"Memory status",
		"tier", policy.Roles,
		"current", currentMemory,
		"target", requestedCapacity.Memory,
		"missing", memoryDelta,
	)

	nodeToAdd := getNodeDelta(memoryDelta, *nodeCapacity.Memory, currentMemory, *requestedCapacity.Memory)

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

func getNodeDelta(memoryDelta, nodeMemoryCapacity, currentMemory, target int64) int {
	nodeToAdd := 0
	switch {
	case memoryDelta > 0:
		for memoryDelta > 0 {
			memoryDelta -= nodeMemoryCapacity
			// Compute how many nodes should be added
			nodeToAdd++
		}
	case memoryDelta < 0:
		for currentMemory > target {
			nodeToAdd--
			currentMemory -= nodeMemoryCapacity
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
