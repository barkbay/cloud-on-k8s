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
	nodeCapacity := scaleVertically(updatedNodeSets, container, requiredCapacity, policy)
	// 2. Scale horizontally
	if err := scaleHorizontally(updatedNodeSets, requiredCapacity.Tier, nodeCapacity, policy); err != nil {
		return updatedNodeSets, err
	}

	return updatedNodeSets, nil
}

var giga = int64(1024 * 1024 * 1024)

// scaleVertically vertically scales nodeSet to match the per node requirements.
func scaleVertically(
	nodeSets []v1.NodeSet,
	containerName string,
	requiredCapacity client.RequiredCapacity,
	policy commonv1.ResourcePolicy,
) client.Capacity {

	// Check if overall tier requirement is higher than node requirement
	minNodesCount := int64(*policy.MinAllowed.Count) * int64(len(nodeSets))
	// Tiers capacity distributed on min. nodes
	tiers := *requiredCapacity.Tier.Memory / minNodesCount
	roundedTiers := roundUp(tiers, giga)
	requiredMemoryCapacity := max64(
		*requiredCapacity.Node.Memory,
		roundedTiers,
	)

	// 1. Set desired memory capacity within the allowed range
	nodeCapacity := client.Capacity{
		Storage: nil,
		Memory:  &requiredMemoryCapacity,
	}
	if requiredMemoryCapacity < policy.MinAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		requiredMemoryCapacity = policy.MinAllowed.Memory.Value()
	}
	if requiredMemoryCapacity > policy.MaxAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		requiredMemoryCapacity = policy.MaxAllowed.Memory.Value()
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
			corev1.ResourceCPU:    *cpuFromMemory(requiredMemoryCapacity, policy),
		}
		nodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)
	}

	return nodeCapacity
}

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	nodeSets []v1.NodeSet,
	requestedCapacity client.Capacity,
	nodeCapacity client.Capacity,
	policy commonv1.ResourcePolicy,
) error {
	// reset all the nodeSets the minimum
	for i := range nodeSets {
		nodeSets[i].Count = *policy.MinAllowed.Count
	}
	// scaleHorizontally always start from the min number of nodes and add nodes as necessary
	minNodes := len(nodeSets) * int(*policy.MinAllowed.Count)
	minMemory := int64(minNodes) * (*nodeCapacity.Memory)

	// memoryDelta holds the memory variation, it can be:
	// * a positive value if some memory needs to be added
	// * a negative value if some memory can be reclaimed
	memoryDelta := *requestedCapacity.Memory - minMemory
	nodeToAdd := getNodeDelta(memoryDelta, *nodeCapacity.Memory, minMemory, *requestedCapacity.Memory)

	log.V(1).Info(
		"Memory status",
		"tier", policy.Roles,
		"tier_target", requestedCapacity.Memory,
		"node_target", minNodes+nodeToAdd,
	)

	if nodeToAdd > 0 {
		nodeToAdd = min(int(*policy.MaxAllowed.Count)-minNodes, nodeToAdd)
		log.V(1).Info("Need to add nodes", "to_add", nodeToAdd)
		fnm := NewFairNodesManager(nodeSets)
		for nodeToAdd > 0 {
			fnm.AddNode()
			nodeToAdd--
		}
	}

	return nil
}

func getNodeDelta(memoryDelta, nodeMemoryCapacity, currentMemory, target int64) int {
	nodeToAdd := 0
	if memoryDelta < 0 {
		return 0
	}

	for memoryDelta > 0 {
		memoryDelta -= nodeMemoryCapacity
		// Compute how many nodes should be added
		nodeToAdd++
	}
	return nodeToAdd
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func maxQuantity(q1, q2 *resource.Quantity) resource.Quantity {
	if q1.Value() > q2.Value() {
		return *q1
	}
	return *q2
}

func max64(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func roundUp(v, n int64) int64 {
	return v + n - v%n
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
