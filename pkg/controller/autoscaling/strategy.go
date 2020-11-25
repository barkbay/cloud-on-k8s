// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"fmt"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

// ensureResourcePolicies ensures that even if no decisions have been returned the nodeSet respect
// the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func ensureResourcePolicies(
	nodeSets []v1.NodeSet,
	containerName string,
	policy commonv1.ResourcePolicy,
) ([]v1.NodeSet, error) {
	updatedNodeSets := make([]v1.NodeSet, len(nodeSets))
	for i, nodeSet := range nodeSets {
		updatedNodeSets[i] = *nodeSet.DeepCopy()
	}

	for i := range updatedNodeSets {
		// ensure that the min. number of nodes is set
		if updatedNodeSets[i].Count < *policy.MinAllowed.Count {
			updatedNodeSets[i].Count = *policy.MinAllowed.Count
		} else if updatedNodeSets[i].Count > *policy.MaxAllowed.Count {
			updatedNodeSets[i].Count = *policy.MaxAllowed.Count
		}

		container, containers := getContainer(containerName, updatedNodeSets[i].PodTemplate.Spec.Containers)
		if container == nil {
			container = &corev1.Container{
				Name: containerName,
			}
		}

		if container.Resources.Requests == nil {
			container.Resources.Requests = corev1.ResourceList{}
		}

		if memoryRequirement, exist := container.Resources.Requests[corev1.ResourceMemory]; !exist ||
			memoryRequirement.Cmp(*policy.MinAllowed.Memory) < 0 {
			container.Resources.Requests[corev1.ResourceMemory] = *policy.MinAllowed.Memory
		} else if memoryRequirement.Cmp(*policy.MaxAllowed.Memory) > 0 {
			container.Resources.Requests[corev1.ResourceMemory] = *policy.MaxAllowed.Memory
		}

		if cpuRequirement, exist := container.Resources.Requests[corev1.ResourceCPU]; !exist ||
			cpuRequirement.Cmp(*policy.MinAllowed.Cpu) < 0 {
			container.Resources.Requests[corev1.ResourceCPU] = *policy.MinAllowed.Cpu
		} else if cpuRequirement.Cmp(*policy.MaxAllowed.Cpu) > 0 {
			container.Resources.Requests[corev1.ResourceCPU] = *policy.MaxAllowed.Cpu
		}

		// Update limits
		container.Resources.Limits = corev1.ResourceList{
			corev1.ResourceMemory: container.Resources.Requests[corev1.ResourceMemory],
		}

		updatedNodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)
	}

	return updatedNodeSets, nil
}

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

	// Check if overall tier requirement is higher than node requirement.
	// This is done to check if we can fulfil the tier requirement only by scaling vertically
	minNodesCount := int64(*policy.MinAllowed.Count) * int64(len(nodeSets))
	// Tiers memory capacity distributed on min. nodes
	memoryOverAllTiers := *requiredCapacity.Tier.Memory / minNodesCount
	requiredMemoryCapacity := max64(
		*requiredCapacity.Node.Memory,
		roundUp(memoryOverAllTiers, giga),
	)

	// Set desired memory capacity within the allowed range
	if requiredMemoryCapacity < policy.MinAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		requiredMemoryCapacity = policy.MinAllowed.Memory.Value()
	}
	if requiredMemoryCapacity > policy.MaxAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		requiredMemoryCapacity = policy.MaxAllowed.Memory.Value()
	}

	// Build the ideal node capacity
	nodeCapacity := client.Capacity{
		Memory: &requiredMemoryCapacity,
	}

	// Prepare the resource storage
	var resourceStorage *resource.Quantity
	if requiredCapacity.Tier.Storage != nil && requiredCapacity.Node.Storage != nil {
		// Tiers storage capacity distributed on min. nodes
		storageOverAllTiers := *requiredCapacity.Tier.Storage / minNodesCount
		requiredStorageCapacity := max64(
			*requiredCapacity.Node.Storage,
			roundUp(storageOverAllTiers, giga),
		)
		// Set desired storage capacity within the allowed range
		if requiredStorageCapacity < policy.MinAllowed.Storage.Value() {
			// The amount of storage requested by Elasticsearch is less than the min. allowed value
			requiredStorageCapacity = policy.MinAllowed.Storage.Value()
		}
		if nodeCapacity.Storage != nil && *nodeCapacity.Storage > policy.MaxAllowed.Storage.Value() {
			// The amount of storage requested by Elasticsearch is more than the max. allowed value
			requiredStorageCapacity = policy.MaxAllowed.Storage.Value()
		}
		if requiredStorageCapacity >= giga && requiredStorageCapacity%giga == 0 {
			// When it's possible we may want to express the memory with a "human readable unit" like the the Gi unit
			resourceStorageAsGiga := resource.MustParse(fmt.Sprintf("%dGi", requiredStorageCapacity/giga))
			resourceStorage = &resourceStorageAsGiga
		} else {
			resourceStorage = resource.NewQuantity(requiredStorageCapacity, resource.DecimalSI)
		}
	}

	for i := range nodeSets {
		container, containers := getContainer(containerName, nodeSets[i].PodTemplate.Spec.Containers)
		if container == nil {
			container = &corev1.Container{
				Name: containerName,
			}
		}

		var resourceMemory *resource.Quantity
		if *nodeCapacity.Memory >= giga && *nodeCapacity.Memory%giga == 0 {
			// When it's possible we may want to express the memory with a "human readable unit" like the the Gi unit
			resourceMemoryAsGiga := resource.MustParse(fmt.Sprintf("%dGi", *nodeCapacity.Memory/giga))
			resourceMemory = &resourceMemoryAsGiga
		} else {
			resourceMemory = resource.NewQuantity(*nodeCapacity.Memory, resource.DecimalSI)
		}

		// Update requests
		container.Resources.Requests = corev1.ResourceList{
			corev1.ResourceMemory: *resourceMemory,
		}
		if policy.MinAllowed.Cpu != nil && policy.MaxAllowed.Cpu != nil {
			container.Resources.Requests[corev1.ResourceCPU] = *cpuFromMemory(requiredMemoryCapacity, policy)
		}

		// Update limits
		container.Resources.Limits = corev1.ResourceList{
			corev1.ResourceMemory: *resourceMemory,
		}

		// Update storage claim
		if len(nodeSets[i].VolumeClaimTemplates) == 0 {
			nodeSets[i].VolumeClaimTemplates = []corev1.PersistentVolumeClaim{volume.DefaultDataVolumeClaim}
		}
		for _, claimTemplate := range nodeSets[i].VolumeClaimTemplates {
			if claimTemplate.Name == volume.ElasticsearchDataVolumeName &&
				claimTemplate.Spec.Resources.Requests != nil {
				previousStorageCapacity, ok := claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
				if !ok {
					break
				}
				if !previousStorageCapacity.Equal(*resourceStorage) {
					log.V(1).Info("Increase storage capacity", "node_set", nodeSets[i].Name, "current_capacity", previousStorageCapacity, "new_capacity", *resourceStorage)
					claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage] = *resourceStorage
				}
			}
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
