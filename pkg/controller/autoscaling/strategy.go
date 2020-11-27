// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"fmt"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type NodeSetsResources []NodeSetResources

type NodeSetResources struct {
	Name  string `json:"name,omitempty"`
	Count int32  `json:"count,omitempty"`
	commonv1.ResourcesSpecification
}

func (nsr NodeSetsResources) byNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources, len(nsr))
	for _, nodeSetsResource := range nsr {
		byNodeSet[nodeSetsResource.Name] = nodeSetsResource
	}
	return byNodeSet
}

// ensureResourcePolicies ensures that even if no decisions have been returned the nodeSet respect
// the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func ensureResourcePolicies(
	nodeSets []v1.NodeSet,
	policy commonv1.ResourcePolicy,
	autoscalingStatus Status,
) NodeSetsResources {
	nodeSetsResources := make(NodeSetsResources, len(nodeSets))
	statusByNodeSet := autoscalingStatus.ByNodeSet()
	for _, nodeSet := range nodeSets {
		nodeSetResources := NodeSetResources{
			Name:  nodeSet.Name,
			Count: *policy.MinAllowed.Count,
			ResourcesSpecification: commonv1.ResourcesSpecification{
				Count:   policy.MinAllowed.Count,
				Cpu:     policy.MinAllowed.Cpu,
				Memory:  policy.MinAllowed.Memory,
				Storage: policy.MinAllowed.Storage,
			},
		}
		nodeSetStatus, ok := statusByNodeSet[nodeSet.Name]
		if !ok {
			// No current status for this nodeSet, create a new one with minimums
			nodeSetsResources = append(nodeSetsResources, nodeSetResources)
			continue
		}

		// ensure that the min. number of nodes is set
		if nodeSetStatus.Count < *policy.MinAllowed.Count {
			nodeSetResources.Count = *policy.MinAllowed.Count
		} else if nodeSetStatus.Count > *policy.MaxAllowed.Count {
			nodeSetResources.Count = *policy.MaxAllowed.Count
		}

		// Ensure memory settings are in the allowed range
		if nodeSetStatus.Memory != nil && policy.MinAllowed.Memory != nil && policy.MaxAllowed.Memory != nil {
			if nodeSetStatus.Memory.Cmp(*policy.MinAllowed.Memory) < 0 {
				nodeSetStatus.Memory = policy.MinAllowed.Memory
			} else if nodeSetStatus.Memory.Cmp(*policy.MaxAllowed.Memory) > 0 {
				nodeSetStatus.Memory = policy.MaxAllowed.Memory
			}
		}

		// Ensure CPU settings are in the allowed range
		if nodeSetStatus.Cpu != nil && policy.MinAllowed.Cpu != nil && policy.MaxAllowed.Cpu != nil {
			if nodeSetStatus.Cpu.Cmp(*policy.MinAllowed.Cpu) < 0 {
				nodeSetStatus.Cpu = policy.MinAllowed.Cpu
			} else if nodeSetStatus.Cpu.Cmp(*policy.MaxAllowed.Cpu) > 0 {
				nodeSetStatus.Cpu = policy.MaxAllowed.Cpu
			}
		}

		// Ensure Storage settings are in the allowed range
		if nodeSetStatus.Storage != nil && policy.MinAllowed.Storage != nil && policy.MaxAllowed.Storage != nil {
			if nodeSetStatus.Storage.Cmp(*policy.MinAllowed.Storage) < 0 {
				nodeSetStatus.Storage = policy.MinAllowed.Storage
			} else if nodeSetStatus.Storage.Cmp(*policy.MaxAllowed.Storage) > 0 {
				nodeSetStatus.Storage = policy.MaxAllowed.Storage
			}
		}

		nodeSetsResources = append(nodeSetsResources, nodeSetResources)
	}

	return nodeSetsResources
}

func getScaleDecision(
	nodeSets []v1.NodeSet,
	requiredCapacity client.RequiredCapacity,
	policy commonv1.ResourcePolicy,
) NodeSetsResources {
	// 1. Scale vertically
	desiredNodeResources := scaleVertically(nodeSets, requiredCapacity, policy)
	// 2. Scale horizontally
	return scaleHorizontally(nodeSets, requiredCapacity.Tier, desiredNodeResources, policy)
}

var giga = int64(1024 * 1024 * 1024)

// scaleVertically computes desired state for a node given the requested capacity from ES and the resource policy
// specified by the user.
// It attempts to scale all the resources vertically until the expectations are met.
func scaleVertically(
	nodeSets []v1.NodeSet,
	requiredCapacity client.RequiredCapacity,
	policy commonv1.ResourcePolicy,
) commonv1.ResourcesSpecification {

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
		if requiredStorageCapacity > policy.MaxAllowed.Storage.Value() {
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

	var resourceMemory *resource.Quantity
	if requiredMemoryCapacity >= giga && requiredMemoryCapacity%giga == 0 {
		// When it's possible we may want to express the memory with a "human readable unit" like the the Gi unit
		resourceMemoryAsGiga := resource.MustParse(fmt.Sprintf("%dGi", requiredMemoryCapacity/giga))
		resourceMemory = &resourceMemoryAsGiga
	} else {
		resourceMemory = resource.NewQuantity(requiredMemoryCapacity, resource.DecimalSI)
	}

	nodeResourcesSpecification := commonv1.ResourcesSpecification{
		Memory:  resourceMemory,
		Storage: resourceStorage,
	}

	if policy.MaxAllowed.Cpu != nil && policy.MinAllowed.Cpu != nil {
		nodeResourcesSpecification.Cpu = cpuFromMemory(requiredMemoryCapacity, policy)
	}

	return nodeResourcesSpecification
}

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	nodeSets []v1.NodeSet,
	requestedCapacity client.Capacity,
	nodeCapacity commonv1.ResourcesSpecification,
	policy commonv1.ResourcePolicy,
) NodeSetsResources {
	nodeSetsResources := make(NodeSetsResources, len(nodeSets))
	for i, nodeSet := range nodeSets {
		nodeSetResources := *nodeCapacity.DeepCopy()
		nodeSetsResources[i] = NodeSetResources{
			Name: nodeSet.Name,
			// set all the nodeSets count the minimum
			Count:                  *policy.MinAllowed.Count,
			ResourcesSpecification: nodeSetResources,
		}
	}

	// scaleHorizontally always start from the min number of nodes and add nodes as necessary
	minNodes := len(nodeSets) * int(*policy.MinAllowed.Count)
	minMemory := int64(minNodes) * (nodeCapacity.Memory.Value())

	// memoryDelta holds the memory variation, it can be:
	// * a positive value if some memory needs to be added
	// * a negative value if some memory can be reclaimed
	memoryDelta := *requestedCapacity.Memory - minMemory
	nodeToAdd := getNodeDelta(memoryDelta, nodeCapacity.Memory.Value(), minMemory, *requestedCapacity.Memory)

	log.V(1).Info(
		"Memory status",
		"tier", policy.Roles,
		"tier_target", requestedCapacity.Memory,
		"node_target", minNodes+nodeToAdd,
	)

	if nodeToAdd > 0 {
		nodeToAdd = min(int(*policy.MaxAllowed.Count)-minNodes, nodeToAdd)
		log.V(1).Info("Need to add nodes", "to_add", nodeToAdd)
		fnm := NewFairNodesManager(nodeSetsResources)
		for nodeToAdd > 0 {
			fnm.AddNode()
			nodeToAdd--
		}
	}

	return nodeSetsResources
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
