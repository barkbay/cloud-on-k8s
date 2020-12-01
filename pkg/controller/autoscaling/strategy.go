// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"context"
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type NodeSetsResources []NodeSetResources

type NodeSetResources struct {
	Name string `json:"name,omitempty"`
	esv1.ResourcesSpecification
}

func (nsr NodeSetsResources) byNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources, len(nsr))
	for _, nodeSetsResource := range nsr {
		byNodeSet[nodeSetsResource.Name] = nodeSetsResource
	}
	return byNodeSet
}

// getMaxStorage extracts the max storage size among a set of nodeSets.
func getMaxStorage(nodeSets ...esv1.NodeSet) resource.Quantity {
	storage := volume.DefaultPersistentVolumeSize.DeepCopy()
	// TODO: refactor for/for/if/if
	for _, nodeSet := range nodeSets {
		for _, claimTemplate := range nodeSet.VolumeClaimTemplates {
			if claimTemplate.Name == volume.ElasticsearchDataVolumeName && claimTemplate.Spec.Resources.Requests != nil {
				nodeSetStorage, ok := claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
				if ok && nodeSetStorage.Cmp(storage) > 0 {
					storage = nodeSetStorage
				}
			}
		}
	}
	return storage
}

func minStorage(minAllowed *resource.Quantity, nodeSets ...esv1.NodeSet) resource.Quantity {
	// TODO: nodeSet with more than once volume claim is not supported
	storage := getMaxStorage(nodeSets...)
	if minAllowed != nil && minAllowed.Cmp(storage) > 0 {
		storage = minAllowed.DeepCopy()
	}
	return storage
}

// ensureResourcePolicies ensures that even if no decisions have been returned the nodeSet respect
// the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func ensureResourcePolicies(
	nodeSets []esv1.NodeSet,
	autoscalingSpec esv1.AutoscalingSpec,
	autoscalingStatus Status,
) NodeSetsResources {
	nodeSetsResources := make(NodeSetsResources, len(nodeSets))
	statusByNodeSet := autoscalingStatus.ByNodeSet()
	for i, nodeSet := range nodeSets {
		storage := minStorage(autoscalingSpec.MinAllowed.Storage, nodeSet)
		nodeSetResources := NodeSetResources{
			Name: nodeSet.Name,
			ResourcesSpecification: esv1.ResourcesSpecification{
				Count:   autoscalingSpec.MinAllowed.Count,
				Cpu:     autoscalingSpec.MinAllowed.Cpu,
				Memory:  autoscalingSpec.MinAllowed.Memory,
				Storage: &storage,
			},
		}
		nodeSetStatus, ok := statusByNodeSet[nodeSet.Name]
		if !ok {
			// No current status for this nodeSet, create a new one with minimums
			nodeSetsResources[i] = nodeSetResources
			continue
		}

		// ensure that the min. number of nodes is set
		if nodeSetStatus.Count < autoscalingSpec.MinAllowed.Count {
			nodeSetResources.Count = autoscalingSpec.MinAllowed.Count
		} else if nodeSetStatus.Count > autoscalingSpec.MaxAllowed.Count {
			nodeSetResources.Count = autoscalingSpec.MaxAllowed.Count
		}

		// Ensure memory settings are in the allowed range
		if nodeSetStatus.Memory != nil && autoscalingSpec.MinAllowed.Memory != nil && autoscalingSpec.MaxAllowed.Memory != nil {
			nodeSetStatus.Memory = adjustQuantity(*nodeSetStatus.Memory, *autoscalingSpec.MinAllowed.Memory, *autoscalingSpec.MinAllowed.Memory)
		}

		// Ensure CPU settings are in the allowed range
		if nodeSetStatus.Cpu != nil && autoscalingSpec.MinAllowed.Cpu != nil && autoscalingSpec.MaxAllowed.Cpu != nil {
			nodeSetStatus.Cpu = adjustQuantity(*nodeSetStatus.Cpu, *autoscalingSpec.MinAllowed.Cpu, *autoscalingSpec.MinAllowed.Cpu)
		}

		// Ensure Storage settings are in the allowed range but not below the current size
		currentStorage := getMaxStorage(nodeSet)
		if nodeSetStatus.Storage != nil && autoscalingSpec.MinAllowed.Storage != nil && autoscalingSpec.MaxAllowed.Storage != nil {
			nodeSetStatus.Storage = adjustQuantity(*nodeSetStatus.Storage, *autoscalingSpec.MinAllowed.Storage, *autoscalingSpec.MinAllowed.Storage)
			// Do not downscale storage capacity.
			if currentStorage.Cmp(*nodeSetStatus.Storage) > 0 {
				nodeSetStatus.Storage = &currentStorage
			}
		} else {
			nodeSetStatus.Storage = &currentStorage
		}

		nodeSetsResources[i] = nodeSetResources
	}

	return nodeSetsResources
}

// adjustQuantity ensures that a quantity is comprised between a min and a max.
func adjustQuantity(value, min, max resource.Quantity) *resource.Quantity {
	if value.Cmp(min) < 0 {
		return &min
	} else if value.Cmp(max) > 0 {
		return &max
	}
	return &value
}

func getScaleDecision(
	ctx context.Context,
	nodeSets []v1.NodeSet,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
) NodeSetsResources {
	// 1. Scale vertically
	desiredNodeResources := scaleVertically(nodeSets, requiredCapacity, autoscalingSpec)
	// 2. Scale horizontally
	return scaleHorizontally(ctx, nodeSets, requiredCapacity.Tier, desiredNodeResources, autoscalingSpec)
}

var giga = int64(1024 * 1024 * 1024)

// scaleVertically computes desired state for a node given the requested capacity from ES and the resource autoscalingSpec
// specified by the user.
// It attempts to scale all the resources vertically until the expectations are met.
// TODO: current code assumes that Elasticsearch API returns at least a memory requirements, storage is not mandatory.
func scaleVertically(
	nodeSets []v1.NodeSet,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
) esv1.ResourcesSpecification {

	// Check if overall tier requirement is higher than node requirement.
	// This is done to check if we can fulfil the tier requirement only by scaling vertically
	minNodesCount := int64(autoscalingSpec.MinAllowed.Count) * int64(len(nodeSets))
	// Tiers memory capacity distributed on min. nodes
	memoryOverAllTiers := *requiredCapacity.Tier.Memory / minNodesCount
	requiredMemoryCapacity := max64(
		*requiredCapacity.Node.Memory,
		roundUp(memoryOverAllTiers, giga),
	)

	// Set desired memory capacity within the allowed range
	if requiredMemoryCapacity < autoscalingSpec.MinAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		requiredMemoryCapacity = autoscalingSpec.MinAllowed.Memory.Value()
	}
	if requiredMemoryCapacity > autoscalingSpec.MaxAllowed.Memory.Value() {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		requiredMemoryCapacity = autoscalingSpec.MaxAllowed.Memory.Value()
	}

	// Prepare the resource storage
	currentMinStorage := minStorage(autoscalingSpec.MinAllowed.Storage, nodeSets...)
	resourceStorage := &currentMinStorage
	if (requiredCapacity.Tier.Storage != nil && requiredCapacity.Node.Storage != nil) &&
		(autoscalingSpec.MinAllowed.Storage == nil || autoscalingSpec.MaxAllowed.Storage == nil) {
		// TODO: not limit defined, raise an event
	} else if requiredCapacity.Tier.Storage != nil && requiredCapacity.Node.Storage != nil {
		// Tiers storage capacity distributed on min. nodes
		storageOverAllTiers := *requiredCapacity.Tier.Storage / minNodesCount
		requiredStorageCapacity := max64(
			*requiredCapacity.Node.Storage,
			roundUp(storageOverAllTiers, giga),
			currentMinStorage.Value(),
		)
		// Set desired storage capacity within the allowed range
		if requiredStorageCapacity < autoscalingSpec.MinAllowed.Storage.Value() {
			// The amount of storage requested by Elasticsearch is less than the min. allowed value
			requiredStorageCapacity = autoscalingSpec.MinAllowed.Storage.Value()
		}
		if requiredStorageCapacity > autoscalingSpec.MaxAllowed.Storage.Value() {
			// The amount of storage requested by Elasticsearch is more than the max. allowed value
			requiredStorageCapacity = autoscalingSpec.MaxAllowed.Storage.Value()
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

	nodeResourcesSpecification := esv1.ResourcesSpecification{
		Memory:  resourceMemory,
		Storage: resourceStorage,
	}

	if autoscalingSpec.MaxAllowed.Cpu != nil && autoscalingSpec.MinAllowed.Cpu != nil {
		nodeResourcesSpecification.Cpu = cpuFromMemory(requiredMemoryCapacity, autoscalingSpec)
	}

	return nodeResourcesSpecification
}

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	ctx context.Context,
	nodeSets []v1.NodeSet,
	requestedCapacity client.Capacity,
	nodeCapacity esv1.ResourcesSpecification,
	autoscalingSpec esv1.AutoscalingSpec,
) NodeSetsResources {
	log := logf.FromContext(ctx)
	nodeSetsResources := make(NodeSetsResources, len(nodeSets))
	for i, nodeSet := range nodeSets {
		nodeSetsResources[i] = NodeSetResources{
			Name:                   nodeSet.Name,
			ResourcesSpecification: nodeCapacity,
		}
		// set all the nodeSets count the minimum
		nodeSetsResources[i].Count = autoscalingSpec.MinAllowed.Count
	}

	// scaleHorizontally always start from the min number of nodes and add nodes as necessary
	minNodes := len(nodeSets) * int(autoscalingSpec.MinAllowed.Count)
	minMemory := int64(minNodes) * (nodeCapacity.Memory.Value())

	// memoryDelta holds the memory variation, it can be:
	// * a positive value if some memory needs to be added
	// * a negative value if some memory can be reclaimed
	memoryDelta := *requestedCapacity.Memory - minMemory
	nodeToAdd := getNodeDelta(memoryDelta, nodeCapacity.Memory.Value(), minMemory, *requestedCapacity.Memory)

	if requestedCapacity.Storage != nil && nodeCapacity.Storage != nil {
		minStorage := int64(minNodes) * (nodeCapacity.Storage.Value())
		storageDelta := *requestedCapacity.Storage - minStorage
		nodeToAddStorage := getNodeDelta(storageDelta, nodeCapacity.Storage.Value(), minStorage, *requestedCapacity.Storage)
		if nodeToAddStorage > nodeToAdd {
			nodeToAdd = nodeToAddStorage
		}
	}

	log.V(1).Info(
		"Memory status",
		"tier", autoscalingSpec.Roles,
		"tier_target", requestedCapacity.Memory,
		"node_target", minNodes+nodeToAdd,
	)

	if nodeToAdd > 0 {
		nodeToAdd = min(int(autoscalingSpec.MaxAllowed.Count)-minNodes, nodeToAdd)
		log.V(1).Info("Need to add nodes", "to_add", nodeToAdd)
		fnm := NewFairNodesManager(ctx, nodeSetsResources)
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

func max64(x int64, others ...int64) int64 {
	max := x
	for _, other := range others {
		if other > max {
			max = other
		}
	}
	return max
}

func roundUp(v, n int64) int64 {
	r := v % n
	if r == 0 {
		return v
	}
	return v + n - r
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
