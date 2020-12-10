// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"fmt"

	"github.com/go-logr/logr"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type NodeSetsResources []NodeSetResources

type NodeSetResources struct {
	Name string `json:"name,omitempty"`
	esv1.ResourcesSpecification
}

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

func (nsr NodeSetsResources) byNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources, len(nsr))
	for _, nodeSetsResource := range nsr {
		byNodeSet[nodeSetsResource.Name] = nodeSetsResource
	}
	return byNodeSet
}

// getMaxStorage extracts the max storage size among a set of nodeSets.
func getMaxStorage(nodeSets NodeSetsStatus) resource.Quantity {
	storage := volume.DefaultPersistentVolumeSize.DeepCopy()
	// TODO: refactor for/for/if/if
	for _, nodeSet := range nodeSets.ByNodeSet() {
		if nodeSet.Storage != nil && nodeSet.Storage.Cmp(storage) > 0 {
			storage = *nodeSet.Storage
		}
	}
	return storage
}

func minStorage(minAllowed *resource.Quantity, nodeSetsStatus NodeSetsStatus) resource.Quantity {
	// TODO: nodeSet with more than once volume claim is not supported
	storage := getMaxStorage(nodeSetsStatus)
	if minAllowed != nil && minAllowed.Cmp(storage) > 0 {
		storage = minAllowed.DeepCopy()
	}
	return storage
}

// ensureResourcePolicies ensures that even if no decisions have been returned the nodeSet respect
// the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func ensureResourcePolicies(
	log logr.Logger,
	nodeSets []esv1.NodeSet,
	autoscalingSpec esv1.AutoscalingSpec,
	nodeSetsStatus NodeSetsStatus,
) NodeSetsResources {
	currentStorage := getMaxStorage(nodeSetsStatus)
	nodeSetsResources := make(NodeSetsResources, len(nodeSets))
	statusByNodeSet := nodeSetsStatus.ByNodeSet()

	// TODO: compute current count and use the fair manager to check that everything is ok
	var totalNodes int32 = 0

	for i, nodeSet := range nodeSets {
		storage := minStorage(autoscalingSpec.MinAllowed.Storage, nodeSetsStatus)
		nodeSetResources := NodeSetResources{
			Name: nodeSet.Name,
			ResourcesSpecification: esv1.ResourcesSpecification{
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

		totalNodes += nodeSetResources.Count

		// Ensure memory settings are in the allowed range
		if nodeSetStatus.Memory != nil && autoscalingSpec.MinAllowed.Memory != nil && autoscalingSpec.MaxAllowed.Memory != nil {
			nodeSetStatus.Memory = adjustQuantity(*nodeSetStatus.Memory, *autoscalingSpec.MinAllowed.Memory, *autoscalingSpec.MinAllowed.Memory)
		}

		// Ensure CPU settings are in the allowed range
		if nodeSetStatus.Cpu != nil && autoscalingSpec.MinAllowed.Cpu != nil && autoscalingSpec.MaxAllowed.Cpu != nil {
			nodeSetStatus.Cpu = adjustQuantity(*nodeSetStatus.Cpu, *autoscalingSpec.MinAllowed.Cpu, *autoscalingSpec.MinAllowed.Cpu)
		}

		// Ensure Storage settings are in the allowed range but not below the current size
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

	// ensure that the min. number of nodes is set
	if totalNodes < autoscalingSpec.MinAllowed.Count {
		totalNodes = autoscalingSpec.MinAllowed.Count
	} else if totalNodes > autoscalingSpec.MaxAllowed.Count {
		totalNodes = autoscalingSpec.MaxAllowed.Count
	}

	fnm := NewFairNodesManager(log, nodeSetsResources)
	for totalNodes > 0 {
		fnm.AddNode()
		totalNodes--
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
	log logr.Logger,
	nodeSets []v1.NodeSet,
	nodeSetsStatus NodeSetsStatus,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *PolicyStatesBuilder,
) NodeSetsResources {
	// 1. Scale vertically
	desiredNodeResources := scaleVertically(log, nodeSets, nodeSetsStatus, requiredCapacity, autoscalingSpec, statusBuilder)
	// 2. Scale horizontally
	return scaleHorizontally(log, nodeSets, requiredCapacity.Total, desiredNodeResources, autoscalingSpec, statusBuilder)
}

var giga = int64(1024 * 1024 * 1024)

// scaleVertically computes desired state for a node given the requested capacity from ES and the resource autoscalingSpec
// specified by the user.
// It attempts to scale all the resources vertically until the expectations are met.
// TODO: current code assumes that Elasticsearch API returns at least a memory requirements, storage is not mandatory.
func scaleVertically(
	log logr.Logger,
	nodeSets []v1.NodeSet,
	nodeSetsStatus NodeSetsStatus,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *PolicyStatesBuilder,
) esv1.ResourcesSpecification {
	// Check if overall tier requirement is higher than node requirement.
	// This is done to check if we can fulfil the tier requirement only by scaling vertically
	minNodesCount := int64(autoscalingSpec.MinAllowed.Count) * int64(len(nodeSets))
	requiredMemoryCapacity := *requiredCapacity.Node.Memory

	if requiredMemoryCapacity > autoscalingSpec.MaxAllowed.Memory.Value() {
		// Elasticsearch requested more memory per node than allowed
		log.Info(
			"Node required memory is greater than the allowed one",
			"scope", "node",
			"policy", autoscalingSpec.Name,
			"required_memory",
			*requiredCapacity.Node.Memory,
			"max_allowed_memory",
			autoscalingSpec.MaxAllowed.Memory.Value(),
		)
		// Update the autoscaling status accordingly
		statusBuilder.
			ForPolicy(autoscalingSpec.Name).
			WithPolicyState(
				VerticalScalingLimitReached,
				fmt.Sprintf("Node required memory %d is greater than max allowed: %d", *requiredCapacity.Node.Memory, autoscalingSpec.MaxAllowed.Memory.Value()),
			)
	}

	// Adjust the requested memory to try to fit the total memory capacity
	if requiredCapacity.Total.Memory != nil {
		memoryOverAllTiers := *requiredCapacity.Total.Memory / minNodesCount
		requiredMemoryCapacity = max64(
			*requiredCapacity.Node.Memory,
			roundUp(memoryOverAllTiers, giga),
		)
	}

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
	currentMinStorage := minStorage(autoscalingSpec.MinAllowed.Storage, nodeSetsStatus)
	resourceStorage := &currentMinStorage
	if (requiredCapacity.Total.Storage != nil && requiredCapacity.Node.Storage != nil) &&
		(autoscalingSpec.MinAllowed.Storage == nil || autoscalingSpec.MaxAllowed.Storage == nil) {
		// TODO: not limit defined, raise an event
	} else if requiredCapacity.Total.Storage != nil && requiredCapacity.Node.Storage != nil {

		if *requiredCapacity.Node.Storage > autoscalingSpec.MaxAllowed.Storage.Value() {
			// Elasticsearch requested more memory per node than allowed
			log.Info(
				"Node required storage is greater than max allowed",
				"scope", "node",
				"policy", autoscalingSpec.Name,
				"required_storage",
				*requiredCapacity.Node.Storage,
				"max_allowed_storage",
				autoscalingSpec.MaxAllowed.Storage.Value(),
			)

			// Update the autoscaling status accordingly
			statusBuilder.
				ForPolicy(autoscalingSpec.Name).
				WithPolicyState(
					VerticalScalingLimitReached,
					fmt.Sprintf("Node required storage %d is greater than max allowed: %d", *requiredCapacity.Node.Storage, autoscalingSpec.MaxAllowed.Storage.Value()),
				)
		}

		// Tiers storage capacity distributed on min. nodes
		storageOverAllTiers := *requiredCapacity.Total.Storage / minNodesCount
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

// adjustMinMaxCount ensures that the min nodes is at leats the smae than the number of nodeSets managed by a policy.
// This is is to avoid nodeSets with count set to 0, which is not supported.
func adjustMinMaxCount(
	log logr.Logger,
	nodeSets []v1.NodeSet,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *PolicyStatesBuilder,
) (int, int) {
	minNodes := int(autoscalingSpec.MinAllowed.Count)
	maxNodes := int(autoscalingSpec.MaxAllowed.Count)
	if !(minNodes >= len(nodeSets)) {
		minNodes = len(nodeSets)
		if minNodes >= maxNodes {
			// If needed also adjust the max number of Pods
			maxNodes = minNodes
		}
		log.Info(
			"Adjusting minimum and maximum number of nodes",
			"policy", autoscalingSpec.Name,
			"scope", "tier",
			"min_count", autoscalingSpec.MinAllowed.Count,
			"new_min_count", minNodes,
			"max_count", autoscalingSpec.MaxAllowed.Count,
			"new_max_count", maxNodes,
		)

		// Update the autoscaling status accordingly
		statusBuilder.
			ForPolicy(autoscalingSpec.Name).
			WithPolicyState(
				InvalidMinimumNodeCount,
				fmt.Sprintf("At leats 1 node per nodeSet is required, minimum number of nodes has been adjusted from %d to %d", autoscalingSpec.MinAllowed.Count, minNodes),
			)
	}
	return minNodes, maxNodes
}

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	log logr.Logger,
	nodeSets []v1.NodeSet,
	requestedCapacity client.Capacity,
	nodeCapacity esv1.ResourcesSpecification,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *PolicyStatesBuilder,
) NodeSetsResources {
	// Ensure that we have at least 1 node per nodeSet
	minNodes, maxNodes := adjustMinMaxCount(log, nodeSets, autoscalingSpec, statusBuilder)

	nodeSetsResources := make(NodeSetsResources, len(nodeSets))
	if requestedCapacity.Memory != nil {
		minMemory := int64(minNodes) * (nodeCapacity.Memory.Value())
		// memoryDelta holds the memory variation, it can be:
		// * a positive value if some memory needs to be added
		// * a negative value if some memory can be reclaimed
		memoryDelta := *requestedCapacity.Memory - minMemory
		nodeToAdd := getNodeDelta(memoryDelta, nodeCapacity.Memory.Value())

		if minNodes+nodeToAdd > maxNodes {
			// Elasticsearch requested more memory per node than allowed
			log.Info(
				"Can't provide total required memory",
				"policy", autoscalingSpec.Name,
				"scope", "tier",
				"resource", "memory",
				"node_value", nodeCapacity.Memory.Value(),
				"requested_value", *requestedCapacity.Memory,
				"requested_count", minNodes+nodeToAdd,
				"max_count", maxNodes,
			)

			// Update the autoscaling status accordingly
			statusBuilder.
				ForPolicy(autoscalingSpec.Name).
				WithPolicyState(
					HorizontalScalingLimitReached,
					fmt.Sprintf("Can't provide total required memory %d, max number of nodes is %d, requires %d nodes", *requestedCapacity.Memory, maxNodes, minNodes+nodeToAdd),
				)
			nodeToAdd = maxNodes - minNodes
		}

		if requestedCapacity.Storage != nil && nodeCapacity.Storage != nil {
			minStorage := int64(minNodes) * (nodeCapacity.Storage.Value())
			storageDelta := *requestedCapacity.Storage - minStorage
			nodeToAddStorage := getNodeDelta(storageDelta, nodeCapacity.Storage.Value())
			if minNodes+nodeToAddStorage > maxNodes {
				// Elasticsearch requested more memory per node than allowed
				log.Info(
					"Can't provide total required storage",
					"policy", autoscalingSpec.Name,
					"scope", "tier",
					"resource", "storage",
					"node_value", nodeCapacity.Storage.Value(),
					"requested_value", *requestedCapacity.Storage,
					"requested_count", minNodes+nodeToAddStorage,
					"max_count", maxNodes,
				)

				// Update the autoscaling status accordingly
				statusBuilder.
					ForPolicy(autoscalingSpec.Name).
					WithPolicyState(
						HorizontalScalingLimitReached,
						fmt.Sprintf("Can't provide total required storage %d, max number of nodes is %d, requires %d nodes", *requestedCapacity.Storage, maxNodes, minNodes+nodeToAddStorage),
					)
				nodeToAddStorage = maxNodes - minNodes
			}
			if nodeToAddStorage > nodeToAdd {
				nodeToAdd = nodeToAddStorage
			}
		}

		// TODO: add a log to explain the computation
		for i, nodeSet := range nodeSets {
			nodeSetsResources[i] = NodeSetResources{
				Name:                   nodeSet.Name,
				ResourcesSpecification: nodeCapacity,
			}
		}
		totalNodes := nodeToAdd + minNodes
		log.Info("Horizontal autoscaler", "policy", autoscalingSpec.Name, "count", totalNodes)
		fnm := NewFairNodesManager(log, nodeSetsResources)
		for totalNodes > 0 {
			fnm.AddNode()
			totalNodes--
		}

	}

	return nodeSetsResources
}

func getNodeDelta(memoryDelta, nodeMemoryCapacity int64) int {
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
