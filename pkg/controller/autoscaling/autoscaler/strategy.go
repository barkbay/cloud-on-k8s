// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

// getMaxStorage extracts the max storage size among a set of nodeSets.
func getMaxStorage(nodeSets status.NodeSetsStatus) resource.Quantity {
	storage := volume.DefaultPersistentVolumeSize.DeepCopy()
	for _, nodeSet := range nodeSets.ByNodeSet() {
		if nodeSet.Storage != nil && nodeSet.Storage.Cmp(storage) > 0 {
			storage = *nodeSet.Storage
		}
	}
	return storage
}

func minStorage(minAllowed *resource.Quantity, nodeSetsStatus status.NodeSetsStatus) resource.Quantity {
	// TODO: nodeSet with more than once volume claim is not supported
	storage := getMaxStorage(nodeSetsStatus)
	if minAllowed != nil && minAllowed.Cmp(storage) > 0 {
		storage = minAllowed.DeepCopy()
	}
	return storage
}

// EnsureResourcePolicies ensures that even if no decisions have been returned the nodeSet respect
// the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func EnsureResourcePolicies(
	log logr.Logger,
	nodeSets []esv1.NodeSet,
	autoscalingSpec esv1.AutoscalingSpec,
	nodeSetsStatus status.NodeSetsStatus,
) nodesets.NodeSetsResources {
	currentStorage := getMaxStorage(nodeSetsStatus)
	nodeSetsResources := make(nodesets.NodeSetsResources, len(nodeSets))
	statusByNodeSet := nodeSetsStatus.ByNodeSet()

	// TODO: compute current count and use the fair manager to check that everything is ok
	var totalNodes int32 = 0

	for i, nodeSet := range nodeSets {
		storage := minStorage(autoscalingSpec.MinAllowed.Storage, nodeSetsStatus)
		nodeSetResources := nodesets.NodeSetResources{
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

func GetScaleDecision(
	log logr.Logger,
	nodeSets []string,
	nodeSetsStatus status.NodeSetsStatus,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.NodeSetsResources {
	// 1. Scale vertically
	desiredNodeResources := scaleVertically(log, len(nodeSets), nodeSetsStatus, requiredCapacity, autoscalingSpec, statusBuilder)
	// 2. Scale horizontally
	return scaleHorizontally(log, nodeSets, requiredCapacity.Total, desiredNodeResources, autoscalingSpec, statusBuilder)
}

var giga = int64(1024 * 1024 * 1024)

// scaleVertically computes desired state for a node given the requested capacity from ES and the resource autoscalingSpec
// specified by the user.
// It attempts to scale all the resources vertically until the expectations are met.
func scaleVertically(
	log logr.Logger,
	nodeSetsCount int,
	nodeSetsStatus status.NodeSetsStatus,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *status.PolicyStatesBuilder,
) esv1.ResourcesSpecification {
	minNodesCount := int64(autoscalingSpec.MinAllowed.Count) * int64(nodeSetsCount)
	currentStorage := minStorage(autoscalingSpec.MinAllowed.Storage, nodeSetsStatus)

	return nodeResources(
		log,
		minNodesCount,
		currentStorage,
		requiredCapacity,
		autoscalingSpec,
		statusBuilder,
	)
}

// adjustMinMaxCount ensures that the min nodes is at leats the smae than the number of nodeSets managed by a policy.
// This is is to avoid nodeSets with count set to 0, which is not supported.
func adjustMinMaxCount(
	log logr.Logger,
	nodeSetCount int,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *status.PolicyStatesBuilder,
) (int, int) {
	minNodes := int(autoscalingSpec.MinAllowed.Count)
	maxNodes := int(autoscalingSpec.MaxAllowed.Count)
	if !(minNodes >= nodeSetCount) {
		minNodes = nodeSetCount
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
				status.InvalidMinimumNodeCount,
				fmt.Sprintf("At leats 1 node per nodeSet is required, minimum number of nodes has been adjusted from %d to %d", autoscalingSpec.MinAllowed.Count, minNodes),
			)
	}
	return minNodes, maxNodes
}
