// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/resource"
)

// EnsureResourcePolicies ensures that even if no decisions have been returned the nodeSet respect
// the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func EnsureResourcePolicies(
	log logr.Logger,
	nodeSets []string,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	nodeSetsStatus status.NodeSetsResourcesWithMeta,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.NodeSetsResources {
	currentStorage := getMaxStorage(nodeSetsStatus)
	nodeSetsResources := make(nodesets.NodeSetsResources, len(nodeSets))
	statusByNodeSet := nodeSetsStatus.ByNodeSet()

	totalNodes := 0
	for i, nodeSet := range nodeSets {
		storage := minStorage(autoscalingSpec.Storage, nodeSetsStatus)
		nodeSetResources := nodesets.NodeSetResources{
			Name:                   nodeSet,
			ResourcesSpecification: nodesets.ResourcesSpecification{Storage: &storage},
		}
		if autoscalingSpec.IsCpuDefined() {
			nodeSetResources.Cpu = &autoscalingSpec.Cpu.Min
		}
		if autoscalingSpec.IsMemoryDefined() {
			nodeSetResources.Memory = &autoscalingSpec.Memory.Min
		}
		nodeSetStatus, ok := statusByNodeSet[nodeSet]
		if !ok {
			// No current status for this nodeSet, create a new one with minimums
			nodeSetsResources[i] = nodeSetResources
			continue
		}
		totalNodes += int(nodeSetStatus.Count)

		// Ensure memory settings are in the allowed range
		if nodeSetStatus.Memory != nil && autoscalingSpec.IsMemoryDefined() {
			nodeSetResources.Memory = adjustQuantity(*nodeSetStatus.Memory, autoscalingSpec.Memory.Min, autoscalingSpec.Memory.Max)
		}

		// Ensure CPU settings are in the allowed range
		if nodeSetStatus.Cpu != nil && autoscalingSpec.IsCpuDefined() {
			nodeSetResources.Cpu = adjustQuantity(*nodeSetStatus.Cpu, autoscalingSpec.Cpu.Min, autoscalingSpec.Cpu.Max)
		}

		// Ensure Storage settings are in the allowed range but not below the current size
		if nodeSetStatus.Storage != nil && autoscalingSpec.IsStorageDefined() {
			nodeSetResources.Storage = adjustQuantity(*nodeSetStatus.Storage, autoscalingSpec.Storage.Min, autoscalingSpec.Storage.Max)
			// Do not downscale storage capacity.
			if currentStorage.Cmp(*nodeSetResources.Storage) > 0 {
				nodeSetResources.Storage = &currentStorage
			}
		} else {
			nodeSetResources.Storage = &currentStorage
		}

		nodeSetsResources[i] = nodeSetResources
	}

	// Ensure that we have at least 1 node per nodeSet
	minNodes, maxNodes := adjustMinMaxCount(log, len(nodeSets), autoscalingSpec, statusBuilder)

	// ensure that the min. number of nodes is set
	if totalNodes < minNodes {
		totalNodes = minNodes
	} else if totalNodes > maxNodes {
		totalNodes = maxNodes
	}

	fnm := NewFairNodesManager(log, nodeSetsResources)
	for totalNodes > 0 {
		fnm.AddNode()
		totalNodes--
	}

	for _, nodesetResources := range nodeSetsResources {
		log.Info(
			"Offline autoscaling",
			"state", "offline",
			"policy", autoscalingSpec.Name,
			"nodeset", nodesetResources.Name,
			"count", nodesetResources.Count,
			"resources", nodesetResources.ToInt64(),
		)
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
