// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// GetOfflineNodeSetsResources attempts to create or restore nodeSetResources without an autoscaling decisions.
// It ensures that even if no decisions have been returned the nodeSet respect the min. and max. resource requirements.
// If resources are within the min. and max. boundaries then they are left untouched.
func GetOfflineNodeSetsResources(
	log logr.Logger,
	nodeSets []string,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	actualAutoscalingStatus status.Status,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.NamedTierResources {
	actualNamedTierResources, hasNamedTierResources := actualAutoscalingStatus.GetNamedTierResources(autoscalingSpec.Name)

	var namedTierResources nodesets.NamedTierResources
	expectedNodeCount := 0
	if !hasNamedTierResources {
		// No current status for this nodeSet, create a new one with minimums
		namedTierResources = newMinNodeSetResources(autoscalingSpec, nodeSets)
	} else {
		namedTierResources = nodeSetResourcesFromStatus(actualAutoscalingStatus, actualNamedTierResources, autoscalingSpec, nodeSets)
		for _, nodeSet := range actualNamedTierResources.NodeSetNodeCount {
			expectedNodeCount += int(nodeSet.NodeCount)
		}
	}

	// Ensure that we have at least 1 node per nodeSet
	minNodes, maxNodes := adjustMinMaxCount(log, len(nodeSets), autoscalingSpec, statusBuilder)

	// ensure that the min. number of nodes is set
	if expectedNodeCount < minNodes {
		expectedNodeCount = minNodes
	} else if expectedNodeCount > maxNodes {
		expectedNodeCount = maxNodes
	}

	fnm := NewFairNodesManager(log, namedTierResources.NodeSetNodeCount)
	for expectedNodeCount > 0 {
		fnm.AddNode()
		expectedNodeCount--
	}

	log.Info(
		"Offline autoscaling",
		"state", "offline",
		"policy", autoscalingSpec.Name,
		"nodeset", namedTierResources.NodeSetNodeCount,
		"count", namedTierResources.NodeSetNodeCount.TotalNodeCount(),
		"resources", namedTierResources.ToInt64(),
	)
	return namedTierResources
}

// nodeSetResourcesFromStatus restores NodeSetResources from the status.
// If user removed the limits while offline we are assuming that it wants to take back control on the resources.
func nodeSetResourcesFromStatus(
	actualAutoscalingStatus status.Status,
	actualNamedTierResources nodesets.NamedTierResources,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	nodeSets []string,
) nodesets.NamedTierResources {
	namedTierResources := nodesets.NewNamedTierResources(autoscalingSpec.Name, nodeSets)
	// Ensure memory settings are in the allowed range,
	if autoscalingSpec.IsMemoryDefined() {
		if actualNamedTierResources.HasRequest(corev1.ResourceMemory) {
			namedTierResources.SetRequest(corev1.ResourceMemory, adjustQuantity(actualNamedTierResources.GetRequest(corev1.ResourceMemory), autoscalingSpec.Memory.Min, autoscalingSpec.Memory.Max))
		} else {
			namedTierResources.SetRequest(corev1.ResourceMemory, autoscalingSpec.Memory.Min.DeepCopy())
		}
	}

	// Ensure CPU settings are in the allowed range
	if autoscalingSpec.IsCpuDefined() {
		if actualNamedTierResources.HasRequest(corev1.ResourceCPU) {
			namedTierResources.SetRequest(corev1.ResourceCPU, adjustQuantity(actualNamedTierResources.GetRequest(corev1.ResourceCPU), autoscalingSpec.Cpu.Min, autoscalingSpec.Cpu.Max))
		} else {
			namedTierResources.SetRequest(corev1.ResourceCPU, autoscalingSpec.Cpu.Min.DeepCopy())
		}
	}

	// Ensure storage capacity is set
	namedTierResources.SetRequest(corev1.ResourceStorage, getStorage(autoscalingSpec, actualAutoscalingStatus))

	return namedTierResources
}

// newMinNodeSetResources returns a NodeSetResources with minimums values
func newMinNodeSetResources(autoscalingSpec esv1.AutoscalingPolicySpec, nodeSets []string) nodesets.NamedTierResources {
	namedTierResources := nodesets.NewNamedTierResources(autoscalingSpec.Name, nodeSets)
	if autoscalingSpec.IsCpuDefined() {
		namedTierResources.SetRequest(corev1.ResourceCPU, autoscalingSpec.Cpu.Min.DeepCopy())
	}
	if autoscalingSpec.IsMemoryDefined() {
		namedTierResources.SetRequest(corev1.ResourceMemory, autoscalingSpec.Memory.Min.DeepCopy())
	}
	if autoscalingSpec.IsStorageDefined() {
		namedTierResources.SetRequest(corev1.ResourceStorage, autoscalingSpec.Storage.Min.DeepCopy())
	}
	return namedTierResources
}

// adjustQuantity ensures that a quantity is comprised between a min and a max.
func adjustQuantity(value, min, max resource.Quantity) resource.Quantity {
	if value.Cmp(min) < 0 {
		return min
	} else if value.Cmp(max) > 0 {
		return max
	}
	return value
}
