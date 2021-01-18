// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"fmt"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/resources"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
)

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func (ctx *Context) scaleHorizontally(
	totalRequiredCapacity client.Capacity, // total required resources, at the tier level.
	nodeCapacity resources.ResourcesSpecification, // resources for each node in the tier/policy, as computed by the vertical autoscaler.
) resources.NamedTierResources {
	minNodes := int(ctx.AutoscalingSpec.NodeCount.Min)
	maxNodes := int(ctx.AutoscalingSpec.NodeCount.Max)
	nodeToAdd := 0
	if !totalRequiredCapacity.Memory.IsZero() && nodeCapacity.HasRequest(corev1.ResourceMemory) {
		nodeMemory := nodeCapacity.GetRequest(corev1.ResourceMemory)
		minMemory := int64(ctx.AutoscalingSpec.NodeCount.Min) * (nodeMemory.Value())
		// memoryDelta holds the memory variation, it can be:
		// * a positive value if some memory needs to be added
		// * a negative value if some memory can be reclaimed
		memoryDelta := totalRequiredCapacity.Memory.Value() - minMemory
		nodeToAdd = getNodeDelta(memoryDelta, nodeMemory.Value())

		if minNodes+nodeToAdd > maxNodes {
			// Elasticsearch requested more memory per node than allowed
			ctx.Log.Info(
				"Can't provide total required memory",
				"policy", ctx.AutoscalingSpec.Name,
				"scope", "tier",
				"resource", "memory",
				"node_value", nodeMemory.Value(),
				"requested_value", *totalRequiredCapacity.Memory,
				"requested_count", minNodes+nodeToAdd,
				"max_count", maxNodes,
			)

			// Update the autoscaling status accordingly
			ctx.StatusBuilder.
				ForPolicy(ctx.AutoscalingSpec.Name).
				WithEvent(
					status.HorizontalScalingLimitReached,
					fmt.Sprintf("Can't provide total required memory %d, max number of nodes is %d, requires %d nodes", *totalRequiredCapacity.Memory, maxNodes, minNodes+nodeToAdd),
				)
			nodeToAdd = maxNodes - minNodes
		}
	}

	if !totalRequiredCapacity.Storage.IsZero() && nodeCapacity.HasRequest(corev1.ResourceStorage) {
		nodeStorage := nodeCapacity.GetRequest(corev1.ResourceStorage)
		minStorage := int64(minNodes) * (nodeStorage.Value())
		storageDelta := totalRequiredCapacity.Storage.Value() - minStorage
		nodeToAddStorage := getNodeDelta(storageDelta, nodeStorage.Value())
		if minNodes+nodeToAddStorage > maxNodes {
			// Elasticsearch requested more memory per node than allowed
			ctx.Log.Info(
				"Can't provide total required storage",
				"policy", ctx.AutoscalingSpec.Name,
				"scope", "tier",
				"resource", "storage",
				"node_value", nodeStorage.Value(),
				"requested_value", *totalRequiredCapacity.Storage,
				"requested_count", minNodes+nodeToAddStorage,
				"max_count", maxNodes,
			)

			// Update the autoscaling status accordingly
			ctx.StatusBuilder.
				ForPolicy(ctx.AutoscalingSpec.Name).
				WithEvent(
					status.HorizontalScalingLimitReached,
					fmt.Sprintf("Can't provide total required storage %d, max number of nodes is %d, requires %d nodes", *totalRequiredCapacity.Storage, maxNodes, minNodes+nodeToAddStorage),
				)
			nodeToAddStorage = maxNodes - minNodes
		}
		if nodeToAddStorage > nodeToAdd {
			nodeToAdd = nodeToAddStorage
		}
	}

	totalNodes := nodeToAdd + minNodes
	ctx.Log.Info("Horizontal autoscaler", "policy", ctx.AutoscalingSpec.Name,
		"scope", "tier",
		"count", totalNodes,
		"required_capacity", totalRequiredCapacity,
	)

	nodeSetsResources := resources.NewNamedTierResources(ctx.AutoscalingSpec.Name, ctx.NodeSets.Names())
	nodeSetsResources.ResourcesSpecification = nodeCapacity
	fnm := NewFairNodesManager(ctx.Log, nodeSetsResources.NodeSetNodeCount)
	for totalNodes > 0 {
		fnm.AddNode()
		totalNodes--
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
