// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"fmt"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/elasticsearch/resources"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/elasticsearch/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
)

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func (ctx *Context) scaleHorizontally(
	totalRequiredCapacity client.Capacity, // total required resources, at the tier level.
	nodeCapacity resources.NodeResources, // resources for each node in the tier/policy, as computed by the vertical autoscaler.
) resources.NodeSetsResources {
	minNodes := int(ctx.AutoscalingSpec.NodeCount.Min)
	maxNodes := int(ctx.AutoscalingSpec.NodeCount.Max)
	nodeToAdd := 0
	if !totalRequiredCapacity.Memory.IsZero() && nodeCapacity.HasRequest(corev1.ResourceMemory) {
		nodeMemory := nodeCapacity.GetRequest(corev1.ResourceMemory)
		nodeToAdd = ctx.getNodesToAdd(nodeMemory.Value(), totalRequiredCapacity.Memory.Value(), minNodes, maxNodes, string(corev1.ResourceMemory))
	}

	if !totalRequiredCapacity.Storage.IsZero() && nodeCapacity.HasRequest(corev1.ResourceStorage) {
		nodeStorage := nodeCapacity.GetRequest(corev1.ResourceStorage)
		nodeToAdd = max(nodeToAdd, ctx.getNodesToAdd(nodeStorage.Value(), totalRequiredCapacity.Storage.Value(), minNodes, maxNodes, string(corev1.ResourceStorage)))
	}

	totalNodes := nodeToAdd + minNodes
	ctx.Log.Info("Horizontal autoscaler", "policy", ctx.AutoscalingSpec.Name,
		"scope", "tier",
		"count", totalNodes,
		"required_capacity", totalRequiredCapacity,
	)

	nodeSetsResources := resources.NewNodeSetsResources(ctx.AutoscalingSpec.Name, ctx.NodeSets.Names())
	nodeSetsResources.NodeResources = nodeCapacity
	fnm := NewFairNodesManager(ctx.Log, nodeSetsResources.NodeSetNodeCount)
	for totalNodes > 0 {
		fnm.AddNode()
		totalNodes--
	}

	return nodeSetsResources
}

// getNodesToAdd calculates the number of nodes to add in order to comply with capacity requested by Elasticsearch.
func (ctx *Context) getNodesToAdd(
	nodeResourceCapacity int64, // resource capacity of a single node, for example the node memory
	totalRequiredCapacity int64, // required capacity at the tier level
	minNodes, maxNodes int, // min and max number of nodes in this tier, as specified by the user the autoscaling spec.
	resourceName string, // used for logging and in events
) int {
	minResourceQuantity := int64(minNodes) * (nodeResourceCapacity)
	// resourceDelta holds the resource needed to comply with what is requested by Elasticsearch.
	resourceDelta := totalRequiredCapacity - minResourceQuantity
	nodeToAdd := getNodeDelta(resourceDelta, nodeResourceCapacity)

	if minNodes+nodeToAdd > maxNodes {
		// Elasticsearch requested more resource quantity per node than allowed.
		ctx.Log.Info(
			fmt.Sprintf("Can't provide total required %s", resourceName),
			"policy", ctx.AutoscalingSpec.Name,
			"scope", "tier",
			"resource", resourceName,
			"node_value", nodeResourceCapacity,
			"requested_value", totalRequiredCapacity,
			"requested_count", minNodes+nodeToAdd,
			"max_count", maxNodes,
		)

		// Update the autoscaling status accordingly
		ctx.StatusBuilder.
			ForPolicy(ctx.AutoscalingSpec.Name).
			WithEvent(
				status.HorizontalScalingLimitReached,
				fmt.Sprintf("Can't provide total required %s %d, max number of nodes is %d, requires %d nodes", resourceName, totalRequiredCapacity, maxNodes, minNodes+nodeToAdd),
			)
		// Adjust the number of nodes to be added to comply with the limit specified by the user.
		nodeToAdd = maxNodes - minNodes
	}
	return nodeToAdd
}

// getNodeDelta computes the nodes to be added given a delta (the additional amount of resource needed) and the individual capacity a single node.
func getNodeDelta(delta, nodeCapacity int64) int {
	nodeToAdd := 0
	if delta < 0 {
		return 0
	}

	for delta > 0 {
		delta -= nodeCapacity
		// Compute how many nodes should be added
		nodeToAdd++
	}
	return nodeToAdd
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
