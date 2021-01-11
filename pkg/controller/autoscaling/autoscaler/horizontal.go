// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/go-logr/logr"
)

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	log logr.Logger,
	nodeSets []string,
	requiredCapacity client.Capacity,
	nodeCapacity nodesets.ResourcesSpecification,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.NamedTierResources {
	// Ensure that we have at least 1 node per nodeSet
	minNodes := int(autoscalingSpec.NodeCount.Min)
	maxNodes := int(autoscalingSpec.NodeCount.Max)
	nodeToAdd := 0
	if requiredCapacity.Memory != nil && nodeCapacity.HasRequest(corev1.ResourceMemory) {
		nodeMemory := nodeCapacity.GetRequest(corev1.ResourceMemory)
		minMemory := int64(autoscalingSpec.NodeCount.Min) * (nodeMemory.Value())
		// memoryDelta holds the memory variation, it can be:
		// * a positive value if some memory needs to be added
		// * a negative value if some memory can be reclaimed
		memoryDelta := *requiredCapacity.Memory - minMemory
		nodeToAdd = getNodeDelta(memoryDelta, nodeMemory.Value())

		if minNodes+nodeToAdd > maxNodes {
			// Elasticsearch requested more memory per node than allowed
			log.Info(
				"Can't provide total required memory",
				"policy", autoscalingSpec.Name,
				"scope", "tier",
				"resource", "memory",
				"node_value", nodeMemory.Value(),
				"requested_value", *requiredCapacity.Memory,
				"requested_count", minNodes+nodeToAdd,
				"max_count", maxNodes,
			)

			// Update the autoscaling status accordingly
			statusBuilder.
				ForPolicy(autoscalingSpec.Name).
				WithPolicyState(
					status.HorizontalScalingLimitReached,
					fmt.Sprintf("Can't provide total required memory %d, max number of nodes is %d, requires %d nodes", *requiredCapacity.Memory, maxNodes, minNodes+nodeToAdd),
				)
			nodeToAdd = maxNodes - minNodes
		}
	}

	if requiredCapacity.Storage != nil && nodeCapacity.HasRequest(corev1.ResourceStorage) {
		nodeStorage := nodeCapacity.GetRequest(corev1.ResourceStorage)
		minStorage := int64(minNodes) * (nodeStorage.Value())
		storageDelta := *requiredCapacity.Storage - minStorage
		nodeToAddStorage := getNodeDelta(storageDelta, nodeStorage.Value())
		if minNodes+nodeToAddStorage > maxNodes {
			// Elasticsearch requested more memory per node than allowed
			log.Info(
				"Can't provide total required storage",
				"policy", autoscalingSpec.Name,
				"scope", "tier",
				"resource", "storage",
				"node_value", nodeStorage.Value(),
				"requested_value", *requiredCapacity.Storage,
				"requested_count", minNodes+nodeToAddStorage,
				"max_count", maxNodes,
			)

			// Update the autoscaling status accordingly
			statusBuilder.
				ForPolicy(autoscalingSpec.Name).
				WithPolicyState(
					status.HorizontalScalingLimitReached,
					fmt.Sprintf("Can't provide total required storage %d, max number of nodes is %d, requires %d nodes", *requiredCapacity.Storage, maxNodes, minNodes+nodeToAddStorage),
				)
			nodeToAddStorage = maxNodes - minNodes
		}
		if nodeToAddStorage > nodeToAdd {
			nodeToAdd = nodeToAddStorage
		}
	}

	totalNodes := nodeToAdd + minNodes
	log.Info("Horizontal autoscaler", "policy", autoscalingSpec.Name,
		"scope", "tier",
		"count", totalNodes,
		"required_capacity", requiredCapacity,
	)

	nodeSetsResources := nodesets.NewNamedTierResources(autoscalingSpec.Name, nodeSets)
	nodeSetsResources.ResourcesSpecification = nodeCapacity
	fnm := NewFairNodesManager(log, nodeSetsResources.NodeSetNodeCount)
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
