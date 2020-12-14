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
	"github.com/go-logr/logr"
)

// scaleHorizontally adds or removes nodes in a set of nodeSet to match the requested capacity in a tier.
func scaleHorizontally(
	log logr.Logger,
	nodeSets []string,
	requestedCapacity client.Capacity,
	nodeCapacity esv1.ResourcesSpecification,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.NodeSetsResources {
	// Ensure that we have at least 1 node per nodeSet
	minNodes, maxNodes := adjustMinMaxCount(log, len(nodeSets), autoscalingSpec, statusBuilder)

	nodeSetsResources := make(nodesets.NodeSetsResources, len(nodeSets))
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
					status.HorizontalScalingLimitReached,
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
						status.HorizontalScalingLimitReached,
						fmt.Sprintf("Can't provide total required storage %d, max number of nodes is %d, requires %d nodes", *requestedCapacity.Storage, maxNodes, minNodes+nodeToAddStorage),
					)
				nodeToAddStorage = maxNodes - minNodes
			}
			if nodeToAddStorage > nodeToAdd {
				nodeToAdd = nodeToAddStorage
			}
		}

		// TODO: add a log to explain the computation
		for i := range nodeSets {
			nodeSetsResources[i] = nodesets.NodeSetResources{
				Name:                   nodeSets[i],
				ResourcesSpecification: nodeCapacity,
			}
		}
		totalNodes := nodeToAdd + minNodes
		log.Info("Horizontal autoscaler", "policy", autoscalingSpec.Name, "scope", "tier", "count", totalNodes)
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
