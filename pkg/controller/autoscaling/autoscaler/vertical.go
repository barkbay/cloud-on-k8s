// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/resource"
)

// nodeResources computes the desired amount of memory and storage
func nodeResources(
	log logr.Logger,
	minNodesCount int64,
	currentStorage resource.Quantity,
	requiredCapacity client.RequiredCapacity,
	autoscalingSpec esv1.AutoscalingSpec,
	statusBuilder *status.PolicyStatesBuilder,
) esv1.ResourcesSpecification {
	resources := esv1.ResourcesSpecification{}

	// Get memory
	if requiredCapacity.Node.Memory != nil &&
		autoscalingSpec.IsMemoryDefined() {
		resources.Memory = getResourceValue(
			log,
			autoscalingSpec.Name,
			"memory",
			statusBuilder,
			*requiredCapacity.Node.Memory,
			requiredCapacity.Total.Memory,
			minNodesCount,
			autoscalingSpec.Memory.Min,
			autoscalingSpec.Memory.Max,
		)
	}

	// Get storage
	if requiredCapacity.Node.Storage != nil &&
		autoscalingSpec.IsStorageDefined() {
		resources.Storage = getResourceValue(
			log,
			autoscalingSpec.Name,
			"memory",
			statusBuilder,
			*requiredCapacity.Node.Storage,
			requiredCapacity.Total.Storage,
			minNodesCount,
			autoscalingSpec.Storage.Min,
			autoscalingSpec.Storage.Max,
		)
		if resources.Storage.Cmp(currentStorage) < 0 {
			// Do not decrease storage capacity
			resources.Storage = &currentStorage
		}
	}

	// Adjust CPU
	if autoscalingSpec.IsCpuDefined() && autoscalingSpec.IsMemoryDefined() && resources.Memory != nil {
		resources.Cpu = cpuFromMemory(resources.Memory.Value(), autoscalingSpec)
	}

	return resources
}

func getResourceValue(
	log logr.Logger,
	autoscalingPolicyName, resourceType string,
	statusBuilder *status.PolicyStatesBuilder,
	nodeRequired int64, // node required capacity as returned by the Elasticsearch API
	totalRequired *int64, // tier required capacity as returned by the Elasticsearch API, considered as optional
	minNodesCount int64,
	min, max resource.Quantity, // as expressed by the user
) *resource.Quantity {
	// Surface the condition where resource is exhausted.
	if nodeRequired > max.Value() {
		// Elasticsearch requested more capacity per node than allowed by the user
		err := fmt.Errorf("node required %s is greater than the maximum one", resourceType)
		log.Error(
			err, err.Error(),
			"scope", "node",
			"policy", autoscalingPolicyName,
			"required_"+resourceType, nodeRequired,
			"max_allowed_memory", max.Value(),
		)
		// Update the autoscaling status accordingly
		statusBuilder.
			ForPolicy(autoscalingPolicyName).
			WithPolicyState(
				status.VerticalScalingLimitReached,
				fmt.Sprintf("Node required %s %d is greater than max allowed: %d", resourceType, nodeRequired, max.Value()),
			)
	}

	nodeResource := nodeRequired
	// Adjust the node requested capacity to try to fit the tier requested capacity
	if totalRequired != nil {
		memoryOverAllTiers := *totalRequired / minNodesCount
		nodeResource = max64(nodeResource, roundUp(memoryOverAllTiers, giga))
	}

	// Set desired memory capacity within the allowed range
	if nodeResource < min.Value() {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		nodeResource = min.Value()
	}
	if nodeResource > max.Value() {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		nodeResource = max.Value()
	}

	var nodeQuantity *resource.Quantity
	if nodeResource >= giga && nodeResource%giga == 0 {
		// When it's possible we may want to express the memory with a "human readable unit" like the the Gi unit
		resourceMemoryAsGiga := resource.MustParse(fmt.Sprintf("%dGi", nodeResource/giga))
		nodeQuantity = &resourceMemoryAsGiga
	} else {
		nodeQuantity = resource.NewQuantity(nodeResource, resource.DecimalSI)
	}

	return nodeQuantity
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
