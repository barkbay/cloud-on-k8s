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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

// getStorage returns the storage capacity for the provided autoscaling policy.The storage capacityis always at least the
func getStorage(autoscalingSpec esv1.AutoscalingPolicySpec, actualAutoscalingStatus status.Status) resource.Quantity {
	// If no storage spec is defined in the autoscaling status we return the default volume size.
	storage := volume.DefaultPersistentVolumeSize.DeepCopy()
	// Always adjust to the min value specified by the user in the limits.
	if autoscalingSpec.IsStorageDefined() {
		storage = autoscalingSpec.Storage.Min
	}
	if actualResources, exists := actualAutoscalingStatus.GetNamedTierResources(autoscalingSpec.Name); exists && actualResources.HasRequest(corev1.ResourceStorage) {
		storageInStatus := actualResources.GetRequest(corev1.ResourceStorage)
		// There is a resources definition for this autoscaling policy
		if storageInStatus.Cmp(storage) > 0 {
			storage = storageInStatus
		}
	}
	return storage
}

func GetScaleDecision(
	log logr.Logger,
	nodeSets []string,
	actualAutoscalingStatus status.Status,
	requiredCapacity client.CapacityInfo,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.NamedTierResources {
	// 1. Scale vertically
	desiredNodeResources := scaleVertically(log, len(nodeSets), actualAutoscalingStatus, requiredCapacity, autoscalingSpec, statusBuilder)
	log.Info(
		"Vertical autoscaler",
		"state", "online",
		"policy", autoscalingSpec.Name,
		"scope", "node",
		"nodesets", nodeSets,
		"resources", desiredNodeResources.ToInt64(),
		"required_capacity", requiredCapacity,
	)

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
	actualAutoscalingStatus status.Status,
	requiredCapacity client.CapacityInfo,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	statusBuilder *status.PolicyStatesBuilder,
) nodesets.ResourcesSpecification {
	minNodesCount, _ := adjustMinMaxCount(log, nodeSetsCount, autoscalingSpec, statusBuilder)
	currentStorage := getStorage(autoscalingSpec, actualAutoscalingStatus)

	return nodeResources(
		log,
		int64(minNodesCount),
		currentStorage,
		requiredCapacity,
		autoscalingSpec,
		statusBuilder,
	)
}

// adjustMinMaxCount ensures that the min nodes is at least the same than the number of nodeSets managed by a policy.
// This is is to avoid nodeSets with count set to 0, which is not supported.
func adjustMinMaxCount(
	log logr.Logger,
	nodeSetCount int,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	statusBuilder *status.PolicyStatesBuilder,
) (int, int) {
	minNodes := int(autoscalingSpec.NodeCount.Min)
	maxNodes := int(autoscalingSpec.NodeCount.Max)
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
			"min_count", autoscalingSpec.NodeCount.Min,
			"new_min_count", minNodes,
			"max_count", autoscalingSpec.NodeCount.Max,
			"new_max_count", maxNodes,
		)

		// Update the autoscaling status accordingly
		statusBuilder.
			ForPolicy(autoscalingSpec.Name).
			WithPolicyState(
				status.InvalidMinimumNodeCount,
				fmt.Sprintf("At leats 1 node per nodeSet is required, minimum number of nodes has been adjusted from %d to %d", autoscalingSpec.NodeCount.Min, minNodes),
			)
	}
	return minNodes, maxNodes
}
