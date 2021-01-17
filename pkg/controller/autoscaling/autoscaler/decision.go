// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/resources"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

// getStorage returns the storage min. capacity that should be used in the autoscaling algorithm.
// The value is the max. value of either the current value in the status or in the autoscaling spec. set by the user.
func getStorage(autoscalingSpec esv1.AutoscalingPolicySpec, actualAutoscalingStatus status.Status) resource.Quantity {
	// If no storage spec is defined in the autoscaling status we return the default volume size.
	storage := volume.DefaultPersistentVolumeSize.DeepCopy()
	// Always adjust to the min value specified by the user in the limits.
	if autoscalingSpec.IsStorageDefined() {
		storage = autoscalingSpec.Storage.Min
	}
	// If a storage value is stored in the status then reuse it.
	if actualResources, exists := actualAutoscalingStatus.GetNamedTierResources(autoscalingSpec.Name); exists && actualResources.HasRequest(corev1.ResourceStorage) {
		storageInStatus := actualResources.GetRequest(corev1.ResourceStorage)
		// There is a resources definition for this autoscaling policy
		if storageInStatus.Cmp(storage) > 0 {
			storage = storageInStatus
		}
	}
	return storage
}

// GetScaleDecision calculates the resources to be required by NodeSets managed by a same autoscaling policy.
func GetScaleDecision(
	log logr.Logger,
	nodeSets []string,
	actualAutoscalingStatus status.Status,
	requiredCapacity client.PolicyCapacityInfo,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	statusBuilder *status.AutoscalingStatusBuilder,
) resources.NamedTierResources {
	// 1. Scale vertically
	desiredNodeResources := scaleVertically(log, actualAutoscalingStatus, requiredCapacity, autoscalingSpec, statusBuilder)
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
	actualAutoscalingStatus status.Status,
	requiredCapacity client.PolicyCapacityInfo,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	statusBuilder *status.AutoscalingStatusBuilder,
) resources.ResourcesSpecification {
	// All resources can be computed "from scratch", without knowing the previous values.
	// It is not true for storage. Storage can't be scaled down, current storage capacity must be considered as an hard min. limit.
	// This limit must be taken into consideration when computing the desired resources.
	currentStorage := getStorage(autoscalingSpec, actualAutoscalingStatus)
	return nodeResources(
		log,
		int64(autoscalingSpec.NodeCount.Min),
		currentStorage,
		requiredCapacity,
		autoscalingSpec,
		statusBuilder,
	)
}
