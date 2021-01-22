// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/elasticsearch/resources"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/elasticsearch/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// GetResources calculates the resources to be required by NodeSets managed by a same autoscaling policy.
func (ctx *Context) GetResources() resources.NodeSetsResources {
	// 1. Scale vertically
	desiredNodeResources := ctx.scaleVertically()
	ctx.Log.Info(
		"Vertical autoscaler",
		"state", "online",
		"policy", ctx.AutoscalingSpec.Name,
		"scope", "node",
		"nodesets", ctx.NodeSets.Names(),
		"resources", desiredNodeResources.ToInt64(),
		"required_capacity", ctx.RequiredCapacity,
	)

	// 2. Scale horizontally
	return ctx.scaleHorizontally(ctx.RequiredCapacity.Total, desiredNodeResources)
}

// scaleVertically computes the desired resources for a node given the requested capacity from ES and the AutoscalingSpec
// specified by the user. It attempts to scale all the resources vertically until the expectations are met.
func (ctx *Context) scaleVertically() resources.NodeResources {
	// All resources can be computed "from scratch", without knowing the previous values.
	// It is not true for storage. Storage can't be scaled down, current storage capacity must be considered as an hard min. limit.
	// This limit must be taken into consideration when computing the desired resources.
	currentStorage := getStorage(ctx.AutoscalingSpec, ctx.ActualAutoscalingStatus)
	return ctx.nodeResources(
		int64(ctx.AutoscalingSpec.NodeCount.Min),
		currentStorage,
	)
}

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
