// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"fmt"
	"time"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const scaleupStabilizationMessage = "scale up stabilization window in progress, %s will be scaled down only if forced using policy limits"

var defaultScaleUpStabilizationWindow = 10 * time.Minute

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (rc RealClock) Now() time.Time {
	return time.Now()
}

type stabilizationContext struct {
	Clock
	es            esv1.Elasticsearch
	policyName    string
	log           logr.Logger
	statusBuilder *status.AutoscalingStatusBuilder
}

func applyCoolDownFilters(
	log logr.Logger,
	es esv1.Elasticsearch,
	nextNodeSetsResources nodesets.NamedTierResources,
	autoscalingPolicy esv1.AutoscalingPolicySpec,
	actualAutoscalingStatus status.Status,
	statusBuilder *status.AutoscalingStatusBuilder,
) {
	sc := stabilizationContext{
		Clock:         RealClock{},
		es:            es,
		policyName:    autoscalingPolicy.Name,
		log:           log,
		statusBuilder: statusBuilder,
	}
	sc.applyScaledownFilter(nextNodeSetsResources, actualAutoscalingStatus)
}

// applyScaledownFilter prevents scale down of resources while in the scale up stabilization window
func (sc *stabilizationContext) applyScaledownFilter(
	nextNodeSetsResources nodesets.NamedTierResources,
	actualAutoscalingStatus status.Status,
) {
	actualNodeSetsResources, exists := actualAutoscalingStatus.GetNamedTierResources(nextNodeSetsResources.Name)
	if !exists {
		return
	}
	now := sc.Now()
	lastModificationTime, exists := actualAutoscalingStatus.GetLastModificationTime(nextNodeSetsResources.Name)
	if !exists {
		return
	}
	if lastModificationTime.Add(defaultScaleUpStabilizationWindow).Before(now) {
		return
	}

	// Memory
	if actualNodeSetsResources.HasRequest(corev1.ResourceMemory) && nextNodeSetsResources.HasRequest(corev1.ResourceMemory) {
		nextNodeSetsResources.SetRequest(corev1.ResourceMemory, sc.filterResourceScaledown("memory", actualNodeSetsResources.GetRequest(corev1.ResourceMemory), nextNodeSetsResources.GetRequest(corev1.ResourceMemory)))
	}
	// CPU
	if actualNodeSetsResources.HasRequest(corev1.ResourceCPU) && nextNodeSetsResources.HasRequest(corev1.ResourceCPU) {
		nextNodeSetsResources.SetRequest(corev1.ResourceCPU, sc.filterResourceScaledown("cpu", actualNodeSetsResources.GetRequest(corev1.ResourceCPU), nextNodeSetsResources.GetRequest(corev1.ResourceCPU)))
	}

	// Node count
	actualByNodeSet := actualNodeSetsResources.NodeSetNodeCount.ByNodeSet()
	for i := range nextNodeSetsResources.NodeSetNodeCount {
		nextNodeSetName := nextNodeSetsResources.NodeSetNodeCount[i].Name
		actualNodeCount, hasActualNodeCount := actualByNodeSet[nextNodeSetName]
		if !hasActualNodeCount {
			continue
		}
		nextNodeSetsResources.NodeSetNodeCount[i].NodeCount = sc.filterNodeCountScaledown(actualNodeCount, nextNodeSetsResources.NodeSetNodeCount[i].NodeCount)
	}
}

func (sc *stabilizationContext) filterNodeCountScaledown(actual, next int32) int32 {
	if next >= actual {
		return next
	}

	sc.statusBuilder.ForPolicy(sc.policyName).WithEvent(status.ScaleUpStabilizationWindow, fmt.Sprintf(scaleupStabilizationMessage, "node count"))
	sc.log.Info("stabilization window", "policy", sc.policyName, "required_count", next, "actual_count", actual)

	return actual
}

func (sc *stabilizationContext) filterResourceScaledown(resourceType string, actual, next resource.Quantity,
) resource.Quantity {
	if next.Cmp(actual) >= 0 {
		// not scaling down, ignore
		return next
	}

	sc.statusBuilder.ForPolicy(sc.policyName).WithEvent(
		status.ScaleUpStabilizationWindow,
		fmt.Sprintf("%s not scaled down because of scale up stabilization window", resourceType),
	)
	sc.log.Info("stabilization window", "policy", sc.policyName, "required_"+resourceType, next, "actual_"+resourceType, actual)
	return actual
}
