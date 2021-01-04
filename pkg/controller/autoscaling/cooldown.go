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
	statusBuilder *status.PolicyStatesBuilder
}

func applyCoolDownFilters(
	log logr.Logger,
	es esv1.Elasticsearch,
	nextNodeSetsResources nodesets.NodeSetsResources,
	autoscalingPolicy esv1.AutoscalingPolicySpec,
	actualNodeSetsResources status.NodeSetsResourcesWithMeta,
	statusBuilder *status.PolicyStatesBuilder,
) {
	sc := stabilizationContext{
		Clock:         RealClock{},
		es:            es,
		policyName:    autoscalingPolicy.Name,
		log:           log,
		statusBuilder: statusBuilder,
	}
	sc.applyScaledownFilter(nextNodeSetsResources, autoscalingPolicy, actualNodeSetsResources)
}

// applyScaledownFilter prevents scale down of resources while in the scale up stabilization window
func (sc *stabilizationContext) applyScaledownFilter(
	nextNodeSetsResources nodesets.NodeSetsResources,
	autoscalingPolicy esv1.AutoscalingPolicySpec,
	actualNodeSetsResources status.NodeSetsResourcesWithMeta,
) {
	now := sc.Now()
	for i := range nextNodeSetsResources {
		actualNodeSetsResourcesByNodeSet := actualNodeSetsResources.ByNodeSet()
		actualNodeSetResources, exists := actualNodeSetsResourcesByNodeSet[nextNodeSetsResources[i].Name]
		if !exists {
			continue
		}
		if actualNodeSetResources.LastModificationTime.Add(defaultScaleUpStabilizationWindow).Before(now) {
			continue
		}
		// Memory
		if actualNodeSetResources.Memory != nil && nextNodeSetsResources[i].Memory != nil && autoscalingPolicy.IsMemoryDefined() {
			nextNodeSetsResources[i].Memory = sc.filterResourceScaledown("memory", actualNodeSetResources.Memory, nextNodeSetsResources[i].Memory, autoscalingPolicy.Memory)
		}
		// CPU
		if actualNodeSetResources.Cpu != nil && nextNodeSetsResources[i].Cpu != nil && autoscalingPolicy.IsCpuDefined() {
			nextNodeSetsResources[i].Cpu = sc.filterResourceScaledown("cpu", actualNodeSetResources.Cpu, nextNodeSetsResources[i].Cpu, autoscalingPolicy.Cpu)
		}
		// Node count
		nextNodeSetsResources[i].Count = sc.filterNodeCountScaledown(actualNodeSetResources.Count, nextNodeSetsResources[i].Count, autoscalingPolicy.NodeCount)
	}
}

func (sc *stabilizationContext) filterNodeCountScaledown(actual, next int32, allowedRange esv1.CountRange) int32 {
	if next >= actual {
		return next
	}

	sc.statusBuilder.ForPolicy(sc.policyName).WithPolicyState(status.ScaleUpStabilizationWindow, fmt.Sprintf(scaleupStabilizationMessage, "node count"))
	sc.log.Info("stabilization window", "policy", sc.policyName, "required_count", next, "actual_count", actual)

	// We still want to ensure that the next value complies with the limits provided by the user
	nodeCount := actual
	if nodeCount > allowedRange.Max {
		nodeCount = allowedRange.Max
	}
	if nodeCount < allowedRange.Min {
		nodeCount = allowedRange.Min
	}
	return nodeCount
}

func (sc *stabilizationContext) filterResourceScaledown(
	resourceType string,
	actual, next *resource.Quantity,
	allowedRange *esv1.QuantityRange,
) *resource.Quantity {
	if next.Cmp(*actual) >= 0 {
		// not scaling down, ignore
		return next
	}

	sc.statusBuilder.ForPolicy(sc.policyName).WithPolicyState(
		status.ScaleUpStabilizationWindow,
		fmt.Sprintf("%s not scaled down because of scale up stabilization window", resourceType),
	)
	sc.log.Info("stabilization window", "policy", sc.policyName, "required_"+resourceType, next, "actual_"+resourceType, actual)
	q := actual.DeepCopy()

	if allowedRange == nil {
		return &q
	}
	// Even if we are in a stabilization window we still want to ensure that the next value complies with the limits provided by the user
	if q.Cmp(allowedRange.Max) > 0 {
		q = allowedRange.Max.DeepCopy()
	}
	if q.Cmp(allowedRange.Min) < 0 {
		q = allowedRange.Min.DeepCopy()
	}
	return &q
}
