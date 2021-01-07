// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fixedClock struct {
	now time.Time
}

func (fc fixedClock) Now() time.Time {
	return fc.now
}

var (
	defaultNow        = time.Date(2010, time.November, 3, 17, 0, 0, 0, time.UTC)
	defaultFixedClock = fixedClock{now: defaultNow}
)

func Test_scaledownFilter(t *testing.T) {
	type ctx struct {
		clock         Clock
		es            esv1.Elasticsearch
		policyName    string
		statusBuilder *status.PolicyStatesBuilder
	}
	type args struct {
		nextNodeSetsResources   nodesets.NamedTierResources
		autoscalingPolicy       esv1.AutoscalingPolicySpec
		actualNodeSetsResources status.Status
	}
	type assertFunc func(t *testing.T, nextNodeSetsResources nodesets.NamedTierResources)
	tests := []struct {
		name       string
		ctx        ctx
		args       args
		assertFunc assertFunc
	}{
		{
			name: "Within a stabilization window: prevent scale down of resources",
			ctx: ctx{
				statusBuilder: status.NewPolicyStatesBuilder(),
				clock:         defaultFixedClock,
				es:            esv1.Elasticsearch{},
				policyName:    "my_policy",
			},
			args: args{
				nextNodeSetsResources: newResourcesBuilder("my-autoscaling-policy").withNodeSet("nodeset-1" /* scale down request to */, 2).withMemoryRequest("6Gi").withCpuRequest("2000").build(),
				autoscalingPolicy:     esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(1, 6).WithMemory("2Gi", "10Gi").WithCpu("1000", "8000").Build(),
				actualNodeSetsResources: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					LastModificationTime:   metav1.NewTime(defaultNow.Add(-2 * time.Minute)),
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "nodeset-1", NodeCount: /* actual node count */ 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("8Gi"), corev1.ResourceCPU: q("4000")}}}},
				},
			},
			assertFunc: func(t *testing.T, nextNodeSetsResources nodesets.NamedTierResources) {
				assert.Equal(t, 1, len(nextNodeSetsResources.NodeSetNodeCount))
				nextNodeSetNodCount := nextNodeSetsResources.NodeSetNodeCount[0]
				// Node count should be the same
				expectedNodeCount := int32(3)
				assert.Equal(t, expectedNodeCount, nextNodeSetNodCount.NodeCount, "expected node count: %d, got %d", expectedNodeCount, nextNodeSetNodCount.NodeCount)
				// Memory should be left intact
				assert.True(t, nextNodeSetsResources.HasRequest(corev1.ResourceMemory))
				currentMemory := nextNodeSetsResources.GetRequest(corev1.ResourceMemory)
				expectedMemory := resource.MustParse("8Gi")
				assert.True(t, currentMemory.Equal(expectedMemory), "expected memory: %s, got %s", expectedMemory.String(), currentMemory.String())
				// Cpu should be left intact
				assert.True(t, nextNodeSetsResources.HasRequest(corev1.ResourceCPU))
				currentCpu := nextNodeSetsResources.GetRequest(corev1.ResourceCPU)
				expectedCpu := resource.MustParse("4000")
				assert.True(t, currentCpu.Equal(expectedCpu), "expected cpu: %s, got %s", expectedCpu.String(), currentCpu.String())
			},
		},
		{
			name: "Outside of a stabilization window: allow scale down of resources",
			ctx: ctx{
				statusBuilder: status.NewPolicyStatesBuilder(),
				clock:         defaultFixedClock,
				es:            esv1.Elasticsearch{},
				policyName:    "my_policy",
			},
			args: args{
				nextNodeSetsResources: newResourcesBuilder("my-autoscaling-policy").withNodeSet("nodeset-1", 2).withMemoryRequest("6Gi").withCpuRequest("2000").build(),
				autoscalingPolicy:     esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(1, 6).WithMemory("2Gi", "10Gi").WithCpu("1000", "8000").Build(),
				actualNodeSetsResources: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					LastModificationTime:   metav1.NewTime(defaultNow.Add(-11 * time.Minute)),
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "nodeset-1", NodeCount: 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("8Gi"), corev1.ResourceCPU: q("4000")}}}},
				},
			},
			assertFunc: func(t *testing.T, nextNodeSetsResources nodesets.NamedTierResources) {
				assert.Equal(t, 1, len(nextNodeSetsResources.NodeSetNodeCount))
				nextNodeSetNodCount := nextNodeSetsResources.NodeSetNodeCount[0]
				// Node count should have been downscaled
				expectedNodeCount := int32(2)
				assert.Equal(t, expectedNodeCount, nextNodeSetNodCount.NodeCount, "expected node count: %d, got %d", expectedNodeCount, nextNodeSetNodCount.NodeCount)
				// Memory should have been downscaled
				assert.True(t, nextNodeSetsResources.HasRequest(corev1.ResourceMemory))
				currentMemory := nextNodeSetsResources.GetRequest(corev1.ResourceMemory)
				expectedMemory := resource.MustParse("6Gi")
				assert.True(t, currentMemory.Equal(expectedMemory), "expected memory: %s, got %s", expectedMemory.String(), currentMemory.String())
				// Cpu should have been downscaled
				assert.True(t, nextNodeSetsResources.HasRequest(corev1.ResourceCPU))
				currentCpu := nextNodeSetsResources.GetRequest(corev1.ResourceCPU)
				expectedCpu := resource.MustParse("2000")
				assert.True(t, currentCpu.Equal(expectedCpu), "expected cpu: %s, got %s", expectedCpu.String(), currentCpu.String())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &stabilizationContext{
				Clock:         tt.ctx.clock,
				es:            tt.ctx.es,
				policyName:    tt.ctx.policyName,
				log:           logTest,
				statusBuilder: tt.ctx.statusBuilder,
			}
			sc.applyScaledownFilter(tt.args.nextNodeSetsResources, tt.args.actualNodeSetsResources)
			tt.assertFunc(t, tt.args.nextNodeSetsResources)
		})
	}
}
