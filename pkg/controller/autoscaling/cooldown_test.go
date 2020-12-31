// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"reflect"
	"testing"
	"time"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/go-logr/logr"
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
		nextNodeSetsResources   nodesets.NodeSetsResources
		autoscalingPolicy       esv1.AutoscalingPolicySpec
		actualNodeSetsResources status.NodeSetsResourcesWithMeta
	}
	type assertFunc func(t *testing.T, nextNodeSetsResources nodesets.NodeSetsResources)
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
				nextNodeSetsResources: []nodesets.NodeSetResources{
					newResourcesBuilder("nodeset-1", 2).withMemoryRequest("6Gi").withCpuRequest("2000").build(),
				},
				autoscalingPolicy: esv1.NewAutoscalingSpecsBuilder().
					WithNodeCounts(1, 6).WithMemory("2Gi", "10Gi").WithCpu("1000", "8000").Build(),
				actualNodeSetsResources: []status.NodeSetResourcesWithMeta{
					{
						LastModificationTime: metav1.NewTime(defaultNow.Add(-2 * time.Minute)),
						NodeSetResources:     newResourcesBuilder("nodeset-1", 3).withMemoryRequest("8Gi").withCpuRequest("4000").build(),
					},
				},
			},
			assertFunc: func(t *testing.T, nextNodeSetsResources nodesets.NodeSetsResources) {
				assert.Equal(t, 1, len(nextNodeSetsResources))
				nextNodeSetResources := nextNodeSetsResources[0]
				// Node count should be the same
				expectedNodeCount := int32(3)
				assert.Equal(t, expectedNodeCount, nextNodeSetResources.Count, "expected node count: %d, got %d", expectedNodeCount, nextNodeSetResources.Count)
				// Memory should be left intact
				expectedMemory := resource.MustParse("8Gi")
				assert.True(t, nextNodeSetResources.Memory.Equal(expectedMemory), "expected memory: %s, got %s", expectedMemory.String(), nextNodeSetResources.Memory.String())
				// Cpu should be left intact
				expectedCpu := resource.MustParse("4000")
				assert.True(t, nextNodeSetResources.Cpu.Equal(expectedCpu), "expected cpu: %s, got %s", expectedCpu.String(), nextNodeSetResources.Cpu.String())
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
				nextNodeSetsResources: []nodesets.NodeSetResources{
					newResourcesBuilder("nodeset-1", 2).withMemoryRequest("6Gi").withCpuRequest("2000").build(),
				},
				autoscalingPolicy: esv1.NewAutoscalingSpecsBuilder().
					WithNodeCounts(1, 6).WithMemory("2Gi", "10Gi").WithCpu("1000", "8000").Build(),
				actualNodeSetsResources: []status.NodeSetResourcesWithMeta{
					{
						LastModificationTime: metav1.NewTime(defaultNow.Add(-11 * time.Minute)),
						NodeSetResources:     newResourcesBuilder("nodeset-1", 3).withMemoryRequest("8Gi").withCpuRequest("4000").build(),
					},
				},
			},
			assertFunc: func(t *testing.T, nextNodeSetsResources nodesets.NodeSetsResources) {
				assert.Equal(t, 1, len(nextNodeSetsResources))
				nextNodeSetResources := nextNodeSetsResources[0]
				// Node count should be the same
				expectedNodeCount := int32(2)
				assert.Equal(t, expectedNodeCount, nextNodeSetResources.Count, "expected node count: %d, got %d", expectedNodeCount, nextNodeSetResources.Count)
				// Memory should be left intact
				expectedMemory := resource.MustParse("6Gi")
				assert.True(t, nextNodeSetResources.Memory.Equal(expectedMemory), "expected memory: %s, got %s", expectedMemory.String(), nextNodeSetResources.Memory.String())
				// Cpu should be left intact
				expectedCpu := resource.MustParse("2000")
				assert.True(t, nextNodeSetResources.Cpu.Equal(expectedCpu), "expected cpu: %s, got %s", expectedCpu.String(), nextNodeSetResources.Cpu.String())
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
			sc.applyScaledownFilter(tt.args.nextNodeSetsResources, tt.args.autoscalingPolicy, tt.args.actualNodeSetsResources)
			tt.assertFunc(t, tt.args.nextNodeSetsResources)
		})
	}
}

func Test_stabilizationContext_filterNodeCountScaledown(t *testing.T) {
	type fields struct {
		es            esv1.Elasticsearch
		policyName    string
		log           logr.Logger
		statusBuilder *status.PolicyStatesBuilder
	}
	type args struct {
		actual       int32
		next         int32
		allowedRange esv1.CountRange
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int32
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &stabilizationContext{
				es:            tt.fields.es,
				policyName:    tt.fields.policyName,
				log:           tt.fields.log,
				statusBuilder: tt.fields.statusBuilder,
			}
			if got := sc.filterNodeCountScaledown(tt.args.actual, tt.args.next, tt.args.allowedRange); got != tt.want {
				t.Errorf("stabilizationContext.filterNodeCountScaledown() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stabilizationContext_filterResourceScaledown(t *testing.T) {
	type fields struct {
		es            esv1.Elasticsearch
		policyName    string
		log           logr.Logger
		statusBuilder *status.PolicyStatesBuilder
	}
	type args struct {
		resource     string
		actual       *resource.Quantity
		next         *resource.Quantity
		allowedRange *esv1.QuantityRange
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *resource.Quantity
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &stabilizationContext{
				es:            tt.fields.es,
				policyName:    tt.fields.policyName,
				log:           tt.fields.log,
				statusBuilder: tt.fields.statusBuilder,
			}
			if got := sc.filterResourceScaledown(tt.args.resource, tt.args.actual, tt.args.next, tt.args.allowedRange); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("stabilizationContext.filterResourceScaledown() = %v, want %v", got, tt.want)
			}
		})
	}
}
