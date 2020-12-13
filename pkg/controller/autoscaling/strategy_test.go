// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"testing"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/autoscaler"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/stretchr/testify/assert"
)

func Test_applyScaleDecision(t *testing.T) {
	type args struct {
		currentNodeSets  []string
		nodeSetsStatus   status.NodeSetsStatus
		requiredCapacity client.RequiredCapacity
		policy           esv1.AutoscalingSpec
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]nodesets.NodeSetResources
		wantErr bool
	}{
		{
			name: "Scale both horizontally to fulfil storage capacity request",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.NodeSetsStatus{
					{
						NodeSetResources: nodesets.NodeSetResources{Name: "default", ResourcesSpecification: v1.ResourcesSpecification{Count: 3, Memory: quantityPtr("3G"), Storage: quantityPtr("1Gi")}},
					},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("3Gi").nodeStorage("8Gi").
					tierMemory("9Gi").tierStorage("50Gi").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(3, 6).WithMemory("3Gi", "4Gi").WithStorage("5Gi", "10Gi").Build(),
			},
			want: map[string]nodesets.NodeSetResources{
				"default": newResourcesBuilder("default", 5).withMemoryRequest("3Gi").withStorageRequest("10Gi").build(),
			},
		},
		{
			name: "Scale existing nodes vertically",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.NodeSetsStatus{
					{
						NodeSetResources: nodesets.NodeSetResources{Name: "default", ResourcesSpecification: v1.ResourcesSpecification{Count: 3, Memory: quantityPtr("3G"), Storage: quantityPtr("1Gi")}},
					},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: map[string]nodesets.NodeSetResources{
				"default": newResourcesBuilder("default", 3).withMemoryRequest("6G").build(),
			},
		},
		{
			name: "Do not scale down storage capacity",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.NodeSetsStatus{
					{
						NodeSetResources: nodesets.NodeSetResources{Name: "default", ResourcesSpecification: v1.ResourcesSpecification{Count: 3, Memory: quantityPtr("4G"), Storage: quantityPtr("10G")}},
					},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					nodeStorage("1Gi").
					tierStorage("3Gi").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(3, 6).WithMemory("5G", "8G").WithStorage("1G", "20G").Build(),
			},
			want: map[string]nodesets.NodeSetResources{
				"default": newResourcesBuilder("default", 3).withMemoryRequest("6G").withStorageRequest("10G").build(),
			},
		},
		{
			name: "Scale existing nodes vertically up to the tier limit",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.NodeSetsStatus{
					{
						NodeSetResources: nodesets.NodeSetResources{Name: "default", ResourcesSpecification: v1.ResourcesSpecification{Count: 3, Memory: quantityPtr("4G"), Storage: quantityPtr("1Gi")}},
					},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("21G").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: map[string]nodesets.NodeSetResources{
				"default": newResourcesBuilder("default", 3).withMemoryRequest("7Gi").build(),
			},
		},
		{
			name: "Scale both vertically and horizontally",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.NodeSetsStatus{
					{
						NodeSetResources: nodesets.NodeSetResources{Name: "default", ResourcesSpecification: v1.ResourcesSpecification{Count: 3, Memory: quantityPtr("4G"), Storage: quantityPtr("1Gi")}},
					},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("48G").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: map[string]nodesets.NodeSetResources{
				"default": newResourcesBuilder("default", 6).withMemoryRequest("8G").build(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statusBuilder := status.NewPolicyStatesBuilder()
			got := autoscaler.GetScaleDecision(logTest, tt.args.currentNodeSets, tt.args.nodeSetsStatus, tt.args.requiredCapacity, tt.args.policy, statusBuilder)
			assert.Equal(t, len(tt.want), len(got))
			for nodeSetName, nodeSetResources := range got.ByNodeSet() {
				wantResources, exists := tt.want[nodeSetName]
				assert.True(t, exists)
				assert.Equal(t, wantResources.Count, nodeSetResources.Count)
				if wantResources.Cpu != nil {
					assert.NotNil(t, nodeSetResources.Cpu, "cpu resource expected but got nil")
					if nodeSetResources.Cpu != nil {
						assert.True(t, nodeSetResources.Cpu.Cmp(*wantResources.Cpu) == 0, "Expected CPU %#v, got %#v", wantResources.Cpu, nodeSetResources.Cpu)
					}
				}
				if wantResources.Memory != nil {
					assert.NotNil(t, nodeSetResources.Memory, "memory resource expected but got nil")
					if nodeSetResources.Memory != nil {
						assert.True(t, nodeSetResources.Memory.Cmp(*wantResources.Memory) == 0, "Expected memory %#v, got %#v", wantResources.Memory, nodeSetResources.Memory)
					}
				}

				if wantResources.Storage != nil {
					assert.NotNil(t, nodeSetResources.Storage, "storage resource expected but got nil")
					if nodeSetResources.Storage != nil {
						assert.True(t, nodeSetResources.Storage.Cmp(*wantResources.Storage) == 0, "Expected storage %#v, got %#v", wantResources.Storage, nodeSetResources.Storage)
					}
				}
			}
		})
	}
}

func quantityPtr(quantity string) *resource.Quantity {
	q := resource.MustParse(quantity)
	return &q
}
