// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/stretchr/testify/assert"
)

func Test_applyScaleDecision(t *testing.T) {
	type args struct {
		currentNodeSets  []esv1.NodeSet
		requiredCapacity client.RequiredCapacity
		policy           esv1.ResourcePolicy
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]NodeSetResources
		wantErr bool
	}{
		{
			name: "Scale existing nodes vertically",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1Gi").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					build(),
				policy: esv1.ResourcePolicy{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").build(),
					},
				},
			},
			want: map[string]NodeSetResources{
				"default": newResourcesBuilder("default", 3).withMemoryRequest("6G").withStorageRequest("1Gi").build(),
			},
		},
		{
			name: "Do not scale down storage capacity",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("10G").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					nodeStorage("1Gi").
					tierStorage("3Gi").
					build(),
				policy: esv1.ResourcePolicy{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").withStorage("1G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").withStorage("20G").build(),
					},
				},
			},
			want: map[string]NodeSetResources{
				"default": newResourcesBuilder("default", 3).withMemoryRequest("6G").withStorageRequest("10G").build(),
			},
		},
		{
			name: "Scale existing nodes vertically up to the tier limit",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1Gi").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("21G").
					build(),
				policy: esv1.ResourcePolicy{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").build(),
					},
				},
			},
			want: map[string]NodeSetResources{
				"default": newResourcesBuilder("default", 3).withMemoryRequest("7Gi").withStorageRequest("1Gi").build(),
			},
		},
		{
			name: "Scale both vertically and horizontally",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1Gi").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("48G").
					build(),
				policy: esv1.ResourcePolicy{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").build(),
					},
				},
			},
			want: map[string]NodeSetResources{
				"default": newResourcesBuilder("default", 6).withMemoryRequest("8G").withStorageRequest("1Gi").build(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getScaleDecision(tt.args.currentNodeSets, tt.args.requiredCapacity, tt.args.policy)
			assert.Equal(t, len(tt.want), len(got))
			for nodeSetName, nodeSetResources := range got.byNodeSet() {
				wantResources, exists := tt.want[nodeSetName]
				assert.True(t, exists)
				assert.Equal(t, wantResources.Count, nodeSetResources.Count)
				if wantResources.Cpu != nil {
					assert.True(t, nodeSetResources.Cpu.Cmp(*wantResources.Cpu) == 0, "Expected CPU %#v, got %#v", wantResources.Cpu, nodeSetResources.Cpu)
				}
				if wantResources.Memory != nil {
					assert.True(t, nodeSetResources.Memory.Cmp(*wantResources.Memory) == 0, "Expected memory %#v, got %#v", wantResources.Memory, nodeSetResources.Memory)
				}

				if wantResources.Storage != nil {
					assert.True(t, nodeSetResources.Storage.Cmp(*wantResources.Storage) == 0, "Expected storage %#v, got %#v", wantResources.Storage, nodeSetResources.Storage)
				}
			}
		})
	}
}
