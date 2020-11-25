// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

func Test_applyScaleDecision(t *testing.T) {
	type args struct {
		currentNodeSets  []v1.NodeSet
		requiredCapacity client.RequiredCapacity
		policy           commonv1.ResourcePolicy
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]v1.NodeSet
		wantErr bool
	}{
		{
			name: "Scale existing nodes vertically",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1G").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					build(),
				policy: commonv1.ResourcePolicy{
					AllowedResources: commonv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").build(),
					},
				},
			},
			want: map[string]v1.NodeSet{
				"default": newNodeSetBuilder("default", 3).withMemoryRequest("6G").withStorageRequest("1G").build(),
			},
		},
		{
			name: "Scale existing nodes vertically up to the tier limit",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1G").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("21G").
					build(),
				policy: commonv1.ResourcePolicy{
					AllowedResources: commonv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").build(),
					},
				},
			},
			want: map[string]v1.NodeSet{
				"default": newNodeSetBuilder("default", 3).withMemoryRequest("7Gi").withStorageRequest("1G").build(),
			},
		},
		{
			name: "Scale both vertically and horizontally",
			args: args{
				currentNodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1G").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("48G").
					build(),
				policy: commonv1.ResourcePolicy{
					AllowedResources: commonv1.AllowedResources{
						MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
						MaxAllowed: newAllowedResourcesBuilder().withCount(6).withMemory("8G").build(),
					},
				},
			},
			want: map[string]v1.NodeSet{
				"default": newNodeSetBuilder("default", 6).withMemoryRequest("8G").withStorageRequest("1G").build(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := applyScaleDecision(tt.args.currentNodeSets, esv1.ElasticsearchContainerName, tt.args.requiredCapacity, tt.args.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("applyScaleDecision() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, len(tt.want), len(got))
			var diff []string
			for _, nodeSet := range got {
				expectedNodeSet, exists := tt.want[nodeSet.Name]
				assert.True(t, exists)
				diff = append(diff, resourcesDiff(expectedNodeSet, nodeSet)...)
			}

			if len(diff) > 0 {
				t.Errorf("applyScaleDecision() diff = %v", diff)
			}
		})
	}
}
