// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"reflect"
	"testing"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_applyScaleDecision(t *testing.T) {
	type args struct {
		nodeSets         []v1.NodeSet
		requiredCapacity client.RequiredCapacity
		policy           commonv1.ResourcePolicy
	}
	tests := []struct {
		name    string
		args    args
		want    []v1.NodeSet
		wantErr bool
	}{
		{
			name: "Scale both vertically and horizontally",
			args: args{
				nodeSets: []v1.NodeSet{
					newNodeSetBuilder("default", 3).withMemoryRequest("4G").withStorageRequest("1G").build(),
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory(resource.MustParse("5G")).
					tierMemory(resource.MustParse("15G")).
					build(),
				policy: commonv1.ResourcePolicy{
					Roles:      nil,
					MinAllowed: newAllowedResourcesBuilder().withCount(3).withMemory("5G").build(),
					MaxAllowed: commonv1.AllowedResources{Count: nil, Memory: nil},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := applyScaleDecision(tt.args.nodeSets, esv1.ElasticsearchContainerName, tt.args.requiredCapacity, tt.args.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("applyScaleDecision() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyScaleDecision() = %v, want %v", got, tt.want)
			}
		})
	}
}
