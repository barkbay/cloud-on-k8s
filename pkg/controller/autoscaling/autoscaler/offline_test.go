// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"reflect"
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"k8s.io/apimachinery/pkg/api/resource"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var logTest = logf.Log.WithName("autoscaling-offline-test")

func TestEnsureResourcePolicies(t *testing.T) {
	type args struct {
		nodeSets        []string
		autoscalingSpec esv1.AutoscalingPolicySpec
		nodeSetsStatus  status.NodeSetsResourcesWithMeta
	}
	tests := []struct {
		name string
		args args
		want nodesets.NodeSetsResources
	}{
		{
			name: "Do not scale down storage",
			args: args{
				nodeSets:        []string{"region-a", "region-b"},
				autoscalingSpec: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(1, 6).WithMemory("2Gi", "6Gi").WithStorage("10Gi", "20Gi").Build(),
				nodeSetsStatus: []status.NodeSetResourcesWithMeta{
					{NodeSetResources: nodesets.NodeSetResources{Name: "region-a", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("35Gi")}}},
					{NodeSetResources: nodesets.NodeSetResources{Name: "region-b", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("35Gi")}}},
				},
			},
			want: nodesets.NodeSetsResources{
				{Name: "region-a", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("35Gi")}},
				{Name: "region-b", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("35Gi")}},
			},
		},
		{
			name: "Re-use status",
			args: args{
				nodeSets:        []string{"region-a", "region-b"},
				autoscalingSpec: esv1.NewAutoscalingSpecsBuilder().WithNodeCounts(1, 6).WithMemory("2Gi", "6Gi").WithStorage("10Gi", "100Gi").Build(),
				nodeSetsStatus: []status.NodeSetResourcesWithMeta{
					{NodeSetResources: nodesets.NodeSetResources{Name: "region-a", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("15Gi")}}},
					{NodeSetResources: nodesets.NodeSetResources{Name: "region-b", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("15Gi")}}},
				},
			},
			want: nodesets.NodeSetsResources{
				{Name: "region-a", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("15Gi")}},
				{Name: "region-b", ResourcesSpecification: nodesets.ResourcesSpecification{Count: 3, Memory: quantityPtr("3Gi"), Storage: quantityPtr("15Gi")}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EnsureResourcePolicies(logTest, tt.args.nodeSets, tt.args.autoscalingSpec, tt.args.nodeSetsStatus, status.NewPolicyStatesBuilder()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EnsureResourcePolicies() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_adjustQuantity(t *testing.T) {
	type args struct {
		value resource.Quantity
		min   resource.Quantity
		max   resource.Quantity
	}
	tests := []struct {
		name string
		args args
		want *resource.Quantity
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := adjustQuantity(tt.args.value, tt.args.min, tt.args.max); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("adjustQuantity() = %v, want %v", got, tt.want)
			}
		})
	}
}
