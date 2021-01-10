// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/autoscaler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_applyScaleDecision(t *testing.T) {
	type args struct {
		currentNodeSets  []string
		nodeSetsStatus   status.Status
		requiredCapacity client.CapacityInfo
		policy           esv1.AutoscalingPolicySpec
	}
	tests := []struct {
		name    string
		args    args
		want    nodesets.NamedTierResources
		wantErr bool
	}{
		{
			name: "Scale both horizontally to fulfil storage capacity request",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("3G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("3Gi").nodeStorage("8Gi").
					tierMemory("9Gi").tierStorage("50Gi").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("3Gi", "4Gi").WithStorage("5Gi", "10Gi").Build(),
			},
			want: nodesets.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 5}},
				ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("3Gi"), corev1.ResourceStorage: q("10Gi")}},
			},
		},
		{
			name: "Scale existing nodes vertically",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("3G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: nodesets.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
				ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("6G")}},
			},
		},
		{
			name: "Do not scale down storage capacity",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("4G"), corev1.ResourceStorage: q("10G")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					nodeStorage("1Gi").
					tierStorage("3Gi").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").WithStorage("1G", "20G").Build(),
			},
			want: nodesets.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
				ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("6G"), corev1.ResourceStorage: q("10G")}},
			},
		},
		{
			name: "Scale existing nodes vertically up to the tier limit",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("4G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("21G").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: nodesets.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
				ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("7Gi")}},
			},
		},
		{
			name: "Scale both vertically and horizontally",
			args: args{
				currentNodeSets: []string{"default"},
				nodeSetsStatus: status.Status{PolicyStates: []status.PolicyStateItem{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("4G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("48G").
					build(),
				policy: esv1.NewAutoscalingSpecsBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: nodesets.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []nodesets.NodeSetNodeCount{{Name: "default", NodeCount: 6}},
				ResourcesSpecification: nodesets.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("8G")}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statusBuilder := status.NewPolicyStatesBuilder()
			if got := autoscaler.GetScaleDecision(
				logTest,
				tt.args.currentNodeSets,
				tt.args.nodeSetsStatus,
				tt.args.requiredCapacity,
				tt.args.policy, statusBuilder,
			); !equality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("autoscaler.GetScaleDecision() = %v, want %v", got, tt.want)
			}
		})
	}
}

func q(quantity string) resource.Quantity {
	return resource.MustParse(quantity)
}
