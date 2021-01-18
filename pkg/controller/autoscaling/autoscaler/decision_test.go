// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/resources"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_applyScaleDecision(t *testing.T) {
	defaultNodeSets := esv1.NodeSetList{{
		Name: "default",
	}}
	type args struct {
		currentNodeSets  esv1.NodeSetList
		nodeSetsStatus   status.Status
		requiredCapacity client.PolicyCapacityInfo
		policy           esv1.AutoscalingPolicySpec
	}
	tests := []struct {
		name    string
		args    args
		want    resources.NamedTierResources
		wantErr bool
	}{
		{
			name: "Scale both horizontally to fulfil storage capacity request",
			args: args{
				currentNodeSets: defaultNodeSets,
				nodeSetsStatus: status.Status{AutoscalingPolicyStatuses: []status.AutoscalingPolicyStatus{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("3G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("3Gi").nodeStorage("8Gi").
					tierMemory("9Gi").tierStorage("50Gi").
					build(),
				policy: esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("3Gi", "4Gi").WithStorage("5Gi", "10Gi").Build(),
			},
			want: resources.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 5}},
				ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("3Gi"), corev1.ResourceStorage: q("10Gi")}},
			},
		},
		{
			name: "Scale existing nodes vertically",
			args: args{
				currentNodeSets: defaultNodeSets,
				nodeSetsStatus: status.Status{AutoscalingPolicyStatuses: []status.AutoscalingPolicyStatus{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("3G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					build(),
				policy: esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: resources.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
				ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("6G")}},
			},
		},
		{
			name: "Do not scale down storage capacity",
			args: args{
				currentNodeSets: defaultNodeSets,
				nodeSetsStatus: status.Status{AutoscalingPolicyStatuses: []status.AutoscalingPolicyStatus{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("4G"), corev1.ResourceStorage: q("10G")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("15G").
					nodeStorage("1Gi").
					tierStorage("3Gi").
					build(),
				policy: esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").WithStorage("1G", "20G").Build(),
			},
			want: resources.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
				ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("6G"), corev1.ResourceStorage: q("10G")}},
			},
		},
		{
			name: "Scale existing nodes vertically up to the tier limit",
			args: args{
				currentNodeSets: defaultNodeSets,
				nodeSetsStatus: status.Status{AutoscalingPolicyStatuses: []status.AutoscalingPolicyStatus{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("4G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("21G").
					build(),
				policy: esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: resources.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
				ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("7Gi")}},
			},
		},
		{
			name: "Scale both vertically and horizontally",
			args: args{
				currentNodeSets: defaultNodeSets,
				nodeSetsStatus: status.Status{AutoscalingPolicyStatuses: []status.AutoscalingPolicyStatus{{
					Name:                   "my-autoscaling-policy",
					NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 3}},
					ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("4G"), corev1.ResourceStorage: q("1Gi")}}}},
				},
				requiredCapacity: newRequiredCapacityBuilder().
					nodeMemory("6G").
					tierMemory("48G").
					build(),
				policy: esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithNodeCounts(3, 6).WithMemory("5G", "8G").Build(),
			},
			want: resources.NamedTierResources{
				Name:                   "my-autoscaling-policy",
				NodeSetNodeCount:       []resources.NodeSetNodeCount{{Name: "default", NodeCount: 6}},
				ResourcesSpecification: resources.ResourcesSpecification{Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceMemory: q("8G")}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := Context{
				Log:                     logTest,
				AutoscalingSpec:         tt.args.policy,
				NodeSets:                tt.args.currentNodeSets,
				ActualAutoscalingStatus: tt.args.nodeSetsStatus,
				RequiredCapacity:        tt.args.requiredCapacity,
				StatusBuilder:           status.NewAutoscalingStatusBuilder(),
			}
			if got := ctx.GetScaleDecision(); !equality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("autoscaler.GetScaleDecision() = %v, want %v", got, tt.want)
			}
		})
	}
}

// - PolicyCapacityInfo builder

type requiredCapacityBuilder struct {
	client.PolicyCapacityInfo
}

func newRequiredCapacityBuilder() *requiredCapacityBuilder {
	return &requiredCapacityBuilder{}
}

func ptr(q int64) *client.CapacityValue {
	v := client.CapacityValue(q)
	return &v
}

func (rcb *requiredCapacityBuilder) build() client.PolicyCapacityInfo {
	return rcb.PolicyCapacityInfo
}

func (rcb *requiredCapacityBuilder) nodeMemory(m string) *requiredCapacityBuilder {
	rcb.Node.Memory = ptr(value(m))
	return rcb
}

func (rcb *requiredCapacityBuilder) tierMemory(m string) *requiredCapacityBuilder {
	rcb.Total.Memory = ptr(value(m))
	return rcb
}

func (rcb *requiredCapacityBuilder) nodeStorage(m string) *requiredCapacityBuilder {
	rcb.Node.Storage = ptr(value(m))
	return rcb
}

func (rcb *requiredCapacityBuilder) tierStorage(m string) *requiredCapacityBuilder {
	rcb.Total.Storage = ptr(value(m))
	return rcb
}

func value(v string) int64 {
	q := resource.MustParse(v)
	return q.Value()
}
