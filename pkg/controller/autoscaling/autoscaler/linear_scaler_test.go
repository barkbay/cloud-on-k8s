// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func quantityPtr(quantity string) *resource.Quantity {
	q := resource.MustParse(quantity)
	return &q
}

func q(quantity string) resource.Quantity {
	return resource.MustParse(quantity)
}

func Test_memoryFromStorage(t *testing.T) {
	type args struct {
		requiredStorageCapacity resource.Quantity
		autoscalingSpec         esv1.AutoscalingPolicySpec
	}
	tests := []struct {
		name       string
		args       args
		wantMemory *resource.Quantity
	}{
		{
			name: "Required storage is at its min. value, return min memory",
			args: args{
				requiredStorageCapacity: q("2Gi"),
				autoscalingSpec:         esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithMemory("3Gi", "6Gi").WithStorage("2Gi", "4Gi").Build(),
			},
			wantMemory: quantityPtr("3Gi"),
		},
		{
			name: "Storage range is 0, keep memory at its minimum",
			args: args{
				requiredStorageCapacity: q("2Gi"),
				autoscalingSpec:         esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithMemory("1Gi", "3Gi").WithStorage("2Gi", "2Gi").Build(),
			},
			wantMemory: quantityPtr("1Gi"), // keep the min. value
		},
		{
			name: "Do not allocate more memory than max allowed",
			args: args{
				requiredStorageCapacity: q("2Gi"),
				autoscalingSpec:         esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithMemory("1Gi", "1500Mi").WithStorage("1Gi", "2Gi").Build(),
			},
			wantMemory: quantityPtr("1500Mi"), // keep the min. value
		},
		{
			name: "Do not allocate more memory than max allowed II",
			args: args{
				requiredStorageCapacity: q("1800Mi"),
				autoscalingSpec:         esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithMemory("1Gi", "1500Mi").WithStorage("1Gi", "2Gi").Build(),
			},
			wantMemory: quantityPtr("1500Mi"), // keep the min. value
		},
		{
			name: "Allocate max of memory when it's possible",
			args: args{
				requiredStorageCapacity: q("2Gi"),
				autoscalingSpec:         esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithMemory("1Gi", "2256Mi").WithStorage("1Gi", "2Gi").Build(),
			},
			wantMemory: quantityPtr("2256Mi"), // keep the min. value
		},
		{
			name: "Half of the storage range should be translated to rounded value of half of the memory range",
			args: args{
				requiredStorageCapacity: q("2Gi"),
				autoscalingSpec:         esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithMemory("1Gi", "3Gi").WithStorage("1Gi", "3Gi").Build(),
			},
			wantMemory: quantityPtr("2Gi"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := memoryFromStorage(tt.args.requiredStorageCapacity, *tt.args.autoscalingSpec.Storage, *tt.args.autoscalingSpec.Memory); !got.Equal(*tt.wantMemory) {
				t.Errorf("memoryFromStorage() = %v, want %v", got, tt.wantMemory)
			}
		})
	}
}

func Test_cpuFromMemory(t *testing.T) {
	type args struct {
		requiredMemoryCapacity resource.Quantity
		autoscalingSpec        esv1.AutoscalingPolicySpec
	}
	tests := []struct {
		name    string
		args    args
		wantCPU *resource.Quantity
	}{
		{
			name: "Memory is at its min value, do not scale up CPU",
			args: args{
				requiredMemoryCapacity: q("2Gi"),
				autoscalingSpec:        esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithCPU("1", "3").WithMemory("2Gi", "2Gi").Build(),
			},
			wantCPU: resource.NewQuantity(1, resource.DecimalSI), // keep the min. value
		},
		{
			name: "1/3 of the memory range should be translated to 1/3 of the CPU range",
			args: args{
				requiredMemoryCapacity: q("2Gi"),
				autoscalingSpec:        esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithCPU("1", "4").WithMemory("1Gi", "4Gi").Build(),
			},
			wantCPU: resource.NewQuantity(2, resource.DecimalSI),
		},
		{
			name: "half of the memory range should be translated to rounded value of half of the CPU range",
			args: args{
				requiredMemoryCapacity: q("2Gi"),
				autoscalingSpec:        esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithCPU("1", "4").WithMemory("1Gi", "3Gi").Build(),
			},
			wantCPU: quantityPtr("3"), // 2500 rounded to 3000
		},
		{
			name: "min and max CPU are equal",
			args: args{
				requiredMemoryCapacity: q("2Gi"),
				autoscalingSpec:        esv1.NewAutoscalingSpecBuilder("my-autoscaling-policy").WithCPU("4", "4").WithMemory("1Gi", "3Gi").Build(),
			},
			wantCPU: quantityPtr("4000m"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cpuFromMemory(tt.args.requiredMemoryCapacity, *tt.args.autoscalingSpec.Memory, *tt.args.autoscalingSpec.CPU); !got.Equal(*tt.wantCPU) {
				t.Errorf("scaleResourceLinearly() = %v, want %v", got, tt.wantCPU)
			}
		})
	}
}
