// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_cpuFromMemory(t *testing.T) {
	type args struct {
		requiredMemoryCapacity int64
		autoscalingSpec        esv1.AutoscalingSpec
	}
	tests := []struct {
		name    string
		args    args
		wantCpu *resource.Quantity
	}{
		{
			name: "Memory is at its min value, do not scale up CPU",
			args: args{
				requiredMemoryCapacity: 2147483648,
				autoscalingSpec: esv1.AutoscalingSpec{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("1"),
							Memory: quantityPtr("2Gi"),
						},
						MaxAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("3"),
							Memory: quantityPtr("2Gi"),
						},
					},
				},
			},
			wantCpu: resource.NewQuantity(1, resource.DecimalSI), // keep the min. value
		},
		{
			name: "1/3 of the memory range should be translated to 1/3 of the CPU range",
			args: args{
				requiredMemoryCapacity: 2147483648,
				autoscalingSpec: esv1.AutoscalingSpec{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("1"),
							Memory: quantityPtr("1Gi"),
						},
						MaxAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("4"),
							Memory: quantityPtr("4Gi"),
						},
					},
				},
			},
			wantCpu: resource.NewQuantity(2, resource.DecimalSI),
		},
		{
			name: "half of the memory range should be translated to half of the CPU range",
			args: args{
				requiredMemoryCapacity: 2147483648,
				autoscalingSpec: esv1.AutoscalingSpec{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("1"),
							Memory: quantityPtr("1Gi"),
						},
						MaxAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("4"),
							Memory: quantityPtr("3Gi"),
						},
					},
				},
			},
			wantCpu: quantityPtr("2500m"),
		},
		{
			name: "min memory == max memory, do not scale cpu",
			args: args{
				requiredMemoryCapacity: 2147483648,
				autoscalingSpec: esv1.AutoscalingSpec{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("2"),
							Memory: quantityPtr("2Gi"),
						},
						MaxAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("4"),
							Memory: quantityPtr("2Gi"),
						},
					},
				},
			},
			wantCpu: quantityPtr("2"),
		},
		{
			name: "min and max CPU are equal",
			args: args{
				requiredMemoryCapacity: 2147483648,
				autoscalingSpec: esv1.AutoscalingSpec{
					AllowedResources: esv1.AllowedResources{
						MinAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("4"),
							Memory: quantityPtr("1Gi"),
						},
						MaxAllowed: esv1.ResourcesSpecification{
							Cpu:    quantityPtr("4"),
							Memory: quantityPtr("3Gi"),
						},
					},
				},
			},
			wantCpu: quantityPtr("4000m"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cpuFromMemory(tt.args.requiredMemoryCapacity, tt.args.autoscalingSpec); !got.Equal(*tt.wantCpu) {
				t.Errorf("cpuFromMemory() = %v, want %v", got, tt.wantCpu)
			}
		})
	}
}
