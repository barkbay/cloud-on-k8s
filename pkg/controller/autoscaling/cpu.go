// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// cpuFromMemory computes a CPU quantity within the specified allowed range by the user proportionally
// to the amount of memory requested by the autoscaling API.
func cpuFromMemory(requiredMemoryCapacity int64, autoscalingSpec esv1.AutoscalingSpec) *resource.Quantity {
	allowedMemoryRange := autoscalingSpec.MaxAllowed.Memory.Value() - autoscalingSpec.MinAllowed.Memory.Value()
	if allowedMemoryRange == 0 {
		// Can't scale CPU as min and max for memory are equal
		minCpu := autoscalingSpec.MinAllowed.Cpu.DeepCopy()
		return &minCpu
	}
	memRatio := float64(requiredMemoryCapacity-autoscalingSpec.MinAllowed.Memory.Value()) / float64(allowedMemoryRange)
	allowedCpuRange := float64(autoscalingSpec.MaxAllowed.Cpu.MilliValue() - autoscalingSpec.MinAllowed.Cpu.MilliValue())
	requiredAdditionalCpuCapacity := int64(allowedCpuRange * memRatio)
	requiredCpuCapacity := autoscalingSpec.MinAllowed.Cpu.MilliValue() + requiredAdditionalCpuCapacity

	if requiredCpuCapacity%1000 == 0 {
		return resource.NewQuantity(requiredCpuCapacity/1000, resource.DecimalSI)
	}
	return resource.NewMilliQuantity(requiredCpuCapacity, resource.DecimalSI)
}
