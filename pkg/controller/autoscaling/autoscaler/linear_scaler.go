// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// cpuFromMemory computes a CPU quantity within the specified allowed range by the user proportionally
// to the amount of memory requested by the autoscaling API.
func cpuFromMemory(requiredMemoryCapacity int64, memoryRange, cpuRange esv1.QuantityRange) *resource.Quantity {
	allowedMemoryRange := memoryRange.Max.Value() - memoryRange.Min.Value()
	if allowedMemoryRange == 0 {
		// Can't scale CPU as min and max for memory are equal
		minCpu := cpuRange.Min.DeepCopy()
		return &minCpu
	}
	memRatio := float64(requiredMemoryCapacity-memoryRange.Min.Value()) / float64(allowedMemoryRange)
	allowedCpuRange := float64(cpuRange.Max.MilliValue() - cpuRange.Min.MilliValue())
	requiredAdditionalCpuCapacity := int64(allowedCpuRange * memRatio)
	requiredCpuCapacity := cpuRange.Min.MilliValue() + requiredAdditionalCpuCapacity

	if requiredCpuCapacity%1000 == 0 {
		return resource.NewQuantity(requiredCpuCapacity/1000, resource.DecimalSI)
	}
	return resource.NewMilliQuantity(requiredCpuCapacity, resource.DecimalSI)
}

// memoryFromStorage computes a memory quantity within the specified allowed range by the user proportionally
// to the amount of storage requested by the autoscaling API.
func memoryFromStorage(requiredStorageCapacity int64, storageRange, memoryRange esv1.QuantityRange) *resource.Quantity {
	allowedStorageRange := storageRange.Max.Value() - storageRange.Min.Value()
	if allowedStorageRange == 0 {
		// Can't scale CPU as min and max for memory are equal
		minCpu := memoryRange.Min.DeepCopy()
		return &minCpu
	}
	storageRatio := float64(requiredStorageCapacity-storageRange.Min.Value()) / float64(allowedStorageRange)
	allowedMemoryRange := float64(memoryRange.Max.Value() - memoryRange.Min.Value())
	requiredAdditionalMemoryCapacity := int64(allowedMemoryRange * storageRatio)
	requiredMemoryCapacity := memoryRange.Min.Value() + requiredAdditionalMemoryCapacity

	var memoryQuantity *resource.Quantity
	if requiredMemoryCapacity >= giga && requiredMemoryCapacity%giga == 0 {
		// When it's possible we may want to express the memory with a "human readable unit" like the the Gi unit
		resourceMemoryAsGiga := resource.MustParse(fmt.Sprintf("%dGi", requiredMemoryCapacity/giga))
		memoryQuantity = &resourceMemoryAsGiga
	} else {
		memoryQuantity = resource.NewQuantity(requiredMemoryCapacity, resource.DecimalSI)
	}
	return memoryQuantity
}
