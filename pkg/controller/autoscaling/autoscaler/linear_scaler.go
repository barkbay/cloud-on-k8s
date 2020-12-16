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

	// memory is at its lowest value, return the min value for CPU
	if memRatio == 0 {
		requiredCpuCapacity := cpuRange.Min.DeepCopy()
		return &requiredCpuCapacity
	}
	// memory is at its max value, return the max value for CPU
	if memRatio == 1 {
		requiredCpuCapacity := cpuRange.Max.DeepCopy()
		return &requiredCpuCapacity
	}

	allowedCpuRange := float64(cpuRange.Max.MilliValue() - cpuRange.Min.MilliValue())
	requiredAdditionalCpuCapacity := int64(allowedCpuRange * memRatio)
	requiredCpuCapacityAsMilli := cpuRange.Min.MilliValue() + requiredAdditionalCpuCapacity

	// Round up memory to the next core
	requiredCpuCapacityAsMilli = roundUp(requiredCpuCapacityAsMilli, 1000)
	requiredCpuCapacity := resource.NewQuantity(requiredCpuCapacityAsMilli/1000, resource.DecimalSI)
	if requiredCpuCapacity.Cmp(cpuRange.Max) > 0 {
		maxCpuQuantity := cpuRange.Max.DeepCopy()
		requiredCpuCapacity = &maxCpuQuantity
	}
	return requiredCpuCapacity
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
	// storage is at its lowest value, return the min value for memory
	if storageRatio == 0 {
		requiredMemoryCapacity := memoryRange.Min.DeepCopy()
		return &requiredMemoryCapacity
	}
	// storage is at its maximum value, return the max value for memory
	if storageRatio == 1 {
		requiredMemoryCapacity := memoryRange.Max.DeepCopy()
		return &requiredMemoryCapacity
	}

	allowedMemoryRange := float64(memoryRange.Max.Value() - memoryRange.Min.Value())
	requiredAdditionalMemoryCapacity := int64(allowedMemoryRange * storageRatio)
	requiredMemoryCapacity := memoryRange.Min.Value() + requiredAdditionalMemoryCapacity

	// Round up memory to the next GB
	requiredMemoryCapacity = roundUp(requiredMemoryCapacity, giga)
	resourceMemoryAsGiga := resource.MustParse(fmt.Sprintf("%dGi", requiredMemoryCapacity/giga))

	if resourceMemoryAsGiga.Cmp(memoryRange.Max) > 0 {
		resourceMemoryAsGiga = memoryRange.Max.DeepCopy()
	}
	return &resourceMemoryAsGiga
}
