package nodesets

import "k8s.io/apimachinery/pkg/api/resource"

type NodeSetsResources []NodeSetResources

type NodeSetResources struct {
	Name string `json:"name,omitempty"`
	ResourcesSpecification
}

// ResourcesSpecification holds the result of the autoscaling algorithm.
type ResourcesSpecification struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	Count int32
	// Cpu represents the CPU value
	Cpu *resource.Quantity
	// memory represents the memory value
	Memory *resource.Quantity
	// storage represents the storage capacity value
	Storage *resource.Quantity
}

// ResourcesSpecificationInt64 is mostly use in logs to print comparable values.
type ResourcesSpecificationInt64 struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	Count int32 `json:"count"`
	// Cpu represents the CPU value
	Cpu *int64 `json:"cpu"`
	// memory represents the memory value
	Memory *int64 `json:"memory"`
	// storage represents the storage capacity value
	Storage *int64 `json:"storage"`
}

func (rs ResourcesSpecification) ToInt64() ResourcesSpecificationInt64 {
	rs64 := ResourcesSpecificationInt64{
		Count: rs.Count,
	}
	if rs.Cpu != nil {
		cpu := rs.Cpu.MilliValue()
		rs64.Cpu = &cpu
	}
	if rs.Memory != nil {
		memory := rs.Memory.Value()
		rs64.Memory = &memory
	}
	if rs.Storage != nil {
		storage := rs.Storage.Value()
		rs64.Storage = &storage
	}
	return rs64
}

func (rs ResourcesSpecification) IsMemoryDefined() bool {
	return rs.Memory != nil
}

func (rs ResourcesSpecification) IsCpuDefined() bool {
	return rs.Cpu != nil
}

func (rs ResourcesSpecification) IsStorageDefined() bool {
	return rs.Storage != nil
}

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

func (nsr NodeSetsResources) ByNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources, len(nsr))
	for _, nodeSetsResource := range nsr {
		byNodeSet[nodeSetsResource.Name] = nodeSetsResource
	}
	return byNodeSet
}
