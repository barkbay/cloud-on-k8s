package v1

import "k8s.io/apimachinery/pkg/api/resource"

// - AutoscalingSpec builder

// +kubebuilder:object:generate=false
type AutoscalingSpecBuilder struct {
	name                       string
	nodeCountMin, nodeCountMax int32
	cpu, memory, storage       *QuantityRange
}

func NewAutoscalingSpecBuilder(name string) *AutoscalingSpecBuilder {
	return &AutoscalingSpecBuilder{name: name}
}

func (asb *AutoscalingSpecBuilder) WithNodeCounts(min, max int) *AutoscalingSpecBuilder {
	asb.nodeCountMin = int32(min)
	asb.nodeCountMax = int32(max)
	return asb
}

func (asb *AutoscalingSpecBuilder) WithMemory(min, max string) *AutoscalingSpecBuilder {
	asb.memory = &QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecBuilder) WithStorage(min, max string) *AutoscalingSpecBuilder {
	asb.storage = &QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecBuilder) WithCPU(min, max string) *AutoscalingSpecBuilder {
	asb.cpu = &QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecBuilder) Build() AutoscalingPolicySpec {
	return AutoscalingPolicySpec{
		NamedAutoscalingPolicy: NamedAutoscalingPolicy{
			Name: asb.name,
		},
		AutoscalingResources: AutoscalingResources{
			CPU:     asb.cpu,
			Memory:  asb.memory,
			Storage: asb.storage,
			NodeCount: CountRange{
				Min: asb.nodeCountMin,
				Max: asb.nodeCountMax,
			},
		},
	}
}
