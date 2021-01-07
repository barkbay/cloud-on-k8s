package v1

import "k8s.io/apimachinery/pkg/api/resource"

// - AutoscalingSpec builder

type AutoscalingSpecsBuilder struct {
	name                       string
	nodeCountMin, nodeCountMax int32
	cpu, memory, storage       *QuantityRange
}

func NewAutoscalingSpecsBuilder(name string) *AutoscalingSpecsBuilder {
	return &AutoscalingSpecsBuilder{name: name}
}

func (asb *AutoscalingSpecsBuilder) WithNodeCounts(min, max int) *AutoscalingSpecsBuilder {
	asb.nodeCountMin = int32(min)
	asb.nodeCountMax = int32(max)
	return asb
}

func (asb *AutoscalingSpecsBuilder) WithMemory(min, max string) *AutoscalingSpecsBuilder {
	asb.memory = &QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecsBuilder) WithStorage(min, max string) *AutoscalingSpecsBuilder {
	asb.storage = &QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecsBuilder) WithCpu(min, max string) *AutoscalingSpecsBuilder {
	asb.cpu = &QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecsBuilder) Build() AutoscalingPolicySpec {
	return AutoscalingPolicySpec{
		NamedAutoscalingPolicy: NamedAutoscalingPolicy{
			Name: asb.name,
		},
		AutoscalingResources: AutoscalingResources{
			Cpu:     asb.cpu,
			Memory:  asb.memory,
			Storage: asb.storage,
			NodeCount: CountRange{
				Min: asb.nodeCountMin,
				Max: asb.nodeCountMax,
			},
		},
	}
}
