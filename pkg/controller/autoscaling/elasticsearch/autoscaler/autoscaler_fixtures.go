// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// - AutoscalingSpec builder

// +kubebuilder:object:generate=false
type AutoscalingSpecBuilder struct {
	name                       string
	nodeCountMin, nodeCountMax int32
	cpu, memory, storage       *esv1.QuantityRange
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
	asb.memory = &esv1.QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecBuilder) WithStorage(min, max string) *AutoscalingSpecBuilder {
	asb.storage = &esv1.QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecBuilder) WithCPU(min, max string) *AutoscalingSpecBuilder {
	asb.cpu = &esv1.QuantityRange{
		Min: resource.MustParse(min),
		Max: resource.MustParse(max),
	}
	return asb
}

func (asb *AutoscalingSpecBuilder) Build() esv1.AutoscalingPolicySpec {
	return esv1.AutoscalingPolicySpec{
		NamedAutoscalingPolicy: esv1.NamedAutoscalingPolicy{
			Name: asb.name,
		},
		AutoscalingResources: esv1.AutoscalingResources{
			CPU:     asb.cpu,
			Memory:  asb.memory,
			Storage: asb.storage,
			NodeCount: esv1.CountRange{
				Min: asb.nodeCountMin,
				Max: asb.nodeCountMax,
			},
		},
	}
}
