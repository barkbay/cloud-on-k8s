// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var logTest = logf.Log.WithName("autoscaling-test")

// - NodeSetResources builder

type resourcesBuilder struct {
	name     string
	count    nodesets.NodeSetNodeCountList
	requests corev1.ResourceList
}

func newResourcesBuilder(name string) *resourcesBuilder {
	return &resourcesBuilder{
		name:     name,
		requests: make(corev1.ResourceList),
	}
}

func (nsb *resourcesBuilder) withNodeSet(name string, count int) *resourcesBuilder {
	nsb.count = append(nsb.count, nodesets.NodeSetNodeCount{
		Name:      name,
		NodeCount: int32(count),
	})
	return nsb
}

func (nsb *resourcesBuilder) withMemoryRequest(qs string) *resourcesBuilder {
	nsb.requests[corev1.ResourceMemory] = resource.MustParse(qs)
	return nsb
}

func (nsb *resourcesBuilder) withStorageRequest(qs string) *resourcesBuilder {
	nsb.requests[corev1.ResourceStorage] = resource.MustParse(qs)
	return nsb
}

func (nsb *resourcesBuilder) withCPURequest(qs string) *resourcesBuilder {
	nsb.requests[corev1.ResourceCPU] = resource.MustParse(qs)
	return nsb
}

func (nsb *resourcesBuilder) build() nodesets.NamedTierResources {
	nodeSetResources := nodesets.NamedTierResources{
		Name:             nsb.name,
		NodeSetNodeCount: nsb.count,
		ResourcesSpecification: nodesets.ResourcesSpecification{
			Requests: nsb.requests,
		},
	}
	return nodeSetResources
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
	return (&q).Value()
}
