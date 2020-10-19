// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// - NodeSet builder

type nodeSetBuilder struct {
	name           string
	count          int32
	memoryRequest  *resource.Quantity
	storageRequest *resource.Quantity
}

func newNodeSetBuilder(name string, count int) *nodeSetBuilder {
	return &nodeSetBuilder{
		name:  name,
		count: int32(count),
	}
}

func (nsb *nodeSetBuilder) withMemoryRequest(qs string) *nodeSetBuilder {
	q := resource.MustParse(qs)
	nsb.memoryRequest = &q
	return nsb
}

func (nsb *nodeSetBuilder) withStorageRequest(qs string) *nodeSetBuilder {
	q := resource.MustParse(qs)
	nsb.storageRequest = &q
	return nsb
}

func (nsb *nodeSetBuilder) build() esv1.NodeSet {
	nodeSet := esv1.NodeSet{
		Name:   nsb.name,
		Config: nil,
		Count:  0,
		PodTemplate: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: esv1.ElasticsearchContainerName,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{},
						},
					},
				},
			},
		},
	}

	// Set memory
	if nsb.memoryRequest != nil {
		nodeSet.PodTemplate.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = *nsb.memoryRequest
	}

	// Set storage
	if nsb.storageRequest != nil {
		storageRequest := corev1.ResourceList{}
		storageRequest[corev1.ResourceStorage] = *nsb.storageRequest
		nodeSet.VolumeClaimTemplates = append(nodeSet.VolumeClaimTemplates,
			corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.ResourceRequirements{
						Requests: nil,
					},
				},
			},
		)
	}
	return nodeSet
}

// - Allowed resource builder

type allowedResourcesBuilder struct {
	count  int32
	memory *resource.Quantity
}

func newAllowedResourcesBuilder() *allowedResourcesBuilder {
	return &allowedResourcesBuilder{}
}

func (arb *allowedResourcesBuilder) withCount(c int) *allowedResourcesBuilder {
	arb.count = int32(c)
	return arb
}

func (arb *allowedResourcesBuilder) withMemory(ms string) *allowedResourcesBuilder {
	m := resource.MustParse(ms)
	arb.memory = &m
	return arb
}

func (arb *allowedResourcesBuilder) build() commonv1.AllowedResources {
	return commonv1.AllowedResources{
		Count:  nil,
		Memory: nil,
	}
}

// - RequiredCapacity builder

type requiredCapacityBuilder struct {
	client.RequiredCapacity
}

func newRequiredCapacityBuilder() *requiredCapacityBuilder {
	return &requiredCapacityBuilder{}
}

func ptr(q resource.Quantity) *resource.Quantity {
	return &q
}

func (rcb *requiredCapacityBuilder) build() client.RequiredCapacity {
	return rcb.RequiredCapacity
}

func (rcb *requiredCapacityBuilder) nodeMemory(q resource.Quantity) *requiredCapacityBuilder {
	rcb.Node.Memory = ptr(q)
	return rcb
}

func (rcb *requiredCapacityBuilder) tierMemory(q resource.Quantity) *requiredCapacityBuilder {
	rcb.Tier.Memory = ptr(q)
	return rcb
}

func (rcb *requiredCapacityBuilder) nodeStorage(q resource.Quantity) *requiredCapacityBuilder {
	rcb.Node.Storage = ptr(q)
	return rcb
}

func (rcb *requiredCapacityBuilder) tierStorage(q resource.Quantity) *requiredCapacityBuilder {
	rcb.Tier.Storage = ptr(q)
	return rcb
}
