// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func quantityPtr(quantity string) *resource.Quantity {
	q := resource.MustParse(quantity)
	return &q
}

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
		Count:  nsb.count,
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
				ObjectMeta: metav1.ObjectMeta{
					Name: volume.ElasticsearchDataVolumeName,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *nsb.storageRequest,
						},
					},
				},
			},
		)
	}
	return nodeSet
}

// - NodeSetResources builder

type resourcesBuilder struct {
	name           string
	count          int32
	memoryRequest  *resource.Quantity
	storageRequest *resource.Quantity
}

func newResourcesBuilder(name string, count int) *resourcesBuilder {
	return &resourcesBuilder{
		name:  name,
		count: int32(count),
	}
}

func (nsb *resourcesBuilder) withMemoryRequest(qs string) *resourcesBuilder {
	q := resource.MustParse(qs)
	nsb.memoryRequest = &q
	return nsb
}

func (nsb *resourcesBuilder) withStorageRequest(qs string) *resourcesBuilder {
	q := resource.MustParse(qs)
	nsb.storageRequest = &q
	return nsb
}

func (nsb *resourcesBuilder) build() NodeSetResources {
	nodeSetResources := NodeSetResources{
		Name: nsb.name,
		ResourcesSpecification: esv1.ResourcesSpecification{
			Count:   nsb.count,
			Memory:  nsb.memoryRequest,
			Storage: nsb.storageRequest,
		},
	}
	return nodeSetResources
}

// - Allowed resource builder

type allowedResourcesBuilder struct {
	count           int32
	memory, storage *resource.Quantity
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

func (arb *allowedResourcesBuilder) withStorage(sto string) *allowedResourcesBuilder {
	s := resource.MustParse(sto)
	arb.storage = &s
	return arb
}

func (arb *allowedResourcesBuilder) build() esv1.ResourcesSpecification {
	return esv1.ResourcesSpecification{
		Count:   arb.count,
		Memory:  arb.memory,
		Storage: arb.storage,
	}
}

// - RequiredCapacity builder

type requiredCapacityBuilder struct {
	client.RequiredCapacity
}

func newRequiredCapacityBuilder() *requiredCapacityBuilder {
	return &requiredCapacityBuilder{}
}

func ptr(q int64) *int64 {
	return &q
}

func (rcb *requiredCapacityBuilder) build() client.RequiredCapacity {
	return rcb.RequiredCapacity
}

func (rcb *requiredCapacityBuilder) nodeMemory(m string) *requiredCapacityBuilder {
	rcb.Node.Memory = ptr(value(m))
	return rcb
}

func (rcb *requiredCapacityBuilder) tierMemory(m string) *requiredCapacityBuilder {
	rcb.Tier.Memory = ptr(value(m))
	return rcb
}

func (rcb *requiredCapacityBuilder) nodeStorage(m string) *requiredCapacityBuilder {
	rcb.Node.Storage = ptr(value(m))
	return rcb
}

func (rcb *requiredCapacityBuilder) tierStorage(m string) *requiredCapacityBuilder {
	rcb.Tier.Storage = ptr(value(m))
	return rcb
}

func value(v string) int64 {
	q := resource.MustParse(v)
	return (&q).Value()
}

// - nodeSet comparison tool

func resourcesDiff(expected, actual esv1.NodeSet) []string {
	var diff []string
	if expected.Count != actual.Count {
		diff = append(diff, fmt.Sprintf("nodeSet: %s, expectedCount: %d, actualCount: %d", expected.Name, expected.Count, actual.Count))
	}
	// We support only one container
	expectedContainer := expected.PodTemplate.Spec.Containers[0]
	actualContainer := actual.PodTemplate.Spec.Containers[0]

	// Compare memory
	expectedMemory := expectedContainer.Resources.Requests.Memory()
	actualMemory := actualContainer.Resources.Requests.Memory()
	switch {
	case expectedMemory != nil && actualMemory != nil:
		if expectedMemory.Value() != actualMemory.Value() {
			diff = append(diff, fmt.Sprintf("nodeSet: %s, expectedMemory: %s, actualMemory: %s", expected.Name, expectedMemory, actualMemory))
		}
	case expectedMemory != nil && actualMemory == nil ||
		expectedMemory == nil && actualMemory != nil:
		diff = append(diff, fmt.Sprintf("nodeSet: %s, expectedMemory: %s, actualMemory: %s", expected.Name, expectedMemory, actualMemory))
	}

	return diff
}
