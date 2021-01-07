// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var logTest = logf.Log.WithName("autoscaling-test")

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

func (nsb *resourcesBuilder) withCpuRequest(qs string) *resourcesBuilder {
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
