// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	corev1 "k8s.io/api/core/v1"
)

func applyScaleDecision(
	nodeSets []v1.NodeSet,
	container string,
	decision client.Decision,
	policy commonv1.ScalePolicy,
) ([]v1.NodeSet, error) {
	updatedNodeSets := make([]v1.NodeSet, len(nodeSets))
	for i, nodeSet := range nodeSets {
		updatedNodeSets[i] = *nodeSet.DeepCopy()
	}

	// 1. Scale vertically
	scaleVertically(updatedNodeSets, container, decision.RequiredCapacity.Node, policy)
	return updatedNodeSets, nil
}

// scaleVertically vertically scales nodeSet to match the per node requirements
func scaleVertically(nodeSets []v1.NodeSet, containerName string, requestedCapacity client.Capacity, policy commonv1.ScalePolicy) {
	memory := requestedCapacity.Memory
	if requestedCapacity.Memory.Cmp(*policy.MinAllowed.Memory) == -1 {
		// The amount of memory requested by Elasticsearch is less than the min. allowed value
		memory = policy.MinAllowed.Memory
	}
	if requestedCapacity.Memory.Cmp(*policy.MaxAllowed.Memory) == 1 {
		// The amount of memory requested by Elasticsearch is more than the max. allowed value
		memory = policy.MaxAllowed.Memory
	}
	for i := range nodeSets {
		container, containers := getContainer(containerName, nodeSets[i].PodTemplate.Spec.Containers)
		if container == nil {
			container = &corev1.Container{
				Name: containerName,
			}
		}
		container.Resources.Requests = corev1.ResourceList{
			corev1.ResourceMemory: *memory,
		}
		nodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)
	}
}

func scaleHorizontally(nodeSet []v1.NodeSet, policy commonv1.ScalePolicy) {

}

func getContainer(name string, containers []corev1.Container) (*corev1.Container, []corev1.Container) {
	for i, container := range containers {
		if container.Name == name {
			// Remove the container
			return &container, append(containers[:i], containers[i+1:]...)
		}
	}
	return nil, containers
}
