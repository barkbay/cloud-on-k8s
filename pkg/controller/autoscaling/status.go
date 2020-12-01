// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"encoding/json"
	"fmt"
	"hash/adler32"
	"sort"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
)

const ElasticsearchAutoscalingStatusAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-status"

type NodeSetResourcesWithHash struct {
	Hash string `json:"hash,omitempty"`
	NodeSetResources
}

type Status struct {
	NodeSetResources []NodeSetResourcesWithHash `json:"nodeSets,omitempty"`
}

func (s Status) ByNodeSet() map[string]NodeSetResourcesWithHash {
	byNodeSet := make(map[string]NodeSetResourcesWithHash)
	for _, nodeSetResources := range s.NodeSetResources {
		nodeSetResources := nodeSetResources
		byNodeSet[nodeSetResources.Name] = nodeSetResources
	}
	return byNodeSet
}

func GetAutoscalingStatus(es esv1.Elasticsearch) (Status, error) {
	status := Status{}
	if es.Annotations == nil {
		return status, nil
	}
	serializedStatus, ok := es.Annotations[ElasticsearchAutoscalingStatusAnnotationName]
	if !ok {
		return status, nil
	}
	err := json.Unmarshal([]byte(serializedStatus), &status)
	return status, err
}

func UpdateAutoscalingStatus(es *esv1.Elasticsearch, nodeSetsResources NodeSetsResources) error {
	status := Status{}
	byNodeSetsResources := nodeSetsResources.byNodeSet()
	for _, nodeSet := range es.Spec.NodeSets {
		nodeSetResource, ok := byNodeSetsResources[nodeSet.Name]
		if !ok {
			// nodeSet not managed by the autoscaler
			continue
		}
		if nodeSet.Name == "ml-zone-b" {
			fmt.Printf("ac: %v", nodeSet.PodTemplate.Spec.Containers)
		}
		status.NodeSetResources = append(status.NodeSetResources,
			NodeSetResourcesWithHash{
				Hash:             ResourcesHash(nodeSet),
				NodeSetResources: nodeSetResource,
			})

	}
	if es.Annotations == nil {
		es.Annotations = make(map[string]string)
	}
	serializedStatus, err := json.Marshal(&status)
	if err != nil {
		return err
	}
	es.Annotations[ElasticsearchAutoscalingStatusAnnotationName] = string(serializedStatus)
	return nil
}

// ResourcesHash computes a reliable hash of the resources spec in a nodeSet.
// The lack of reliability may come from the fact that Quantity.String() has a side effect on the
// struct, and hence any hash computation must ensure that Quantity.String() is called in a consistent
// when the hash is computed.
func ResourcesHash(nodeSet esv1.NodeSet) string {
	configChecksum := adler32.New()
	_, _ = configChecksum.Write([]byte{byte(nodeSet.Count)})
	for _, container := range nodeSet.PodTemplate.Spec.Containers {
		if container.Name != esv1.ElasticsearchContainerName {
			continue
		}
		for _, resourceName := range orderedResourceNames(container.Resources.Requests) {
			_, _ = configChecksum.Write([]byte(resourceName))
			resourceQuantity := container.Resources.Requests[resourceName]
			_, _ = configChecksum.Write([]byte(resourceQuantity.String()))
		}
		for _, resourceName := range orderedResourceNames(container.Resources.Limits) {
			_, _ = configChecksum.Write([]byte(resourceName))
			resourceQuantity := container.Resources.Requests[resourceName]
			_, _ = configChecksum.Write([]byte(resourceQuantity.String()))
		}
	}
	for _, volumeClaim := range orderVolumeClaimTemplates(nodeSet.VolumeClaimTemplates) {
		_, _ = configChecksum.Write([]byte(volumeClaim.Name))
		for _, storageResourceName := range orderedResourceNames(volumeClaim.Spec.Resources.Requests) {
			_, _ = configChecksum.Write([]byte(storageResourceName))
			resourceQuantity := volumeClaim.Spec.Resources.Requests[storageResourceName]
			_, _ = configChecksum.Write([]byte(resourceQuantity.String()))
		}

	}
	return fmt.Sprintf("%x", configChecksum.Sum(nil))
}

func orderVolumeClaimTemplates(pvc []corev1.PersistentVolumeClaim) []corev1.PersistentVolumeClaim {
	sort.Slice(pvc, func(i, j int) bool {
		return pvc[i].String() < pvc[j].String()
	})
	return pvc
}

func orderedResourceNames(m v1.ResourceList) []v1.ResourceName {
	keys := make([]v1.ResourceName, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	return keys
}
