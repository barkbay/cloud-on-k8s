// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"encoding/json"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/hash"
)

const ElasticsearchAutoscalingStatusAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-policies"

type NodeSetResourcesWithHash struct {
	Hash string `json:"hash,omitempty"`
	NodeSetResources
}

type Status struct {
	NodeSetResources []NodeSetResourcesWithHash `json:"nodeSets,omitempty"`
}

func (s Status) ByNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources)
	for _, nodeSetResources := range s.NodeSetResources {
		nodeSetResources := nodeSetResources.NodeSetResources
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
		status.NodeSetResources = append(status.NodeSetResources,
			NodeSetResourcesWithHash{
				Hash:             hash.HashObject(nodeSet.DeepCopy()),
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
