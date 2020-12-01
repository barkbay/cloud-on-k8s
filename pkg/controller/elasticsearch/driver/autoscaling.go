// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling"
)

func resourcesAutoscaled(es esv1.Elasticsearch) (bool, error) {
	if !es.IsAutoscalingDefined() {
		return true, nil
	}
	autoscalingStatus, err := autoscaling.GetAutoscalingStatus(es)
	if err != nil {
		return false, err
	}
	statusByNodeSet := autoscalingStatus.ByNodeSet()
	for _, nodeSet := range es.Spec.NodeSets {
		status, ok := statusByNodeSet[nodeSet.Name]
		if !ok {
			return false, nil
		}

		nodeSetHash := autoscaling.ResourcesHash(nodeSet)
		if nodeSet.Name == "ml-zone-b" {
			fmt.Printf("driver: %v", nodeSet.PodTemplate.Spec.Containers)
		}
		if status.Hash != nodeSetHash {
			return false, nil
		}
	}

	return true, nil
}
