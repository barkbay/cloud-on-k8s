// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling"
)

// resourcesAutoscaled checks that the autoscaler controller has updated the resources
// if autoscaling is enabled. This is to avoid situations where resources have been manually
// deleted or replaced by an external event.
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
		if status.Hash != nodeSetHash {
			return false, nil
		}
	}

	return true, nil
}
