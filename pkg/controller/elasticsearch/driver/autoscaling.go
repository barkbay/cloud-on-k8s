// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
)

// resourcesAutoscaled checks that the autoscaler controller has updated the resources
// if autoscaling is enabled. This is to avoid situations where resources have been manually
// deleted or replaced by an external event.
func resourcesAutoscaled(es esv1.Elasticsearch) (bool, error) {
	if !es.IsAutoscalingDefined() {
		return true, nil
	}
	autoscalingSpec, err := es.GetAutoscalingSpecification()
	if err != nil {
		return false, err
	}
	autoscalingStatus, err := status.GetStatus(es)
	if err != nil {
		return false, err
	}

	for _, nodeSet := range es.Spec.NodeSets {
		nodeSetAutoscalingSpec, err := autoscalingSpec.GetAutoscalingSpecFor(nodeSet)
		if err != nil {
			return false, err
		}
		if nodeSetAutoscalingSpec == nil {
			// This nodeSet is not managed by an autoscaling configuration
			log.Info("NodeSet not managed by an autoscaling controller", "nodeset", nodeSet.Name)
			continue
		}

		s, ok := autoscalingStatus.GetNamedTierResources(nodeSetAutoscalingSpec.Name)
		if !ok {
			log.Info("NodeSet managed by the autoscaling controller but not found in status",
				"nodeset", nodeSet.Name,
			)
			return false, nil
		}
		if s.IsUsedBy(nodeSet) {
			log.Info("NodeSet managed by the autoscaling controller but not in sync",
				"nodeset", nodeSet.Name,
				"expected", s.ResourcesSpecification,
			)
			return false, nil
		}
	}

	return true, nil
}
