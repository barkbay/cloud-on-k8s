// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/go-logr/logr"
)

// Context contains the required objects used by the autoscaler functions.
type Context struct {
	log logr.Logger
	// autoscalingSpec is the autoscaling specification as provided by the user.
	autoscalingSpec esv1.AutoscalingPolicySpec
	// nodeSets is the list of the NodeSets managed by the autoscaling specification.
	nodeSets esv1.NodeSetList
	// actualAutoscalingStatus is the current resources status as stored in the Elasticsearch resource.
	actualAutoscalingStatus status.Status
	// requiredCapacity contains the Elasticsearch Autoscaling API result.
	requiredCapacity client.PolicyCapacityInfo
	// statusBuilder is used to track any event that should be surfaced to the user.
	statusBuilder *status.AutoscalingStatusBuilder
}
