// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1alpha1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
)

// ElasticsearchHealth is the health of the cluster as returned by the health API.
type ElasticsearchHealth = escommon.ElasticsearchHealth

// Possible traffic light states Elasticsearch health can have.
const (
	ElasticsearchRedHealth     = escommon.ElasticsearchRedHealth
	ElasticsearchYellowHealth  = escommon.ElasticsearchYellowHealth
	ElasticsearchGreenHealth   = escommon.ElasticsearchGreenHealth
	ElasticsearchUnknownHealth = escommon.ElasticsearchUnknownHealth
)

// ElasticsearchOrchestrationPhase is the phase Elasticsearch is in from the controller point of view.
type ElasticsearchOrchestrationPhase = escommon.ElasticsearchOrchestrationPhase

const (
	// ElasticsearchReadyPhase is operating at the desired spec.
	ElasticsearchReadyPhase = escommon.ElasticsearchReadyPhase
	// ElasticsearchApplyingChangesPhase controller is working towards a desired state, cluster can be unavailable.
	ElasticsearchApplyingChangesPhase = escommon.ElasticsearchApplyingChangesPhase
	// ElasticsearchResourceInvalid is marking a resource as invalid.
	ElasticsearchResourceInvalid = escommon.ElasticsearchResourceInvalid
)

// ConditionType aliases v1alpha1.ConditionType for convenience.
type ConditionType = v1alpha1.ConditionType

// Condition types for ElasticsearchStateless.
const (
	ElasticsearchIsReachable ConditionType = "ElasticsearchIsReachable"
	ReconciliationComplete   ConditionType = "ReconciliationComplete"
	RunningDesiredVersion    ConditionType = "RunningDesiredVersion"
)

// IsDegraded returns true if the current status is worse than the previous.
func (s ElasticsearchStatelessStatus) IsDegraded(prev ElasticsearchStatelessStatus) bool {
	return s.Health.Less(prev.Health)
}
