// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package elasticsearch

import (
	"reflect"

	corev1 "k8s.io/api/core/v1"

	commonv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1alpha1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/hints"
	esreconcile "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/reconcile"
)

// statelessState holds the accumulated state during the reconcile loop including the response and a copy of the
// ElasticsearchStateless resource from the start of reconciliation, for status updates.
type statelessState struct {
	*events.Recorder
	cluster          essv1alpha1.ElasticsearchStateless
	status           essv1alpha1.ElasticsearchStatelessStatus
	hints            hints.OrchestrationsHints
	esReconcileState *esreconcile.State
}

// newStatelessState creates a new reconcile state based on the given cluster
func newStatelessState(c essv1alpha1.ElasticsearchStateless) *statelessState {
	hs, _ := hints.NewFromAnnotations(c.Annotations)
	status := *c.Status.DeepCopy()
	status.ObservedGeneration = c.Generation
	// reset the health to 'unknown' so that if reconciliation fails before the observer has had a chance to get it,
	// we stop reporting a health that may be out of date
	status.Health = escommon.ElasticsearchUnknownHealth
	// reset the phase to an empty string so that we do not report an outdated phase given that certain phases are
	// stickier than others (eg. invalid)
	status.Phase = ""
	return &statelessState{
		Recorder:         events.NewRecorder(),
		cluster:          c,
		status:           status,
		hints:            hs,
		esReconcileState: esreconcile.NewStatelessState(),
	}
}

func (s *statelessState) UpdateClusterHealth(clusterHealth escommon.ElasticsearchHealth) *statelessState {
	if clusterHealth == "" {
		s.status.Health = escommon.ElasticsearchUnknownHealth
		return s
	}
	s.status.Health = clusterHealth
	return s
}

func (s *statelessState) UpdateWithPhase(phase escommon.ElasticsearchOrchestrationPhase) *statelessState {
	switch {
	// do not overwrite the Invalid marker
	case s.status.Phase == escommon.ElasticsearchResourceInvalid:
		return s
	// do not overwrite non-ready phases like MigratingData
	case s.status.Phase != "" && phase == escommon.ElasticsearchApplyingChangesPhase:
		return s
	}
	s.status.Phase = phase
	return s
}

// ReportCondition reports a condition on the ElasticsearchStateless status.
func (s *statelessState) ReportCondition(conditionType commonv1alpha1.ConditionType, status corev1.ConditionStatus, message string) {
	s.esReconcileState.ReportCondition(conditionType, status, message)
}

// Apply takes the current ElasticsearchStateless status, compares it to the previous status, and updates the status accordingly.
// It returns the events to emit and an updated version of the ElasticsearchStateless cluster resource with
// the current status applied to its status sub-resource.
func (s *statelessState) Apply() ([]events.Event, *essv1alpha1.ElasticsearchStateless) {
	previous := s.cluster.Status
	current := s.status

	if reflect.DeepEqual(previous, current) {
		return s.Events(), nil
	}
	if current.Health.Less(previous.Health) {
		s.AddEvent(corev1.EventTypeWarning, events.EventReasonUnhealthy, "Elasticsearch cluster health degraded")
	}
	s.cluster.Status = current
	return s.Events(), &s.cluster
}

// UpdateOrchestrationHints updates the orchestration hints collected so far with the hints in hint.
func (s *statelessState) UpdateOrchestrationHints(hint hints.OrchestrationsHints) {
	s.hints = s.hints.Merge(hint)
}

// OrchestrationHints returns the current annotation hints as maintained in reconciliation state.
func (s *statelessState) OrchestrationHints() hints.OrchestrationsHints {
	return s.hints
}
