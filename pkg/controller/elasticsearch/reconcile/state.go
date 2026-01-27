// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package reconcile

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/hints"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

// State holds the accumulated state during the reconcile loop including the response and a copy of the
// Elasticsearch resource from the start of reconciliation, for status updates.
type State struct {
	*events.Recorder
	*StatusReporter
	cluster escommon.ElasticsearchCluster
	// prevHealth stores the health before reconciliation started for degradation detection
	prevHealth escommon.ElasticsearchHealth
	hints      hints.OrchestrationsHints
}

// NewState creates a new reconcile state based on the given cluster.
// The cluster parameter must be a pointer type that implements ElasticsearchCluster.
func NewState(cluster escommon.ElasticsearchCluster) (*State, error) {
	h, err := hints.NewFromAnnotations(cluster.GetAnnotations())
	if err != nil {
		return nil, err
	}

	// Store previous health for degradation detection
	prevHealth := cluster.GetStatusHealth()

	// Reset status fields for a fresh reconciliation
	cluster.SetStatusObservedGeneration(cluster.GetGeneration())
	// reset the health to 'unknown' so that if reconciliation fails before the observer has had a chance to get it,
	// we stop reporting a health that may be out of date
	cluster.SetStatusHealth(escommon.ElasticsearchUnknownHealth)
	// reset the phase to an empty string so that we do not report an outdated phase given that certain phases are
	// stickier than others (eg. invalid)
	cluster.SetStatusPhase("")

	return &State{
		Recorder: events.NewRecorder(),
		StatusReporter: &StatusReporter{
			DownscaleReporter: &DownscaleReporter{},
			UpscaleReporter:   &UpscaleReporter{},
			UpgradeReporter:   &UpgradeReporter{},
		},
		cluster:    cluster,
		prevHealth: prevHealth,
		hints:      h,
	}, nil
}

// MustNewState like NewState but panics on error. Use recommended only in test code.
func MustNewState(c esv1.Elasticsearch) *State {
	state, err := NewState(&c)
	if err != nil {
		panic(err)
	}
	return state
}

func (s *State) fetchMinRunningVersion(ctx context.Context, resourcesState ResourcesState) (*version.Version, error) {
	log := ulog.FromContext(ctx)
	minPodVersion, err := version.MinInPods(resourcesState.AllPods, label.VersionLabelName)
	if err != nil {
		log.Error(err, "failed to parse running Pods version", "namespace", s.cluster.GetNamespace(), "es_name", s.cluster.GetName())
		return nil, err
	}
	minSsetVersion, err := version.MinInStatefulSets(resourcesState.StatefulSets, label.VersionLabelName)
	if err != nil {
		log.Error(err, "failed to parse running Pods version", "namespace", s.cluster.GetNamespace(), "es_name", s.cluster.GetName())
		return nil, err
	}

	if minPodVersion == nil {
		return minSsetVersion, nil
	}
	if minSsetVersion == nil {
		return minPodVersion, nil
	}

	if minPodVersion.GT(*minSsetVersion) {
		return minSsetVersion, nil
	}

	return minPodVersion, nil
}

func (s *State) UpdateClusterHealth(clusterHealth escommon.ElasticsearchHealth) *State {
	if clusterHealth == "" {
		s.cluster.SetStatusHealth(escommon.ElasticsearchUnknownHealth)
		return s
	}
	s.cluster.SetStatusHealth(clusterHealth)
	return s
}

func (s *State) UpdateWithPhase(
	phase escommon.ElasticsearchOrchestrationPhase,
) *State {
	currentPhase := s.cluster.GetStatusPhase()
	switch {
	// do not overwrite the Invalid marker
	case currentPhase == escommon.ElasticsearchResourceInvalid:
		return s
	// do not overwrite non-ready phases like MigratingData
	case currentPhase != "" && phase == escommon.ElasticsearchApplyingChangesPhase:
		return s
	}
	s.cluster.SetStatusPhase(phase)
	return s
}

func (s *State) UpdateAvailableNodes(
	resourcesState ResourcesState,
) *State {
	s.cluster.SetStatusAvailableNodes(int32(len(AvailableElasticsearchNodes(resourcesState.CurrentPods))))
	return s
}

func (s *State) UpdateMinRunningVersion(
	ctx context.Context,
	resourcesState ResourcesState,
) *State {
	lowestVersion, err := s.fetchMinRunningVersion(ctx, resourcesState)
	// error already handled in fetchMinRunningVersion, move on with the status update
	if err == nil && lowestVersion != nil {
		s.cluster.SetStatusVersion(lowestVersion.String())
	}
	// Update the related condition.
	statusVersion := s.cluster.GetStatusVersion()
	if statusVersion == "" {
		s.ReportCondition(esv1.RunningDesiredVersion, corev1.ConditionUnknown, "No running version reported")
		return s
	}

	desiredVersion, err := version.Parse(s.cluster.GetVersion())
	if err != nil {
		s.ReportCondition(esv1.RunningDesiredVersion, corev1.ConditionUnknown, fmt.Sprintf("Error while parsing desired version: %s", err.Error()))
		return s
	}

	runningVersion, err := version.Parse(statusVersion)
	if err != nil {
		s.ReportCondition(esv1.RunningDesiredVersion, corev1.ConditionUnknown, fmt.Sprintf("Error while parsing running version: %s", err.Error()))
		return s
	}

	if desiredVersion.GT(runningVersion) {
		s.ReportCondition(
			esv1.RunningDesiredVersion,
			corev1.ConditionFalse,
			fmt.Sprintf("Upgrading from %s to %s", runningVersion.String(), desiredVersion.String()),
		)
		return s
	}
	s.ReportCondition(esv1.RunningDesiredVersion, corev1.ConditionTrue, fmt.Sprintf("All nodes are running version %s", runningVersion))

	return s
}

// UpdateElasticsearchInvalidWithEvent is a convenient method to set the phase to ElasticsearchResourceInvalid
// and generate an event at the same time.
func (s *State) UpdateElasticsearchInvalidWithEvent(msg string) {
	s.cluster.SetStatusPhase(escommon.ElasticsearchResourceInvalid)
	s.AddEvent(corev1.EventTypeWarning, events.EventReasonValidation, msg)
}

// Apply takes the current Elasticsearch status, checks for degradation, and returns the cluster for status update.
// It returns the events to emit and the cluster resource (which has been modified in-place during reconciliation).
func (s *State) Apply() ([]events.Event, escommon.ElasticsearchCluster) {
	// Check for degradation by comparing current health to previous health
	if s.cluster.StatusIsDegraded(s.prevHealth) {
		s.AddEvent(corev1.EventTypeWarning, events.EventReasonUnhealthy, "Elasticsearch cluster health degraded")
	}

	// For stateful Elasticsearch, merge conditions and operation status from the StatusReporter.
	// This is done via type assertion since stateless Elasticsearch has a different status structure.
	if es, ok := s.cluster.(*esv1.Elasticsearch); ok {
		es.Status = s.MergeStatusReportingWith(es.Status)
	}

	return s.Events(), s.cluster
}

// UpdateOrchestrationHints updates the orchestration hints collected so far with the hints in hint.
func (s *State) UpdateOrchestrationHints(hint hints.OrchestrationsHints) {
	s.hints = s.hints.Merge(hint)
}

// OrchestrationHints returns the current annotation hints as maintained in reconciliation state. Initially these will be
// populated from the Elasticsearch resource. But after calls to UpdateOrchestrationHints they can deviate from the state
// stored in the API server.
func (s *State) OrchestrationHints() hints.OrchestrationsHints {
	return s.hints
}
