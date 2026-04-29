// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	appsv1 "k8s.io/api/apps/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commondeployment "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
)

// statelessHasMasterRole returns a HasMasterRoleFunc that knows whether the
// given Deployment belongs to a stateless tier that carries the master role
// under the current spec. "Master-eligible" is a spec-level property: in a
// spec with a dedicated master tier, only the master tier carries it; without
// one, the index tier does. The function is bound to the ES resource because
// settings.TierHasMasterRole needs the whole spec to decide.
func statelessHasMasterRole(es esv1.Elasticsearch) deployment.HasMasterRoleFunc {
	return func(d appsv1.Deployment) bool {
		tier := esv1.StatelessTier(d.Labels[label.TierLabelName])
		return settings.TierHasMasterRole(es, tier)
	}
}

// masterTierState captures the raw facts the dedicated-master-tier logic
// needs, computed once from the spec + observed Deployments + what the
// reconciler is about to push. The rest of the driver consumes this state
// through the predicate methods below (adding, removing, removalFinishing,
// inFlight) rather than an enum: each predicate is a named boolean, so the
// call sites read as plain English.
//
// Facts captured:
//   - specHas: the spec declares a non-empty dedicated master tier.
//   - observedHas: at least one observed Deployment carries the master-tier
//     label (may or may not be rolled out).
//   - observedReady: at least one observed master-tier Deployment exists
//     AND every observed master-tier Deployment is rolled out.
//   - indexAtExpected: every expected index Deployment is observed at the
//     expected template hash AND rolled out; the signal the index tier has
//     acquired the master-role-inclusive template after a master removal.
type masterTierState struct {
	specHas         bool
	observedHas     bool
	observedReady   bool
	indexAtExpected bool
}

// newMasterTierState collects the four facts that describe the master
// tier's place in the dedicated-master lifecycle on this reconcile pass.
//
// expected carries the Deployments the reconciler is about to push; it is
// consulted for indexAtExpected, which is the completion signal of a
// removal (only once the index tier has rolled out with the new
// master-role-inclusive template is it safe to delete the outgoing master).
//
// During bootstrap (no observed Deployments) every fact defaults to the
// zero value, which makes all predicates return false — the initial pass
// pushes the final topology in one shot regardless of whether the spec
// declares a dedicated master tier.
func (d *Driver) newMasterTierState(observed, expected []appsv1.Deployment) masterTierState {
	if len(observed) == 0 {
		return masterTierState{}
	}
	return masterTierState{
		specHas:         specHasMasterTier(d.ES),
		observedHas:     observedHasMasterTier(observed),
		observedReady:   observedMasterTierReady(observed),
		indexAtExpected: indexTierAtExpected(observed, expected),
	}
}

// adding reports whether we are mid-way through introducing a dedicated
// master tier: the spec declares one but the observed master tier isn't
// ready yet. Plan holds the existing index/search Deployments untouched
// on this pass and rolls the new master tier first.
func (s masterTierState) adding() bool {
	return s.specHas && !s.observedReady
}

// removing reports whether we are mid-way through retiring a dedicated
// master tier: the spec no longer declares one but a master-tier
// Deployment is still observed AND the index tier has not yet rolled out
// with the new master-role-inclusive template. On this pass the index
// tier rolls first, the observed master is held back from GC, and voting
// exclusions are pushed so ES stops counting the outgoing masters in the
// quorum.
func (s masterTierState) removing() bool {
	return !s.specHas && s.observedHas && !s.indexAtExpected
}

// removalFinishing reports whether a master-tier removal just completed
// its Kubernetes-visible work: the index tier has acquired master
// eligibility, the master Deployment is still observed (it is about to
// be GC'd in this very reconcile pass), and it is time to clear voting
// exclusions so ES returns to a clean state once the masters are gone.
func (s masterTierState) removalFinishing() bool {
	return !s.specHas && s.observedHas && s.indexAtExpected
}

// inFlight reports whether any dedicated-master-tier work is in progress
// on this pass. Callers use it to force a requeue so the reconciler picks
// up the next step of the lifecycle without waiting on a timer.
func (s masterTierState) inFlight() bool {
	return s.adding() || s.removing() || s.removalFinishing()
}

// reason returns a short human-readable description of the active master-
// tier lifecycle step for logging. Empty string means no in-flight work.
func (s masterTierState) reason() string {
	switch {
	case s.adding():
		return "adding master tier"
	case s.removing():
		return "removing master tier"
	case s.removalFinishing():
		return "finishing master-tier removal"
	}
	return ""
}

// specHasMasterTier reports whether the spec declares a non-empty master tier
// NodeSet.
func specHasMasterTier(es esv1.Elasticsearch) bool {
	for _, ns := range es.Spec.NodeSets {
		tier, err := ns.ResolvedTier()
		if err != nil {
			continue
		}
		if tier == esv1.MasterTier && ns.Count > 0 {
			return true
		}
	}
	return false
}

// observedHasMasterTier reports whether any observed Deployment carries the
// master-tier label, regardless of its rollout status.
func observedHasMasterTier(observed []appsv1.Deployment) bool {
	for i := range observed {
		if observed[i].Labels[label.TierLabelName] == string(esv1.MasterTier) {
			return true
		}
	}
	return false
}

// observedMasterTierReady reports whether at least one observed master-tier
// Deployment exists AND every observed master-tier Deployment is rolled out.
// Used as the completion signal for an Adding transition: only when the new
// masters are fully up do we let the index tier shed its master role.
func observedMasterTierReady(observed []appsv1.Deployment) bool {
	found := false
	for i := range observed {
		d := &observed[i]
		if d.Labels[label.TierLabelName] != string(esv1.MasterTier) {
			continue
		}
		if !deployment.IsRolledOut(d) {
			return false
		}
		found = true
	}
	return found
}

// indexTierAtExpected reports whether every observed index-tier Deployment
// matches the currently-expected template hash AND is rolled out. Used as
// the completion signal for a Removing transition: only once the index tier
// has acquired master-eligibility with the new template is it safe to delete
// the outgoing master tier.
//
// When an expected index Deployment is missing from observed, or when any
// observed index Deployment doesn't yet match the expected hash, the index
// tier is considered "not yet at expected" and the transition stays active.
func indexTierAtExpected(observed []appsv1.Deployment, expected []appsv1.Deployment) bool {
	expectedIndex := map[string]string{}
	for i := range expected {
		if expected[i].Labels[label.TierLabelName] != string(esv1.IndexTier) {
			continue
		}
		hashed := commondeployment.WithTemplateHash(expected[i])
		expectedIndex[expected[i].Name] = hash.GetTemplateHashLabel(hashed.Labels)
	}
	if len(expectedIndex) == 0 {
		// No index tier in the spec — nothing to wait for (unlikely in
		// practice since stateless clusters always have an index tier).
		return true
	}

	observedMatched := 0
	for i := range observed {
		o := &observed[i]
		if o.Labels[label.TierLabelName] != string(esv1.IndexTier) {
			continue
		}
		expectedHash, ok := expectedIndex[o.Name]
		if !ok {
			// Observed index Deployment is not in the spec — it will be
			// GC'd. Don't gate on it (it can't go "to expected").
			continue
		}
		if hash.GetTemplateHashLabel(o.Labels) != expectedHash {
			return false
		}
		if !deployment.IsRolledOut(o) {
			return false
		}
		observedMatched++
	}
	// Every expected index Deployment must be observed and rolled-out.
	return observedMatched == len(expectedIndex)
}
