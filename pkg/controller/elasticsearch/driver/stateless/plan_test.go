// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless/fixtures"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
)

// TestPlan exercises the decision tree in Driver.plan on its own, without
// the rest of the reconcile loop. Each case builds a spec-derived target
// from the ES spec and a matching observed-state fixture, then asserts
// the Plan's Apply/Keep/Transitioning/Reason shape.
//
// Scenarios covered (one per lifecycle branch in plan.go):
//
//   - bootstrap: empty observed, target pushed in full
//   - steady:    observed matches target, target pushed (upserts no-op)
//   - upgrade:   pending version bump, search-first, index held
//   - adding:    new master tier, master-first, index/search held
//   - removing:  master retired, index-first, master held from GC
//   - finishing: master retired AND index at target, master GC'd
func TestPlan(t *testing.T) {
	type plannedNames struct {
		apply []string
		keep  []string
	}

	// buildTarget mirrors the minimal shape reconcileNodeSets uses:
	// one nodeSetResources per NodeSet, with a Deployment carrying the
	// canonical labels produced by the fixtures. We only need the shape
	// Plan examines (name, tier, deployment labels).
	buildTarget := func(es esv1.Elasticsearch, image string) []nodeSetResources {
		out := make([]nodeSetResources, 0, len(es.Spec.NodeSets))
		for _, ns := range es.Spec.NodeSets {
			tier, _ := ns.ResolvedTier()
			d := *fixtures.SeededDeployment(es, ns.Name, tier, image, fixtures.RolledOutDeploymentStatus(1))
			out = append(out, nodeSetResources{
				nodeSetName: ns.Name,
				tier:        tier,
				deployment:  d,
			})
		}
		return out
	}

	// rolledObserved returns the Deployments that the API server would
	// report after a reconcile has applied each resource (template hash
	// labels stamped in, status rolled out).
	rolledObserved := func(target []nodeSetResources) []appsv1.Deployment {
		return observedAsIfReconciled(deploymentsOf(target))
	}

	esIS := fixtures.NewStatelessES("es", nsIndexSearch, fixtures.WithBootstrapped("fake-uuid"))
	esMIS := fixtures.NewStatelessES("es", nsMasterIndexSearch, fixtures.WithBootstrapped("fake-uuid"))

	tests := []struct {
		name             string
		es               esv1.Elasticsearch
		observed         []appsv1.Deployment
		target           []nodeSetResources
		wantNames        plannedNames
		wantTransitioning bool
	}{
		{
			name:              "bootstrap: no observed, push full target",
			es:                esIS,
			observed:          nil,
			target:            buildTarget(esIS, fixtures.OldESImage),
			wantNames:         plannedNames{apply: []string{"es-es-index-a", "es-es-search-a"}},
			wantTransitioning: false,
		},
		{
			name:              "steady: observed matches target, push full target",
			es:                esIS,
			observed:          rolledObserved(buildTarget(esIS, fixtures.OldESImage)),
			target:            buildTarget(esIS, fixtures.OldESImage),
			wantNames:         plannedNames{apply: []string{"es-es-index-a", "es-es-search-a"}},
			wantTransitioning: false,
		},
		{
			name: "upgrade pending: apply search, keep index",
			es:   esIS,
			// Observed carries old image, target carries new → upgrade
			// pending; plan must stage search first.
			observed:          rolledObserved(buildTarget(esIS, fixtures.OldESImage)),
			target:            buildTarget(esIS, fixtures.NewESImage),
			wantNames:         plannedNames{apply: []string{"es-es-search-a"}, keep: []string{"es-es-index-a"}},
			wantTransitioning: true,
		},
		{
			name: "adding master tier: apply master, keep index+search",
			es:   esMIS,
			// Observed has only index+search (old world); target
			// introduces the master tier.
			observed:          rolledObserved(buildTarget(esIS, fixtures.OldESImage)),
			target:            buildTarget(esMIS, fixtures.OldESImage),
			wantNames:         plannedNames{apply: []string{"es-es-master-a"}, keep: []string{"es-es-index-a", "es-es-search-a"}},
			wantTransitioning: true,
		},
		{
			name: "removing master tier: apply index, keep master+search",
			es:   esIS,
			// Observed still has a master tier at the old image; the
			// spec has removed it AND bumped the image. Plan must roll
			// the index tier first (it re-gains the master role under
			// the new target) and hold the outgoing master Deployment
			// back from GC. Different images guarantee the observed
			// and target index hashes differ, so indexAtExpected is
			// false and removing() fires.
			observed:          rolledObserved(buildTarget(esMIS, fixtures.OldESImage)),
			target:            buildTarget(esIS, fixtures.NewESImage),
			wantNames:         plannedNames{apply: []string{"es-es-index-a"}, keep: []string{"es-es-master-a", "es-es-search-a"}},
			wantTransitioning: true,
		},
		{
			name: "finishing master removal: apply full target, master GC'd",
			es:   esIS,
			// Observed has master AND an index tier already at target
			// (hash matches target and rolled out) → Plan emits the
			// full target; the outgoing master is not in target, so GC
			// will delete it.
			observed: append(
				// Existing master Deployment (to be GC'd).
				[]appsv1.Deployment{*fixtures.SeededDeployment(esMIS, "master-a", esv1.MasterTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1))},
				// Index + search observed at target hash (rolled out).
				rolledObserved(buildTarget(esIS, fixtures.OldESImage))...,
			),
			target:            buildTarget(esIS, fixtures.OldESImage),
			wantNames:         plannedNames{apply: []string{"es-es-index-a", "es-es-search-a"}},
			wantTransitioning: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := newDriver(t, tc.es)
			p := d.plan(context.Background(), tc.observed, tc.target)

			applyNames := make([]string, 0, len(p.Apply))
			for i := range p.Apply {
				applyNames = append(applyNames, p.Apply[i].deployment.Name)
			}
			sort.Strings(applyNames)
			wantApply := append([]string(nil), tc.wantNames.apply...)
			sort.Strings(wantApply)
			assert.Equal(t, wantApply, applyNames, "Apply names")

			keep := append([]string(nil), p.Keep...)
			sort.Strings(keep)
			wantKeep := append([]string(nil), tc.wantNames.keep...)
			sort.Strings(wantKeep)
			assert.Equal(t, wantKeep, keep, "Keep names")

			assert.Equal(t, tc.wantTransitioning, p.Transitioning, "Transitioning")
			if tc.wantTransitioning {
				assert.NotEmpty(t, p.Reason, "Transitioning plans must have a non-empty Reason")
			}
		})
	}
}

// TestPlan_SteadyStateReasonEmpty spot-checks that a steady-state plan
// has an empty Reason, so the reconcile loop's "if Reason != ''" log
// gate doesn't fire on the hot path.
func TestPlan_SteadyStateReasonEmpty(t *testing.T) {
	es := fixtures.NewStatelessES("es", nsIndexSearch)
	d := newDriver(t, es)
	// Use the real build path so we also cover the wiring to
	// buildAllNodeSetResources + settings.NewStatelessConfig. We do
	// not need a serving client; the fake inside newDriver is enough.
	_ = settings.HasDedicatedMasterTier(es) // touch to keep import used even if future refactors drop it
	target := []nodeSetResources{
		{nodeSetName: "index-a", tier: esv1.IndexTier, deployment: *fixtures.SeededDeployment(es, "index-a", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1))},
		{nodeSetName: "search-a", tier: esv1.SearchTier, deployment: *fixtures.SeededDeployment(es, "search-a", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1))},
	}
	observed := observedAsIfReconciled(deploymentsOf(target))
	p := d.plan(context.Background(), observed, target)

	assert.False(t, p.Transitioning)
	assert.Empty(t, p.Reason)
	assert.Empty(t, p.Keep)
}

func TestObservedDeployments_FiltersToCluster(t *testing.T) {
	ctx := context.Background()
	esA := fixtures.NewStatelessES("es-a", nil)
	esB := fixtures.NewStatelessES("es-b", nil)

	// Deployment that belongs to cluster "es-a" and carries the deployment
	// marker label observedDeployments selects on.
	mine := fixtures.SeededDeployment(esA, "index", esv1.IndexTier, "img:1", fixtures.RolledOutDeploymentStatus(1))
	// Decoy Deployment belonging to a different cluster in the same namespace.
	other := fixtures.SeededDeployment(esB, "index", esv1.IndexTier, "img:1", fixtures.RolledOutDeploymentStatus(1))
	// Decoy: carries the cluster-name label but is missing the deployment
	// marker label, so observedDeployments must skip it.
	noDepLabel := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "no-label", Namespace: esA.Namespace,
			Labels: map[string]string{label.ClusterNameLabelName: esA.Name},
		},
	}

	d := newDriver(t, esA, mine, other, noDepLabel)

	got, err := d.observedDeployments(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, mine.Name, got[0].Name)
}
