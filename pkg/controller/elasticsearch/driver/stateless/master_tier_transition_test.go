// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commondeployment "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless/fixtures"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

// TestStatelessHasMasterRole verifies the spec-aware master-eligibility
// predicate: the index tier carries the master role unless the spec declares
// a dedicated master tier, in which case the master tier owns it exclusively.
func TestStatelessHasMasterRole(t *testing.T) {
	indexOnly := fixtures.NewStatelessES("es",
		[]esv1.NodeSet{
			{Name: "index-a", Count: 3, Tier: esv1.IndexTier},
			{Name: "search-a", Count: 2, Tier: esv1.SearchTier},
		},
	)
	withMasters := fixtures.NewStatelessES("es",
		[]esv1.NodeSet{
			{Name: "master-a", Count: 3, Tier: esv1.MasterTier},
			{Name: "index-a", Count: 3, Tier: esv1.IndexTier},
			{Name: "search-a", Count: 2, Tier: esv1.SearchTier},
		},
	)

	tests := []struct {
		name string
		es   esv1.Elasticsearch
		tier string
		want bool
	}{
		{name: "index-only: index is master-eligible", es: indexOnly, tier: "index", want: true},
		{name: "index-only: search is not", es: indexOnly, tier: "search", want: false},
		{name: "with masters: index is NOT master-eligible", es: withMasters, tier: "index", want: false},
		{name: "with masters: master tier is", es: withMasters, tier: "master", want: true},
		{name: "with masters: search still isn't", es: withMasters, tier: "search", want: false},
		{name: "unknown tier is not master-eligible", es: indexOnly, tier: "unknown", want: false},
		{name: "empty tier is not master-eligible", es: indexOnly, tier: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{label.TierLabelName: tt.tier}},
			}
			assert.Equal(t, tt.want, statelessHasMasterRole(tt.es)(d))
		})
	}
}

// nodeSets describes the NodeSets on a test cluster in a compact way.
type nodeSets = []esv1.NodeSet

var (
	nsIndexSearch = nodeSets{
		{Name: "index-a", Count: 3, Tier: esv1.IndexTier},
		{Name: "search-a", Count: 2, Tier: esv1.SearchTier},
	}
	nsMasterIndexSearch = nodeSets{
		{Name: "master-a", Count: 3, Tier: esv1.MasterTier},
		{Name: "index-a", Count: 3, Tier: esv1.IndexTier},
		{Name: "search-a", Count: 2, Tier: esv1.SearchTier},
	}
)

// observedAsIfReconciled simulates the state of observed Deployments after
// commondeployment.Reconcile has run: each Deployment's template hash label
// is stamped using WithTemplateHash. This is what the API server would
// return on a subsequent reconcile pass.
func observedAsIfReconciled(ds []appsv1.Deployment) []appsv1.Deployment {
	out := make([]appsv1.Deployment, len(ds))
	for i := range ds {
		out[i] = commondeployment.WithTemplateHash(ds[i])
	}
	return out
}

// predicates bundles the four master-tier predicate expectations for a
// given test case. Only one should be true at a time; "inFlight" is the
// logical OR of the first three and is asserted for free.
type predicates struct {
	adding           bool
	removing         bool
	removalFinishing bool
}

func TestNewMasterTierState(t *testing.T) {
	esIndexSearch := fixtures.NewStatelessES("es", nsIndexSearch)
	esMasterAdded := fixtures.NewStatelessES("es", nsMasterIndexSearch)

	// expectedFor returns what buildAllNodeSetResources would produce for
	// each spec, as *raw* Deployments (no template-hash yet, matching how
	// reconcile_nodesets.go calls the state builder). SeededDeployment
	// hard-codes Spec.Replicas=1 so status.Replicas must also be 1 for
	// IsRolledOut to return true.
	expectedFor := func(es esv1.Elasticsearch) []appsv1.Deployment {
		out := []appsv1.Deployment{}
		for _, ns := range es.Spec.NodeSets {
			tier, _ := ns.ResolvedTier()
			out = append(out, *fixtures.SeededDeployment(es, ns.Name, tier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)))
		}
		return out
	}

	tests := []struct {
		name     string
		es       esv1.Elasticsearch
		observed []appsv1.Deployment
		expected []appsv1.Deployment
		want     predicates
	}{
		{
			name:     "bootstrap: no observed Deployments, all predicates false",
			es:       esMasterAdded,
			observed: nil,
			expected: expectedFor(esMasterAdded),
			want:     predicates{},
		},
		{
			name: "steady-state index-only: all predicates false",
			es:   esIndexSearch,
			observed: []appsv1.Deployment{
				*fixtures.SeededDeployment(esIndexSearch, "index-a", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(3)),
				*fixtures.SeededDeployment(esIndexSearch, "search-a", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(2)),
			},
			expected: expectedFor(esIndexSearch),
			want:     predicates{},
		},
		{
			name: "adding master: spec has master, no master observed",
			es:   esMasterAdded,
			observed: []appsv1.Deployment{
				*fixtures.SeededDeployment(esMasterAdded, "index-a", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(3)),
				*fixtures.SeededDeployment(esMasterAdded, "search-a", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(2)),
			},
			expected: expectedFor(esMasterAdded),
			want:     predicates{adding: true},
		},
		{
			name: "adding master: master deployment exists but not rolled out yet",
			es:   esMasterAdded,
			observed: []appsv1.Deployment{
				*fixtures.SeededDeployment(esMasterAdded, "master-a", esv1.MasterTier, fixtures.NewESImage, fixtures.InProgressDeploymentRolloutStatus(1)),
				*fixtures.SeededDeployment(esMasterAdded, "index-a", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				*fixtures.SeededDeployment(esMasterAdded, "search-a", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
			},
			expected: expectedFor(esMasterAdded),
			want:     predicates{adding: true},
		},
		{
			name: "adding master: master deployment is rolled out -> all predicates false",
			es:   esMasterAdded,
			observed: []appsv1.Deployment{
				*fixtures.SeededDeployment(esMasterAdded, "master-a", esv1.MasterTier, fixtures.NewESImage, fixtures.RolledOutDeploymentStatus(1)),
				*fixtures.SeededDeployment(esMasterAdded, "index-a", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				*fixtures.SeededDeployment(esMasterAdded, "search-a", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
			},
			expected: expectedFor(esMasterAdded),
			want:     predicates{},
		},
		{
			name: "removing master: spec has no master, master observed, index not yet at expected",
			es:   esIndexSearch,
			// "index" observed template has no hash label, so the
			// template-hash check in indexTierAtExpected will fail and
			// removing() stays true until the index tier is rolled out
			// with the new (master-inclusive) template.
			observed: []appsv1.Deployment{
				*fixtures.SeededDeployment(esIndexSearch, "master-a", esv1.MasterTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				*fixtures.SeededDeployment(esIndexSearch, "index-a", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				*fixtures.SeededDeployment(esIndexSearch, "search-a", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
			},
			expected: expectedFor(esIndexSearch),
			want:     predicates{removing: true},
		},
		{
			name: "removing master: index already at expected AND rolled out -> removalFinishing",
			es:   esIndexSearch,
			// After pass 1 of removal, commondeployment.Reconcile has
			// stamped the template hash on the index/search Deployments
			// and the rollout has completed. The master Deployment is
			// still observed (GC-protected). removalFinishing() is now
			// true, which lets the voting gate clear exclusions before
			// the master is GC'd.
			observed: append(
				observedAsIfReconciled(expectedFor(esIndexSearch)),
				*fixtures.SeededDeployment(esIndexSearch, "master-a", esv1.MasterTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
			),
			expected: expectedFor(esIndexSearch),
			want:     predicates{removalFinishing: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDriver(t, tt.es)
			got := d.newMasterTierState(tt.observed, tt.expected)
			assert.Equal(t, tt.want.adding, got.adding(), "adding()")
			assert.Equal(t, tt.want.removing, got.removing(), "removing()")
			assert.Equal(t, tt.want.removalFinishing, got.removalFinishing(), "removalFinishing()")
			wantInFlight := tt.want.adding || tt.want.removing || tt.want.removalFinishing
			assert.Equal(t, wantInFlight, got.inFlight(), "inFlight()")
			if wantInFlight {
				assert.NotEmpty(t, got.reason(), "reason() must be non-empty when in flight")
			} else {
				assert.Empty(t, got.reason(), "reason() must be empty in steady state")
			}
		})
	}
}
