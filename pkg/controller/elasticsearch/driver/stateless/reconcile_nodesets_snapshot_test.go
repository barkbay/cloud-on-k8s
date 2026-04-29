// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless/fixtures"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

// reconcileNodeSetsCase describes one end-to-end scenario exercised against
// Driver.reconcileNodeSets.
//
// The fields model three extension points, chosen to keep the happy path as
// a one-liner in the table while still letting richer scenarios (config
// rotation across two passes, explicit post-reconcile invariants) stay
// self-contained:
//
//   - build: required. Returns the Elasticsearch fixture and any objects
//     that should be pre-seeded in the fake client (stale Deployments,
//     existing observed Deployments, etc.).
//   - run:   optional. Override to take full control of the reconcile flow,
//     typically to perform multiple passes with mutations in between. When
//     nil, the runner performs a single reconcileNodeSets pass.
//   - check: optional. Additional explicit assertions, run after the
//     snapshot match, so failures surface a human-readable message before a
//     reader has to diff the snapshot.
type reconcileNodeSetsCase struct {
	name  string
	build func(t *testing.T) (esv1.Elasticsearch, []client.Object)
	run   func(t *testing.T, d *Driver) *reconciler.Results
	check func(t *testing.T, d *Driver, res *reconciler.Results)
}

// TestReconcileNodeSets exercises Driver.reconcileNodeSets end-to-end against
// a fake client and captures the resulting cluster state as JSON snapshots.
// Each row below focuses on a distinct stateless-driver behaviour; the
// snapshot is the primary artefact, the per-row check only asserts the
// headline invariant so failures read well before anyone opens the .snap.
//
// Every scenario uses a topology ECK's stateless webhook would accept — at
// least one index tier AND one search tier — since reconcileNodeSets is
// only called after validation has let the resource through.
//
// Snapshots live under __snapshots__/<scenario_name>.snap (one file per
// scenario). Regenerate with UPDATE_SNAPS=true when the shape of the
// expected resources changes on purpose.
func TestReconcileNodeSets(t *testing.T) {
	cases := []reconcileNodeSetsCase{
		{
			name: "initial_create_minimal_cluster",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				// Smallest valid stateless topology: one search + one index
				// NodeSet. An index-only cluster would be rejected by the
				// stateless validation webhook.
				return fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("search", 1, esv1.SearchTier),
					fixtures.NodeSet("index", 1, esv1.IndexTier),
				}), nil
			},
			check: func(t *testing.T, d *Driver, res *reconciler.Results) {
				t.Helper()
				require.False(t, res.HasError())
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-search")
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-index")
			},
		},
		{
			name: "initial_create_multi_tier",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				return fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("search", 2, esv1.SearchTier),
					fixtures.NodeSet("index", 2, esv1.IndexTier),
					fixtures.NodeSet("ml", 1, esv1.MLTier),
				}), nil
			},
			check: func(t *testing.T, d *Driver, res *reconciler.Results) {
				t.Helper()
				require.False(t, res.HasError())
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-search")
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-index")
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-ml")
			},
		},
		{
			name: "config_change_rotates_immutable_secret",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				// Two NodeSets so we can assert the search Secret stays put
				// when only the index-tier user config changes.
				return fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("search", 1, esv1.SearchTier),
					fixtures.NodeSetWithConfig("index", 1, esv1.IndexTier, map[string]any{
						"indices.recovery.max_bytes_per_sec": "42mb",
					}),
				}), nil
			},
			run: func(t *testing.T, d *Driver) *reconciler.Results {
				t.Helper()
				ctx := context.Background()
				// Pass 1: establish the baseline (two Deployments, two
				// content-addressed config Secrets).
				res := d.reconcileNodeSets(ctx, metadata.Metadata{})
				require.False(t, res.HasError(), "pass 1: %v", fixtures.AggregateReconcileErr(res))
				firstIndex := fixtures.ConfigSecretRef(t, d.Client, d.ES.Namespace, "test-es-es-index")
				firstSearch := fixtures.ConfigSecretRef(t, d.Client, d.ES.Namespace, "test-es-es-search")

				// Mutate the index tier user config: the driver must produce
				// a fresh content-addressed Secret for the index Deployment
				// and leave the search Deployment's Secret untouched.
				d.ES.Spec.NodeSets[1] = fixtures.NodeSetWithConfig("index", 1, esv1.IndexTier, map[string]any{
					"indices.recovery.max_bytes_per_sec": "84mb",
				})
				res = d.reconcileNodeSets(ctx, metadata.Metadata{})
				require.False(t, res.HasError(), "pass 2: %v", fixtures.AggregateReconcileErr(res))

				secondIndex := fixtures.ConfigSecretRef(t, d.Client, d.ES.Namespace, "test-es-es-index")
				secondSearch := fixtures.ConfigSecretRef(t, d.Client, d.ES.Namespace, "test-es-es-search")
				assert.NotEqual(t, firstIndex, secondIndex,
					"index tier config Secret must rotate when its user config changes")
				assert.Equal(t, firstSearch, secondSearch,
					"search tier config Secret must not rotate when only index config changes")
				return res
			},
		},
		{
			name: "garbage_collects_stale_deployment",
			build: func(t *testing.T) (esv1.Elasticsearch, []client.Object) {
				t.Helper()
				es := fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("search", 1, esv1.SearchTier),
					fixtures.NodeSet("index", 1, esv1.IndexTier),
				})
				// Stale Deployment: carries the cluster-name label and the
				// deployment-name marker label the GC selector looks for,
				// but does not correspond to any NodeSet in the spec.
				stale := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-es-es-stale",
						Namespace: es.Namespace,
						Labels: map[string]string{
							label.ClusterNameLabelName:    es.Name,
							label.DeploymentNameLabelName: "test-es-es-stale",
						},
					},
					Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)},
				}
				return es, []client.Object{stale}
			},
			check: func(t *testing.T, d *Driver, _ *reconciler.Results) {
				t.Helper()
				fixtures.RequireDeploymentAbsent(t, d.Client, d.ES.Namespace, "test-es-es-stale")
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-search")
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-index")
			},
		},
		{
			name: "upgrade_pushes_search_first",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				// Pending version upgrade: observed tiers carry the old
				// image, the spec carries the new one. The Plan must
				// push the search tier first (readers before writers),
				// leave the index tier untouched, and requeue.
				es := fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("search", 1, esv1.SearchTier),
					fixtures.NodeSet("index", 1, esv1.IndexTier),
				}, fixtures.WithBootstrapped("fake-uuid"))
				return es, []client.Object{
					fixtures.SeededDeployment(es, "search", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
					fixtures.SeededDeployment(es, "index", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				}
			},
			check: func(t *testing.T, d *Driver, res *reconciler.Results) {
				t.Helper()
				assert.True(t, res.HasRequeue(), "expected a requeue while search tier rolls out")
				_, reason := res.IsReconciled()
				assert.Contains(t, reason, "search",
					"requeue reason should mention the search tier, got %q", reason)
				assert.Equal(t, fixtures.NewESImage, fixtures.DeploymentImage(t, d.Client, d.ES.Namespace, "test-es-es-search"),
					"search tier should be pushed to the new image on this pass")
				assert.Equal(t, fixtures.OldESImage, fixtures.DeploymentImage(t, d.Client, d.ES.Namespace, "test-es-es-index"),
					"index tier should stay at the old image until search has rolled out")
			},
		},
		{
			name: "adds_master_tier_pushes_master_first",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				// Starting from a bootstrapped cluster with [index, search],
				// the user introduces a dedicated master tier. First pass
				// should create the master Deployment; it is not yet rolled
				// out so the driver requeues before touching group 2.
				es := fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("master", 3, esv1.MasterTier),
					fixtures.NodeSet("index", 2, esv1.IndexTier),
					fixtures.NodeSet("search", 2, esv1.SearchTier),
				}, fixtures.WithBootstrapped("fake-uuid"))
				return es, []client.Object{
					fixtures.SeededDeployment(es, "index", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
					fixtures.SeededDeployment(es, "search", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				}
			},
			check: func(t *testing.T, d *Driver, res *reconciler.Results) {
				t.Helper()
				assert.True(t, res.HasRequeue(), "expected a requeue while master tier rolls out")
				_, reason := res.IsReconciled()
				assert.Contains(t, reason, "master",
					"requeue reason should mention the master tier group, got %q", reason)
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-master")
				// During Adding, the index tier is held (its observed
				// Deployment is in Plan.Keep, never in Plan.Apply), so
				// its live Deployment template is untouched and its
				// image is still the pre-existing one.
				assert.Equal(t, fixtures.OldESImage, fixtures.DeploymentImage(t, d.Client, d.ES.Namespace, "test-es-es-index"),
					"index tier image should stay unchanged on the first pass of an Adding transition")
			},
		},
		{
			name: "removes_master_tier_keeps_master_until_index_ready",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				// Starting from a bootstrapped cluster with a dedicated
				// master tier, the user removes the master NodeSet. The
				// spec-aware config rebuilds the index tier with the
				// master role re-included; the observed master Deployment
				// must NOT be GC'd on this pass.
				es := fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("index", 2, esv1.IndexTier),
					fixtures.NodeSet("search", 2, esv1.SearchTier),
				}, fixtures.WithBootstrapped("fake-uuid"))
				// Pre-existing masters tier (not in the current spec).
				masterD := fixtures.SeededDeployment(es, "master", esv1.MasterTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1))
				return es, []client.Object{
					masterD,
					fixtures.SeededDeployment(es, "index", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
					fixtures.SeededDeployment(es, "search", esv1.SearchTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				}
			},
			check: func(t *testing.T, d *Driver, res *reconciler.Results) {
				t.Helper()
				assert.True(t, res.HasRequeue(), "expected a requeue while index tier re-acquires master role")
				// Master Deployment is preserved during the Removing transition
				// until the index tier is rolled out with the master role
				// re-included.
				fixtures.RequireDeploymentExists(t, d.Client, d.ES.Namespace, "test-es-es-master")
			},
		},
		{
			name: "upgrade_gates_on_search_rollout",
			build: func(_ *testing.T) (esv1.Elasticsearch, []client.Object) {
				es := fixtures.NewStatelessES("test-es", []esv1.NodeSet{
					fixtures.NodeSet("search", 1, esv1.SearchTier),
					fixtures.NodeSet("index", 1, esv1.IndexTier),
				}, fixtures.WithBootstrapped("fake-uuid"))
				return es, []client.Object{
					// search mid-rollout: grouping kicks in, search group
					// gets reconciled, isGroupRolledOut returns false, and
					// the driver breaks out before touching the index group.
					fixtures.SeededDeployment(es, "search", esv1.SearchTier, fixtures.OldESImage, fixtures.InProgressDeploymentRolloutStatus(1)),
					fixtures.SeededDeployment(es, "index", esv1.IndexTier, fixtures.OldESImage, fixtures.RolledOutDeploymentStatus(1)),
				}
			},
			check: func(t *testing.T, d *Driver, res *reconciler.Results) {
				t.Helper()
				assert.True(t, res.HasRequeue(), "expected a requeue while search rolls out")
				reconciled, reason := res.IsReconciled()
				assert.False(t, reconciled)
				assert.Contains(t, reason, "search",
					"requeue reason should mention the gating tier, got %q", reason)
				assert.Equal(t, fixtures.NewESImage, fixtures.DeploymentImage(t, d.Client, d.ES.Namespace, "test-es-es-search"),
					"search tier must be updated to the new image")
				assert.Equal(t, fixtures.OldESImage, fixtures.DeploymentImage(t, d.Client, d.ES.Namespace, "test-es-es-index"),
					"index tier must stay on the old image until search rolls out")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			es, seed := tc.build(t)
			d := newDriver(t, es, seed...)

			run := tc.run
			if run == nil {
				run = func(t *testing.T, d *Driver) *reconciler.Results {
					t.Helper()
					res := d.reconcileNodeSets(context.Background(), metadata.Metadata{})
					require.NoError(t, fixtures.AggregateReconcileErr(res))
					return res
				}
			}
			res := run(t, d)

			// One .snap file per scenario (not per-test-file) keeps diff
			// reviews tractable: a failure in one row touches one small
			// file instead of a 2kloc shared snapshot.
			snaps.WithConfig(snaps.Filename(tc.name)).
				MatchJSON(t, fixtures.ClusterSnapshot(t, d.Client, d.ES, res))

			if tc.check != nil {
				tc.check(t, d, res)
			}
		})
	}
}
