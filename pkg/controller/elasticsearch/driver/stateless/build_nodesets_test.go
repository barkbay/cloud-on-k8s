// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless/fixtures"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// TestBuildNodeSetResources exercises buildNodeSetResources end-to-end for
// the full matrix of stateless driver behaviours: input validation, tier
// defaults, user-config override semantics (both scalar and list-valued),
// Deployment shape, and zone awareness.
//
// All cases target nodeSet index 0 on the fixture; rows that need to
// observe a different NodeSet should be shaped around that in buildES.
func TestBuildNodeSetResources(t *testing.T) {
	cases := []buildNodeSetCase{
		// ----- error cases -----
		{
			name: "missing_objectStore_is_rejected",
			buildES: func() esv1.Elasticsearch {
				es := fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("index-a", 1, esv1.IndexTier)})
				es.Spec.ObjectStore = nil
				return es
			},
			wantErrContains: "objectStore is required",
		},
		{
			name: "unresolvable_tier_is_rejected",
			buildES: func() esv1.Elasticsearch {
				// Name prefix is not a known tier AND NodeSet.Tier is unset.
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{{Name: "weird-name", Count: 1}})
			},
			wantErrContains: "cannot infer stateless tier",
		},

		// ----- tier defaults -----
		{
			name: "tier_defaults/index",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("index-a", 1, esv1.IndexTier)})
			},
			assert: assertTierAndRoles(esv1.IndexTier, []string{"master", "index", "ingest", "remote_cluster_client"}),
		},
		{
			name: "tier_defaults/search",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("search-a", 1, esv1.SearchTier)})
			},
			assert: assertTierAndRoles(esv1.SearchTier, []string{"search", "remote_cluster_client", "transform"}),
		},
		{
			name: "tier_defaults/master",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("master-a", 1, esv1.MasterTier)})
			},
			assert: assertTierAndRoles(esv1.MasterTier, []string{"master", "remote_cluster_client"}),
		},
		{
			name: "tier_defaults/ml",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("ml-a", 1, esv1.MLTier)})
			},
			assert: assertTierAndRoles(esv1.MLTier, []string{"ml", "remote_cluster_client"}),
		},

		// ----- user-config override semantics -----
		{
			// A user-supplied scalar setting (http.publish_host) must
			// override the stateless default which pins publish_host to "0".
			name: "scalar_user_config_wins",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{
					fixtures.NodeSetWithConfig("index-a", 1, esv1.IndexTier, map[string]any{
						esv1.HTTPPublishHost: "1.2.3.4",
					}),
				})
			},
			assert: func(t *testing.T, res nodeSetResources) {
				t.Helper()
				raw, err := res.config.Render()
				require.NoError(t, err)
				assert.Contains(t, string(raw), "publish_host: 1.2.3.4",
					"user-supplied scalar settings must override stateless defaults")
				assert.NotContains(t, string(raw), `publish_host: "0"`,
					"stateless default for publish_host must NOT survive when user overrides it")
			},
		},
		{
			// Pins the "user wins for list-valued stateless defaults"
			// contract implemented by applyUserConfigOverrides. Without
			// that helper, ucfg.AppendValues would produce the UNION of
			// tier-default roles and the user's list, which contradicts
			// both the "user overrides on top" comment at the merge call
			// site and the admission warning emitted by
			// statelessNodeRolesWarning.
			//
			// If this row starts reporting the union again, the most
			// likely suspect is that applyUserConfigOverrides has been
			// bypassed or node.roles is no longer listed in
			// statelessListValuedDefaults.
			name: "user_node_roles_override_tier_default",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{
					fixtures.NodeSetWithConfig("index-a", 1, esv1.IndexTier, map[string]any{
						esv1.NodeRoles: []any{"master"},
					}),
				})
			},
			assert: func(t *testing.T, res nodeSetResources) {
				t.Helper()
				assert.Equal(t, []string{"master"}, renderedNodeRoles(t, res),
					"user-supplied node.roles must fully replace the stateless tier defaults")
			},
		},

		// ----- Deployment shape -----
		{
			name: "deployment_shape",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("index-a", 3, esv1.IndexTier)})
			},
			assert: func(t *testing.T, res nodeSetResources) {
				t.Helper()
				d := res.deployment
				assert.Equal(t, "ns", d.Namespace)
				assert.Equal(t, esv1.Deployment("my-es", "index-a"), d.Name)

				// Labels: cluster/deployment/tier triple must be present.
				assert.Equal(t, "my-es", d.Labels[label.ClusterNameLabelName])
				assert.Equal(t, d.Name, d.Labels[label.DeploymentNameLabelName])
				assert.Equal(t, string(esv1.IndexTier), d.Labels[label.TierLabelName])

				// Replicas come from the NodeSet.Count.
				require.NotNil(t, d.Spec.Replicas)
				assert.Equal(t, int32(3), *d.Spec.Replicas)

				// Selector binds cluster + deployment. Tier is NOT part of
				// the selector on purpose: changing a NodeSet's tier must
				// not orphan pods of the existing Deployment.
				require.NotNil(t, d.Spec.Selector)
				assert.Equal(t, map[string]string{
					label.ClusterNameLabelName:    "my-es",
					label.DeploymentNameLabelName: d.Name,
				}, d.Spec.Selector.MatchLabels)

				// RollingUpdate: zero downtime (MaxUnavailable=0), cautious
				// burst (MaxSurge=25%).
				require.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, d.Spec.Strategy.Type)
				require.NotNil(t, d.Spec.Strategy.RollingUpdate)
				assert.Equal(t, ptr.To(intstr.FromInt32(0)), d.Spec.Strategy.RollingUpdate.MaxUnavailable)
				assert.Equal(t, ptr.To(intstr.FromString("25%")), d.Spec.Strategy.RollingUpdate.MaxSurge)

				// RevisionHistoryLimit defaults to 0 for ephemeral pods.
				require.NotNil(t, d.Spec.RevisionHistoryLimit)
				assert.EqualValues(t, 0, *d.Spec.RevisionHistoryLimit)

				// The pod template advertises the ES container with HTTPS
				// + transport ports.
				es1 := fixtures.FindContainer(d.Spec.Template.Spec, esv1.ElasticsearchContainerName)
				require.NotNil(t, es1, "expected %s container", esv1.ElasticsearchContainerName)
				gotPorts := map[string]int32{}
				for _, p := range es1.Ports {
					gotPorts[p.Name] = p.ContainerPort
				}
				assert.Contains(t, gotPorts, "https")
				assert.Contains(t, gotPorts, "transport")
			},
		},

		// ----- zone awareness -----
		{
			name: "zone_awareness/absent_by_default",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES("my-es", []esv1.NodeSet{fixtures.NodeSet("index-a", 1, esv1.IndexTier)})
			},
			assert: func(t *testing.T, res nodeSetResources) {
				t.Helper()
				assert.Empty(t, res.deployment.Spec.Template.Spec.TopologySpreadConstraints)
				c := fixtures.FindContainer(res.deployment.Spec.Template.Spec, esv1.ElasticsearchContainerName)
				assert.False(t, fixtures.HasEnv(c, "ZONE"),
					"ZONE env var must only be injected when zone awareness is enabled")
			},
		},
		{
			name: "zone_awareness/propagates_to_topology_spread_and_env_var",
			buildES: func() esv1.Elasticsearch {
				return fixtures.NewStatelessES(
					"my-es",
					[]esv1.NodeSet{fixtures.NodeSet("index-a", 1, esv1.IndexTier)},
					fixtures.WithZoneAwareness(),
				)
			},
			assert: func(t *testing.T, res nodeSetResources) {
				t.Helper()
				constraints := res.deployment.Spec.Template.Spec.TopologySpreadConstraints
				require.NotEmpty(t, constraints,
					"zone awareness must emit at least one topology spread constraint")

				foundZoneKey := false
				for _, c := range constraints {
					if c.TopologyKey == esv1.DefaultZoneAwarenessTopologyKey {
						foundZoneKey = true
						break
					}
				}
				assert.True(t, foundZoneKey, "expected a topology spread constraint on %s",
					esv1.DefaultZoneAwarenessTopologyKey)

				c := fixtures.FindContainer(res.deployment.Spec.Template.Spec, esv1.ElasticsearchContainerName)
				assert.True(t, fixtures.HasEnv(c, "ZONE"),
					"ZONE env var must be injected when cluster has zone awareness")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			es := tc.buildES()
			res, err := buildNodeSetResources(
				context.Background(),
				k8s.NewFakeClient(),
				es,
				es.Spec.NodeSets[0],
				fixtures.DefaultTestVersion,
				corev1.IPv4Protocol,
				true, // setDefaultSecurityContext
				nodespec.PolicyConfig{},
				metadata.Metadata{},
				"", // actualPodsRestartTriggerAnnotationValue
			)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			require.NoError(t, err)
			if tc.assert != nil {
				tc.assert(t, res)
			}
		})
	}
}

// buildNodeSetCase describes one scenario exercised against
// buildNodeSetResources. Rows either:
//
//   - expect an error from the builder (wantErrContains is non-empty); or
//   - expect a successful build and carry an assert func that checks the
//     returned nodeSetResources.
//
// buildES is called lazily inside the subtest so rows can mutate the
// Elasticsearch fixture (e.g. clearing spec.objectStore) without sharing
// state with their neighbours.
type buildNodeSetCase struct {
	name            string
	buildES         func() esv1.Elasticsearch
	wantErrContains string
	assert          func(t *testing.T, res nodeSetResources)
}

// assertTierAndRoles returns an assert func that checks both the resolved
// tier and the rendered node.roles list against the given expectations.
// Used by the tier-default rows so each row stays a one-liner.
func assertTierAndRoles(tier esv1.StatelessTier, wantRoles []string) func(t *testing.T, res nodeSetResources) {
	return func(t *testing.T, res nodeSetResources) {
		t.Helper()
		assert.Equal(t, tier, res.tier)
		assert.Equal(t, wantRoles, renderedNodeRoles(t, res))
	}
}

func TestRevisionHistoryLimit(t *testing.T) {
	t.Run("defaults to 0", func(t *testing.T) {
		got := revisionHistoryLimit(fixtures.NewStatelessES("my-es",
			[]esv1.NodeSet{fixtures.NodeSet("index-a", 1, esv1.IndexTier)}))
		require.NotNil(t, got)
		assert.EqualValues(t, 0, *got)
	})

	t.Run("user-provided value wins", func(t *testing.T) {
		es := fixtures.NewStatelessES("my-es",
			[]esv1.NodeSet{fixtures.NodeSet("index-a", 1, esv1.IndexTier)},
			fixtures.WithRevisionHistoryLimit(ptr.To[int32](5)))
		got := revisionHistoryLimit(es)
		require.NotNil(t, got)
		assert.EqualValues(t, 5, *got)
	})
}
