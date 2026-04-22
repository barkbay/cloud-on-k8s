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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless/fixtures"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

func TestStatelessHasMasterRole(t *testing.T) {
	tests := []struct {
		tier string
		want bool
	}{
		{tier: "index", want: true},
		{tier: "master", want: true},
		{tier: "search", want: false},
		{tier: "ml", want: false},
		{tier: "", want: false},
		{tier: "unknown", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.tier, func(t *testing.T) {
			d := appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{label.TierLabelName: tt.tier}},
			}
			assert.Equal(t, tt.want, statelessHasMasterRole(d))
		})
	}
}

// TestNodeSetTierWithGroupByTier covers the wiring between the stateless
// tierOf accessor and the shared deployment.GroupByTier helper. The grouping
// rules themselves are exhaustively covered in the shared package's tests.
func TestNodeSetTierWithGroupByTier(t *testing.T) {
	resources := []nodeSetResources{
		{nodeSetName: "index-0", tier: esv1.IndexTier},
		{nodeSetName: "search-0", tier: esv1.SearchTier},
		{nodeSetName: "ml-0", tier: esv1.MLTier},
		{nodeSetName: "search-1", tier: esv1.SearchTier},
		{nodeSetName: "master-0", tier: esv1.MasterTier},
	}

	groups := deployment.GroupByTier(resources, nodeSetTier)
	require.Len(t, groups, 2)

	group0Names := make([]string, 0)
	for _, r := range groups[0] {
		group0Names = append(group0Names, r.nodeSetName)
	}
	assert.ElementsMatch(t, []string{"search-0", "search-1"}, group0Names)

	group1Names := make([]string, 0)
	for _, r := range groups[1] {
		group1Names = append(group1Names, r.nodeSetName)
	}
	assert.ElementsMatch(t, []string{"index-0", "ml-0", "master-0"}, group1Names)
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
