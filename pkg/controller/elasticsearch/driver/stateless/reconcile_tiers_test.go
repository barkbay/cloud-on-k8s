// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/stretchr/testify/require"
)

func TestExpectedDeployments(t *testing.T) {
	buildTierResources := map[esv1.ElasticsearchTierName]*TierResources{
		esv1.MLTierName: {
			deployment: &appsv1.Deployment{},
		},
		esv1.IndexTierName: {
			deployment: &appsv1.Deployment{},
		},
		esv1.SearchTierName: {
			deployment: &appsv1.Deployment{},
		},
	}

	expected := expectedDeployments(buildTierResources)
	require.Len(t, expected, 3)
	require.Same(t, buildTierResources[esv1.IndexTierName].deployment, expected[0])
	require.Same(t, buildTierResources[esv1.SearchTierName].deployment, expected[1])
	require.Same(t, buildTierResources[esv1.MLTierName].deployment, expected[2])
}

func TestGroupedTierReconciliationOrder(t *testing.T) {
	require.Equal(
		t,
		[]esv1.ElasticsearchTierName{esv1.SearchTierName, esv1.IndexTierName, esv1.MLTierName},
		groupedTierReconciliationOrder(),
	)
}
