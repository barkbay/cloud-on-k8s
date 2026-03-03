// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/stretchr/testify/require"
)

func TestExpectedTierResources(t *testing.T) {
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

	expected := expectedTierResources(buildTierResources)
	require.Len(t, expected, 3)
	require.Same(t, buildTierResources[esv1.IndexTierName], expected[0])
	require.Same(t, buildTierResources[esv1.SearchTierName], expected[1])
	require.Same(t, buildTierResources[esv1.MLTierName], expected[2])
}

func TestGroupExpectedTierResources(t *testing.T) {
	search := &TierResources{deployment: &appsv1.Deployment{ObjectMeta: newTierMeta(esv1.SearchTierName)}}
	index := &TierResources{deployment: &appsv1.Deployment{ObjectMeta: newTierMeta(esv1.IndexTierName)}}
	ml := &TierResources{deployment: &appsv1.Deployment{ObjectMeta: newTierMeta(esv1.MLTierName)}}

	grouped := groupExpectedTierResources([]*TierResources{index, search, ml})
	require.Len(t, grouped, 2)
	require.Equal(t, []*TierResources{search}, grouped[0])
	require.Equal(t, []*TierResources{index, ml}, grouped[1])
}

func newTierMeta(tier esv1.ElasticsearchTierName) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Labels: map[string]string{
			label.TierLabelName: string(tier),
		},
	}
}
