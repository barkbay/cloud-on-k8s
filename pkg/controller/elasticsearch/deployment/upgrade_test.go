// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package deployment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
)

const testTierLabel = "example.io/tier"

func depWithImage(name, image string) appsv1.Deployment {
	return appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "sidecar", Image: "other:1"},
						{Name: esv1.ElasticsearchContainerName, Image: image},
					},
				},
			},
		},
	}
}

func depWithReplicasAndAvailable(replicas int32, available int32) appsv1.Deployment {
	return appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{Replicas: ptr.To(replicas)},
		Status: appsv1.DeploymentStatus{
			AvailableReplicas: available,
		},
	}
}

func TestImagesInUse(t *testing.T) {
	assert.Empty(t, ImagesInUse(nil).UnsortedList())

	images := ImagesInUse([]appsv1.Deployment{
		depWithImage("a", "es:8.14.0"),
		depWithImage("b", "es:8.14.0"),
		depWithImage("c", "es:8.15.0"),
	})
	assert.ElementsMatch(t, []string{"es:8.14.0", "es:8.15.0"}, images.UnsortedList())

	// Deployment without an elasticsearch container contributes the empty string.
	noES := appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "sidecar", Image: "x:1"}},
				},
			},
		},
	}
	assert.True(t, ImagesInUse([]appsv1.Deployment{noES}).Has(""))
}

func TestIsVersionUpgradeInProgress(t *testing.T) {
	assert.False(t, IsVersionUpgradeInProgress(nil))
	assert.False(t, IsVersionUpgradeInProgress([]appsv1.Deployment{
		depWithImage("a", "es:8.14.0"),
		depWithImage("b", "es:8.14.0"),
	}))
	assert.True(t, IsVersionUpgradeInProgress([]appsv1.Deployment{
		depWithImage("a", "es:8.14.0"),
		depWithImage("b", "es:8.15.0"),
	}))
}

func TestIsVersionUpgradePending(t *testing.T) {
	observed := []appsv1.Deployment{
		depWithImage("a", "es:8.14.0"),
		depWithImage("b", "es:8.14.0"),
	}
	sameExpected := []appsv1.Deployment{
		depWithImage("a", "es:8.14.0"),
		depWithImage("b", "es:8.14.0"),
	}
	newVersionExpected := []appsv1.Deployment{
		depWithImage("a", "es:8.15.0"),
		depWithImage("b", "es:8.15.0"),
	}

	// No observed deployments => no upgrade pending (initial bootstrap).
	assert.False(t, IsVersionUpgradePending(newVersionExpected, nil))
	assert.False(t, IsVersionUpgradePending(sameExpected, observed))
	assert.True(t, IsVersionUpgradePending(newVersionExpected, observed))
}

// tieredDep returns a Deployment fixture labelled with the given tier and
// serving the given ES container image, with the specified desired and
// available replicas.
func tieredDep(tier, image string, replicas, available int32) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{testTierLabel: tier},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: esv1.ElasticsearchContainerName, Image: image},
					},
				},
			},
		},
		Status: appsv1.DeploymentStatus{AvailableReplicas: available},
	}
}

// testHasMasterRole is a HasMasterRoleFunc that treats tiers "master" and
// "index" as carrying the master role. It exercises the exported
// ShouldGroupDeploymentReconciliation contract without coupling the test to
// any specific caller's tier scheme.
func testHasMasterRole(d appsv1.Deployment) bool {
	switch d.Labels[testTierLabel] {
	case "master", "index":
		return true
	}
	return false
}

func TestShouldGroupDeploymentReconciliation(t *testing.T) {
	ctx := context.Background()

	oldImage := "es:8.14.0"
	newImage := "es:8.15.0"

	healthy := []appsv1.Deployment{
		tieredDep("index", oldImage, 2, 2),
		tieredDep("search", oldImage, 2, 2),
	}
	newImages := []appsv1.Deployment{
		tieredDep("index", newImage, 2, 0),
		tieredDep("search", newImage, 2, 0),
	}

	tests := []struct {
		name          string
		observed      []appsv1.Deployment
		expected      []appsv1.Deployment
		bootstrapped  bool
		hasMasterRole HasMasterRoleFunc
		want          bool
	}{
		{
			name:          "not bootstrapped => do not group",
			observed:      healthy,
			expected:      newImages,
			bootstrapped:  false,
			hasMasterRole: testHasMasterRole,
			want:          false,
		},
		{
			name: "all deployments unavailable => do not group",
			observed: []appsv1.Deployment{
				tieredDep("index", oldImage, 2, 0),
				tieredDep("search", oldImage, 2, 0),
			},
			expected:      newImages,
			bootstrapped:  true,
			hasMasterRole: testHasMasterRole,
			want:          false,
		},
		{
			name: "all masters unavailable => do not group",
			observed: []appsv1.Deployment{
				tieredDep("index", oldImage, 2, 0),
				tieredDep("master", oldImage, 3, 0),
				tieredDep("search", oldImage, 2, 2),
			},
			expected:      []appsv1.Deployment{tieredDep("index", newImage, 2, 0)},
			bootstrapped:  true,
			hasMasterRole: testHasMasterRole,
			want:          false,
		},
		{
			name: "no deployment carries master role => treated as all masters unavailable",
			observed: []appsv1.Deployment{
				tieredDep("search", oldImage, 2, 2),
			},
			expected:      []appsv1.Deployment{tieredDep("search", newImage, 2, 0)},
			bootstrapped:  true,
			hasMasterRole: testHasMasterRole,
			want:          false,
		},
		{
			name:          "no version change, cluster healthy => do not group",
			observed:      healthy,
			expected:      []appsv1.Deployment{tieredDep("index", oldImage, 2, 0), tieredDep("search", oldImage, 2, 0)},
			bootstrapped:  true,
			hasMasterRole: testHasMasterRole,
			want:          false,
		},
		{
			name:          "version upgrade pending => group",
			observed:      healthy,
			expected:      newImages,
			bootstrapped:  true,
			hasMasterRole: testHasMasterRole,
			want:          true,
		},
		{
			name: "version upgrade in progress => group",
			observed: []appsv1.Deployment{
				tieredDep("index", oldImage, 2, 2),
				tieredDep("search", newImage, 2, 2),
			},
			expected:      newImages,
			bootstrapped:  true,
			hasMasterRole: testHasMasterRole,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldGroupDeploymentReconciliation(
				ctx, tt.observed, tt.expected, tt.bootstrapped, tt.hasMasterRole,
			)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAllDeploymentsUnavailable(t *testing.T) {
	assert.True(t, AllDeploymentsUnavailable(nil))
	assert.True(t, AllDeploymentsUnavailable([]appsv1.Deployment{
		depWithReplicasAndAvailable(3, 0),
		depWithReplicasAndAvailable(2, 0),
	}))
	// zero replicas counts as unavailable
	assert.True(t, AllDeploymentsUnavailable([]appsv1.Deployment{
		depWithReplicasAndAvailable(0, 0),
	}))
	// nil replicas counts as unavailable
	assert.True(t, AllDeploymentsUnavailable([]appsv1.Deployment{{}}))
	// one available replica => not all unavailable
	assert.False(t, AllDeploymentsUnavailable([]appsv1.Deployment{
		depWithReplicasAndAvailable(3, 0),
		depWithReplicasAndAvailable(2, 1),
	}))
}

func TestGroupByTier(t *testing.T) {
	type tierResource struct {
		name string
		tier string
	}
	tierOf := func(r tierResource) string { return r.tier }

	tests := []struct {
		name       string
		in         []tierResource
		wantGroup0 []string
		wantGroup1 []string
	}{
		{
			name:       "empty input => two empty groups",
			in:         nil,
			wantGroup0: []string{},
			wantGroup1: []string{},
		},
		{
			name: "only search => group 1 empty",
			in: []tierResource{
				{name: "a", tier: "search"},
				{name: "b", tier: "search"},
			},
			wantGroup0: []string{"a", "b"},
			wantGroup1: []string{},
		},
		{
			name: "only non-search => group 0 empty",
			in: []tierResource{
				{name: "a", tier: "index"},
				{name: "b", tier: "ml"},
				{name: "c", tier: "master"},
			},
			wantGroup0: []string{},
			wantGroup1: []string{"a", "b", "c"},
		},
		{
			name: "mixed tiers preserve input order within each group",
			in: []tierResource{
				{name: "index-0", tier: "index"},
				{name: "search-0", tier: "search"},
				{name: "ml-0", tier: "ml"},
				{name: "search-1", tier: "search"},
				{name: "master-0", tier: "master"},
			},
			wantGroup0: []string{"search-0", "search-1"},
			wantGroup1: []string{"index-0", "ml-0", "master-0"},
		},
		{
			name: "empty tier is classified as non-search",
			in: []tierResource{
				{name: "a", tier: ""},
				{name: "b", tier: "search"},
			},
			wantGroup0: []string{"b"},
			wantGroup1: []string{"a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GroupByTier(tc.in, tierOf)
			assert.Len(t, got, 2)

			names := func(group []tierResource) []string {
				out := make([]string, 0, len(group))
				for _, r := range group {
					out = append(out, r.name)
				}
				return out
			}
			assert.Equal(t, tc.wantGroup0, names(got[0]), "group 0 (search)")
			assert.Equal(t, tc.wantGroup1, names(got[1]), "group 1 (others)")
		})
	}
}
