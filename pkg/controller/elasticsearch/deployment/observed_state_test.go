// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package deployment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

func deploymentWithStatus(name string, desired int32, status appsv1.DeploymentStatus, generation int64) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "ns",
			Generation: generation,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(desired),
		},
		Status: status,
	}
}

func TestIsRolledOut(t *testing.T) {
	tests := []struct {
		name string
		d    *appsv1.Deployment
		want bool
	}{
		{
			name: "nil deployment",
			d:    nil,
			want: false,
		},
		{
			name: "nil replicas",
			d: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{Replicas: nil},
			},
			want: false,
		},
		{
			name: "fewer updated replicas than desired",
			d: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)},
				Status: appsv1.DeploymentStatus{
					UpdatedReplicas: 2, Replicas: 3, AvailableReplicas: 3, ObservedGeneration: 1,
				},
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
			},
			want: false,
		},
		{
			name: "fewer available replicas than desired",
			d: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)},
				Status: appsv1.DeploymentStatus{
					UpdatedReplicas: 3, Replicas: 3, AvailableReplicas: 2, ObservedGeneration: 1,
				},
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
			},
			want: false,
		},
		{
			name: "total replicas differ from desired (terminating pods)",
			d: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)},
				Status: appsv1.DeploymentStatus{
					UpdatedReplicas: 3, Replicas: 4, AvailableReplicas: 3, ObservedGeneration: 1,
				},
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
			},
			want: false,
		},
		{
			name: "stale observed generation",
			d: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)},
				Status: appsv1.DeploymentStatus{
					UpdatedReplicas: 3, Replicas: 3, AvailableReplicas: 3, ObservedGeneration: 1,
				},
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
			},
			want: false,
		},
		{
			name: "happy path",
			d: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)},
				Status: appsv1.DeploymentStatus{
					UpdatedReplicas: 3, Replicas: 3, AvailableReplicas: 3, ObservedGeneration: 2,
				},
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsRolledOut(tt.d))
		})
	}
}

func TestGC_DeletesUnexpectedDeployments(t *testing.T) {
	ctx := context.Background()

	keep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "ns", Labels: map[string]string{"cluster": "a"}},
	}
	stale := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "stale", Namespace: "ns", Labels: map[string]string{"cluster": "a"}},
	}
	otherCluster := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "ns", Labels: map[string]string{"cluster": "b"}},
	}

	c := k8s.NewFakeClient(keep, stale, otherCluster)

	require.NoError(t, GC(ctx, c, "ns", sets.New("keep"), client.MatchingLabels{"cluster": "a"}))

	var got appsv1.Deployment
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "keep"}, &got))

	err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "stale"}, &got)
	assert.True(t, apierrors.IsNotFound(err), "stale Deployment should have been deleted")

	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "other"}, &got), "non-matching Deployment should be untouched")
}

// TestGC_TolerateConcurrentDeletion simulates a race where a listed Deployment
// is removed between List and Delete (e.g. by a user, another controller, or a
// retrying reconcile). GC must not surface the resulting 404.
func TestGC_TolerateConcurrentDeletion(t *testing.T) {
	ctx := context.Background()

	stale := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "stale", Namespace: "ns", Labels: map[string]string{"cluster": "a"}},
	}
	c := &racingDeleteClient{Client: k8s.NewFakeClient(stale), toVanish: "stale"}

	require.NoError(t, GC(ctx, c, "ns", sets.New[string](), client.MatchingLabels{"cluster": "a"}))
}

// racingDeleteClient wraps a k8s.Client and deletes the named object out from
// under the caller just before Delete is invoked, so Delete returns NotFound.
type racingDeleteClient struct {
	k8s.Client
	toVanish string
}

func (r *racingDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if obj.GetName() == r.toVanish {
		_ = r.Client.Delete(ctx, obj, opts...)
		return r.Client.Delete(ctx, obj, opts...) // second delete returns NotFound
	}
	return r.Client.Delete(ctx, obj, opts...)
}
