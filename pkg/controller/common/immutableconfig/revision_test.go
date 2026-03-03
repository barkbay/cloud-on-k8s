// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var (
	testGCLabels = client.MatchingLabels{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}
	testRSLabels = client.MatchingLabels{
		"app": "elasticsearch",
	}
)

func testRevisions(t *testing.T, c client.Client, owner client.Object) Revisions {
	revisions, err := NewRevisions(c, owner, "default").
		WithGCLabels(testGCLabels).
		WithReplicaSetLabels(testRSLabels).
		Build()
	require.NoError(t, err)
	return revisions
}

func TestSecretRevision_Reconcile(t *testing.T) {
	ctx := context.Background()

	t.Run("creates secret and tracks name", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().Build()
		owner := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default", UID: "uid-1"},
		}
		rev := testRevisions(t, k8sClient, owner).ForSecretVolume("config")

		secret := BuildImmutableSecret("my-config", "default", map[string][]byte{"key": []byte("val")}, nil)
		name, err := rev.Reconcile(ctx, &secret)
		require.NoError(t, err)
		assert.Contains(t, name, "my-config-")

		var got corev1.Secret
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &got))
		assert.Equal(t, secret.Data, got.Data)

		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, "owner", got.OwnerReferences[0].Name)
		assert.True(t, rev.reconciled.Has(name))
	})

	t.Run("idempotent on existing secret", func(t *testing.T) {
		existing := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "my-config-aabbccdd", Namespace: "default"},
			Data:       map[string][]byte{"key": []byte("existing")},
		}
		k8sClient := fake.NewClientBuilder().WithObjects(existing).Build()
		rev := testRevisions(t, k8sClient, nil).ForSecretVolume("config")

		secret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "my-config-aabbccdd", Namespace: "default"},
			Data:       map[string][]byte{"key": []byte("new")},
		}
		name, err := rev.Reconcile(ctx, &secret)
		require.NoError(t, err)
		assert.Equal(t, "my-config-aabbccdd", name)
		assert.True(t, rev.reconciled.Has(name))

		var got corev1.Secret
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &got))
		assert.Equal(t, []byte("existing"), got.Data["key"])
	})

	t.Run("tracks multiple names", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().Build()
		rev := testRevisions(t, k8sClient, nil).ForSecretVolume("config")

		s1 := BuildImmutableSecret("cfg", "default", map[string][]byte{"a": []byte("1")}, nil)
		s2 := BuildImmutableSecret("cfg", "default", map[string][]byte{"b": []byte("2")}, nil)

		name1, err := rev.Reconcile(ctx, &s1)
		require.NoError(t, err)
		name2, err := rev.Reconcile(ctx, &s2)
		require.NoError(t, err)

		assert.NotEqual(t, name1, name2)
		assert.True(t, rev.reconciled.Has(name1))
		assert.True(t, rev.reconciled.Has(name2))
	})
}

func TestSecretRevision_PatchVolumes(t *testing.T) {
	rev := testRevisions(t, fake.NewClientBuilder().Build(), nil).ForSecretVolume("config-volume")

	volumes := []corev1.Volume{
		{
			Name: "config-volume",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "old-config"},
			},
		},
		{
			Name: "other-volume",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "other-secret"},
			},
		},
	}

	rev.PatchVolumes(volumes, "new-config-a1b2c3d4")

	assert.Equal(t, "new-config-a1b2c3d4", volumes[0].Secret.SecretName)
	assert.Equal(t, "other-secret", volumes[1].Secret.SecretName)
}

func TestSecretRevision_GC(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	t.Run("deletes unreferenced secrets", func(t *testing.T) {
		current := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-aabbccdd", Namespace: "default", Labels: labels},
		}
		old := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-11223344", Namespace: "default", Labels: labels},
		}
		unrelated := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", Labels: map[string]string{"x": "y"}},
		}

		k8sClient := fake.NewClientBuilder().WithObjects(current, old, unrelated).Build()
		rev := testRevisions(t, k8sClient, nil).ForSecretVolume("config")
		rev.reconciled.Insert("cfg-aabbccdd")

		require.NoError(t, rev.GC(ctx))

		var s corev1.Secret
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-aabbccdd", Namespace: "default"}, &s))
		assert.Error(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-11223344", Namespace: "default"}, &s))
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "other", Namespace: "default"}, &s))
	})

	t.Run("protects secrets referenced by ReplicaSets", func(t *testing.T) {
		current := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-aabbccdd", Namespace: "default", Labels: labels},
		}
		oldStillUsed := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-11223344", Namespace: "default", Labels: labels},
		}
		veryOld := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cfg-00000000", Namespace: "default", Labels: labels},
		}

		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "old-rs", Namespace: "default",
				Labels: map[string]string{"app": "elasticsearch"},
			},
			Spec: appsv1.ReplicaSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{SecretName: "cfg-11223344"},
							},
						}},
					},
				},
			},
		}

		k8sClient := fake.NewClientBuilder().WithObjects(current, oldStillUsed, veryOld, rs).Build()
		rev := testRevisions(t, k8sClient, nil).ForSecretVolume("config")
		rev.reconciled.Insert("cfg-aabbccdd")

		require.NoError(t, rev.GC(ctx))

		var s corev1.Secret
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-aabbccdd", Namespace: "default"}, &s))
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-11223344", Namespace: "default"}, &s))
		assert.Error(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-00000000", Namespace: "default"}, &s))
	})
}

func TestConfigMapRevision_Reconcile(t *testing.T) {
	ctx := context.Background()

	t.Run("creates configmap and tracks name", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().Build()
		owner := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default", UID: "uid-2"},
		}
		rev := testRevisions(t, k8sClient, owner).ForConfigMapVolume("scripts")

		cm := BuildImmutableConfigMap("my-scripts", "default", map[string]string{"s.sh": "echo hi"}, nil)
		name, err := rev.Reconcile(ctx, &cm)
		require.NoError(t, err)
		assert.Contains(t, name, "my-scripts-")

		var got corev1.ConfigMap
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, &got))
		assert.Equal(t, cm.Data, got.Data)
		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, "owner", got.OwnerReferences[0].Name)
		assert.True(t, rev.reconciled.Has(name))
	})

	t.Run("idempotent on existing configmap", func(t *testing.T) {
		existing := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-aabbccdd", Namespace: "default"},
			Data:       map[string]string{"s.sh": "existing"},
		}
		k8sClient := fake.NewClientBuilder().WithObjects(existing).Build()
		rev := testRevisions(t, k8sClient, nil).ForConfigMapVolume("scripts")

		cm := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-aabbccdd", Namespace: "default"},
			Data:       map[string]string{"s.sh": "new"},
		}
		name, err := rev.Reconcile(ctx, &cm)
		require.NoError(t, err)
		assert.Equal(t, "scripts-aabbccdd", name)
		assert.True(t, rev.reconciled.Has(name))
	})
}

func TestConfigMapRevision_PatchVolumes(t *testing.T) {
	rev := testRevisions(t, fake.NewClientBuilder().Build(), nil).ForConfigMapVolume("scripts-volume")

	volumes := []corev1.Volume{
		{
			Name: "scripts-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "old-scripts"},
				},
			},
		},
		{
			Name: "other-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "other-configmap"},
				},
			},
		},
	}

	rev.PatchVolumes(volumes, "new-scripts-a1b2c3d4")

	assert.Equal(t, "new-scripts-a1b2c3d4", volumes[0].ConfigMap.Name)
	assert.Equal(t, "other-configmap", volumes[1].ConfigMap.Name)
}

func TestConfigMapRevision_GC(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	t.Run("deletes unreferenced configmaps", func(t *testing.T) {
		current := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-aabbccdd", Namespace: "default", Labels: labels},
		}
		old := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-11223344", Namespace: "default", Labels: labels},
		}

		k8sClient := fake.NewClientBuilder().WithObjects(current, old).Build()
		rev := testRevisions(t, k8sClient, nil).ForConfigMapVolume("scripts")
		rev.reconciled.Insert("scripts-aabbccdd")

		require.NoError(t, rev.GC(ctx))

		var cm corev1.ConfigMap
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "scripts-aabbccdd", Namespace: "default"}, &cm))
		assert.Error(t, k8sClient.Get(ctx, types.NamespacedName{Name: "scripts-11223344", Namespace: "default"}, &cm))
	})

	t.Run("protects configmaps referenced by ReplicaSets", func(t *testing.T) {
		current := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-aabbccdd", Namespace: "default", Labels: labels},
		}
		oldStillUsed := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-11223344", Namespace: "default", Labels: labels},
		}
		veryOld := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "scripts-00000000", Namespace: "default", Labels: labels},
		}

		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "old-rs", Namespace: "default",
				Labels: map[string]string{"app": "elasticsearch"},
			},
			Spec: appsv1.ReplicaSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{
							Name: "scripts",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "scripts-11223344"},
								},
							},
						}},
					},
				},
			},
		}

		k8sClient := fake.NewClientBuilder().WithObjects(current, oldStillUsed, veryOld, rs).Build()
		rev := testRevisions(t, k8sClient, nil).ForConfigMapVolume("scripts")
		rev.reconciled.Insert("scripts-aabbccdd")

		require.NoError(t, rev.GC(ctx))

		var cm corev1.ConfigMap
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "scripts-aabbccdd", Namespace: "default"}, &cm))
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "scripts-11223344", Namespace: "default"}, &cm))
		assert.Error(t, k8sClient.Get(ctx, types.NamespacedName{Name: "scripts-00000000", Namespace: "default"}, &cm))
	})
}

func TestGCAll(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	oldSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-11223344", Namespace: "default", Labels: labels},
	}
	oldCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "scripts-11223344", Namespace: "default", Labels: labels},
	}

	k8sClient := fake.NewClientBuilder().WithObjects(oldSecret, oldCM).Build()
	revs := testRevisions(t, k8sClient, nil)
	secretRev := revs.ForSecretVolume("config")
	cmRev := revs.ForConfigMapVolume("scripts")

	require.NoError(t, GCAll(ctx, secretRev, cmRev))

	var s corev1.Secret
	assert.Error(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-11223344", Namespace: "default"}, &s))
	var cm corev1.ConfigMap
	assert.Error(t, k8sClient.Get(ctx, types.NamespacedName{Name: "scripts-11223344", Namespace: "default"}, &cm))
}

func TestSecretRevision_GCWithProtectedNames(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}
	current := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-aabbccdd", Namespace: "default", Labels: labels},
	}
	old := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-11223344", Namespace: "default", Labels: labels},
	}
	k8sClient := fake.NewClientBuilder().WithObjects(current, old).Build()
	rev := testRevisions(t, k8sClient, nil).ForSecretVolume("config")
	rev.reconciled.Insert("cfg-aabbccdd")

	require.NoError(t, rev.GCWithProtectedNames(ctx, sets.New("cfg-11223344")))

	var s corev1.Secret
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-aabbccdd", Namespace: "default"}, &s))
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "cfg-11223344", Namespace: "default"}, &s))
}

func TestRevisionsBuilder_Build(t *testing.T) {
	tests := []struct {
		name    string
		builder RevisionsBuilder
		wantErr string
	}{
		{
			name: "fails when client is missing",
			builder: NewRevisions(nil, nil, "default").
				WithGCLabels(testGCLabels).
				WithReplicaSetLabels(testRSLabels),
			wantErr: "client is required",
		},
		{
			name: "fails when namespace is missing",
			builder: NewRevisions(fake.NewClientBuilder().Build(), nil, "").
				WithGCLabels(testGCLabels).
				WithReplicaSetLabels(testRSLabels),
			wantErr: "namespace is required",
		},
		{
			name: "fails when gc labels are missing",
			builder: NewRevisions(fake.NewClientBuilder().Build(), nil, "default").
				WithReplicaSetLabels(testRSLabels),
			wantErr: "gc labels are required",
		},
		{
			name: "fails when replicaset labels are missing",
			builder: NewRevisions(fake.NewClientBuilder().Build(), nil, "default").
				WithGCLabels(testGCLabels),
			wantErr: "replicaset labels are required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.builder.Build()
			require.EqualError(t, err, tt.wantErr)
		})
	}

	t.Run("succeeds when all required fields are set", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().Build()
		gcLabels := client.MatchingLabels{
			"app":               "elasticsearch",
			ConfigTypeLabelName: ConfigTypeImmutable,
		}
		rsLabels := client.MatchingLabels{
			"app": "elasticsearch",
		}

		revisions, err := NewRevisions(k8sClient, nil, "default").
			WithGCLabels(gcLabels).
			WithReplicaSetLabels(rsLabels).
			Build()
		require.NoError(t, err)
		gcLabels["app"] = "mutated"
		rsLabels["app"] = "mutated"
		assert.Equal(t, "default", revisions.namespace)
		assert.Equal(t, "elasticsearch", revisions.gcLabels["app"])
		assert.Equal(t, "elasticsearch", revisions.rsLabels["app"])
	})
}
