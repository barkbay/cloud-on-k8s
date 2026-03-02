// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileImmutableSecret(t *testing.T) {
	ctx := context.Background()

	t.Run("creates secret if not exists", func(t *testing.T) {
		client := fake.NewClientBuilder().Build()

		secret := BuildImmutableSecret("my-config", "default", map[string][]byte{"key": []byte("value")}, nil)
		err := ReconcileImmutableSecret(ctx, client, secret)
		require.NoError(t, err)

		var got corev1.Secret
		err = client.Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: "default"}, &got)
		require.NoError(t, err)
		assert.Equal(t, secret.Data, got.Data)
	})

	t.Run("does not update if exists", func(t *testing.T) {
		existingSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-config-a1b2c3d4",
				Namespace: "default",
			},
			Data: map[string][]byte{"key": []byte("existing")},
		}
		client := fake.NewClientBuilder().WithObjects(existingSecret).Build()

		newSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-config-a1b2c3d4",
				Namespace: "default",
			},
			Data: map[string][]byte{"key": []byte("new")},
		}
		err := ReconcileImmutableSecret(ctx, client, newSecret)
		require.NoError(t, err)

		var got corev1.Secret
		err = client.Get(ctx, types.NamespacedName{Name: "my-config-a1b2c3d4", Namespace: "default"}, &got)
		require.NoError(t, err)
		assert.Equal(t, []byte("existing"), got.Data["key"], "should not update existing secret")
	})

	t.Run("sets owner reference when owner provided", func(t *testing.T) {
		owner := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "owner-resource",
				Namespace: "default",
				UID:       "test-uid-123",
			},
		}
		client := fake.NewClientBuilder().Build()

		secret := BuildImmutableSecret("my-config", "default", map[string][]byte{"key": []byte("value")}, nil)
		err := ReconcileImmutableSecret(ctx, client, secret, owner)
		require.NoError(t, err)

		var got corev1.Secret
		err = client.Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: "default"}, &got)
		require.NoError(t, err)
		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, "owner-resource", got.OwnerReferences[0].Name)
		assert.Equal(t, types.UID("test-uid-123"), got.OwnerReferences[0].UID)
	})

	t.Run("no owner reference when owner not provided", func(t *testing.T) {
		client := fake.NewClientBuilder().Build()

		secret := BuildImmutableSecret("my-config", "default", map[string][]byte{"key": []byte("value")}, nil)
		err := ReconcileImmutableSecret(ctx, client, secret)
		require.NoError(t, err)

		var got corev1.Secret
		err = client.Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: "default"}, &got)
		require.NoError(t, err)
		assert.Empty(t, got.OwnerReferences)
	})
}

func TestReconcileImmutableConfigMap(t *testing.T) {
	ctx := context.Background()

	t.Run("creates configmap if not exists", func(t *testing.T) {
		client := fake.NewClientBuilder().Build()

		cm := BuildImmutableConfigMap("my-scripts", "default", map[string]string{"script.sh": "echo hello"}, nil)
		err := ReconcileImmutableConfigMap(ctx, client, cm)
		require.NoError(t, err)

		var got corev1.ConfigMap
		err = client.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: "default"}, &got)
		require.NoError(t, err)
		assert.Equal(t, cm.Data, got.Data)
	})

	t.Run("does not update if exists", func(t *testing.T) {
		existingCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-scripts-a1b2c3d4",
				Namespace: "default",
			},
			Data: map[string]string{"script.sh": "existing"},
		}
		client := fake.NewClientBuilder().WithObjects(existingCM).Build()

		newCM := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-scripts-a1b2c3d4",
				Namespace: "default",
			},
			Data: map[string]string{"script.sh": "new"},
		}
		err := ReconcileImmutableConfigMap(ctx, client, newCM)
		require.NoError(t, err)

		var got corev1.ConfigMap
		err = client.Get(ctx, types.NamespacedName{Name: "my-scripts-a1b2c3d4", Namespace: "default"}, &got)
		require.NoError(t, err)
		assert.Equal(t, "existing", got.Data["script.sh"], "should not update existing configmap")
	})

	t.Run("sets owner reference when owner provided", func(t *testing.T) {
		owner := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "owner-resource",
				Namespace: "default",
				UID:       "test-uid-456",
			},
		}
		client := fake.NewClientBuilder().Build()

		cm := BuildImmutableConfigMap("my-scripts", "default", map[string]string{"script.sh": "echo hello"}, nil)
		err := ReconcileImmutableConfigMap(ctx, client, cm, owner)
		require.NoError(t, err)

		var got corev1.ConfigMap
		err = client.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: "default"}, &got)
		require.NoError(t, err)
		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, "owner-resource", got.OwnerReferences[0].Name)
		assert.Equal(t, types.UID("test-uid-456"), got.OwnerReferences[0].UID)
	})
}

func TestGCUnreferencedSecrets(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	secret1 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "config-a1b2c3d4",
			Namespace: "default",
			Labels:    labels,
		},
	}
	secret2 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "config-e5f6g7h8",
			Namespace: "default",
			Labels:    labels,
		},
	}
	secret3 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-secret",
			Namespace: "default",
			Labels:    map[string]string{"app": "other"},
		},
	}

	client := fake.NewClientBuilder().WithObjects(secret1, secret2, secret3).Build()

	protectedNames := sets.New[string]("config-a1b2c3d4")
	labelSelector := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	err := GCUnreferencedSecrets(ctx, client, "default", labelSelector, protectedNames)
	require.NoError(t, err)

	// Protected secret should still exist
	var got corev1.Secret
	err = client.Get(ctx, types.NamespacedName{Name: "config-a1b2c3d4", Namespace: "default"}, &got)
	require.NoError(t, err)

	// Unprotected secret should be deleted
	err = client.Get(ctx, types.NamespacedName{Name: "config-e5f6g7h8", Namespace: "default"}, &got)
	require.Error(t, err)

	// Secret with different labels should not be affected
	err = client.Get(ctx, types.NamespacedName{Name: "other-secret", Namespace: "default"}, &got)
	require.NoError(t, err)
}

func TestGCUnreferencedConfigMaps(t *testing.T) {
	ctx := context.Background()

	labels := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	cm1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scripts-a1b2c3d4",
			Namespace: "default",
			Labels:    labels,
		},
	}
	cm2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scripts-e5f6g7h8",
			Namespace: "default",
			Labels:    labels,
		},
	}

	client := fake.NewClientBuilder().WithObjects(cm1, cm2).Build()

	protectedNames := sets.New[string]("scripts-a1b2c3d4")
	labelSelector := map[string]string{
		"app":               "elasticsearch",
		ConfigTypeLabelName: ConfigTypeImmutable,
	}

	err := GCUnreferencedConfigMaps(ctx, client, "default", labelSelector, protectedNames)
	require.NoError(t, err)

	// Protected configmap should still exist
	var got corev1.ConfigMap
	err = client.Get(ctx, types.NamespacedName{Name: "scripts-a1b2c3d4", Namespace: "default"}, &got)
	require.NoError(t, err)

	// Unprotected configmap should be deleted
	err = client.Get(ctx, types.NamespacedName{Name: "scripts-e5f6g7h8", Namespace: "default"}, &got)
	require.Error(t, err)
}
