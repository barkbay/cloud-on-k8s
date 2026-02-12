// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package filesettings

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commonannotation "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

func Test_ReconcileClusterSecrets(t *testing.T) {
	es := esv1.Elasticsearch{ObjectMeta: metav1.ObjectMeta{
		Namespace: "esNs",
		Name:      "esName",
	}}
	secretNsn := types.NamespacedName{Namespace: "esNs", Name: esv1.FileSettingsSecretName("esName")}

	clusterSecrets := &commonv1.Config{Data: map[string]any{
		"string_secrets": map[string]any{
			"s3": map[string]any{
				"client": map[string]any{
					"default": map[string]any{
						"access_key": "AKIA...",
						"secret_key": "secret",
					},
				},
			},
		},
	}}

	t.Run("no-op when secret does not exist", func(t *testing.T) {
		fakeClient := k8s.NewFakeClient()
		err := ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		assert.NoError(t, err)
	})

	t.Run("sets cluster_secrets on existing empty secret", func(t *testing.T) {
		// Create an empty file settings secret first.
		fakeClient := k8s.NewFakeClient()
		err := ReconcileEmptyFileSettingsSecret(context.Background(), fakeClient, es, true)
		require.NoError(t, err)

		// ReconcileClusterSecrets should update the secret with cluster_secrets.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		// Verify cluster_secrets is set.
		var secret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret)
		require.NoError(t, err)
		settings := parseSettings(t, secret)
		require.NotNil(t, settings.State.ClusterSecrets)
		assert.Equal(t, clusterSecrets.Data, settings.State.ClusterSecrets.Data)
	})

	t.Run("no-op when cluster_secrets unchanged", func(t *testing.T) {
		fakeClient := k8s.NewFakeClient()
		err := ReconcileEmptyFileSettingsSecret(context.Background(), fakeClient, es, true)
		require.NoError(t, err)

		// Set cluster_secrets.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		// Read the secret and note the resource version.
		var secret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret)
		require.NoError(t, err)
		rv := secret.ResourceVersion

		// Reconcile again with the same cluster_secrets — should be a no-op.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		var secret2 corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret2)
		require.NoError(t, err)
		assert.Equal(t, rv, secret2.ResourceVersion, "secret should not have been updated")
	})

	t.Run("updates cluster_secrets when they change", func(t *testing.T) {
		fakeClient := k8s.NewFakeClient()
		err := ReconcileEmptyFileSettingsSecret(context.Background(), fakeClient, es, true)
		require.NoError(t, err)

		// Set initial cluster_secrets.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		var secret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret)
		require.NoError(t, err)
		rv := secret.ResourceVersion

		// Update with different cluster_secrets.
		updatedSecrets := &commonv1.Config{Data: map[string]any{
			"string_secrets": map[string]any{
				"new_key": "new_value",
			},
		}}
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, updatedSecrets)
		require.NoError(t, err)

		var secret2 corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret2)
		require.NoError(t, err)
		assert.NotEqual(t, rv, secret2.ResourceVersion, "secret should have been updated")

		settings := parseSettings(t, secret2)
		require.NotNil(t, settings.State.ClusterSecrets)
		assert.Equal(t, updatedSecrets.Data, settings.State.ClusterSecrets.Data)
	})

	t.Run("preserves other settings fields", func(t *testing.T) {
		fakeClient := k8s.NewFakeClient()
		err := ReconcileEmptyFileSettingsSecret(context.Background(), fakeClient, es, true)
		require.NoError(t, err)

		// Manually set some cluster_settings on the secret (simulating SCP controller).
		var secret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret)
		require.NoError(t, err)
		var settings Settings
		err = json.Unmarshal(secret.Data[SettingsSecretKey], &settings)
		require.NoError(t, err)
		settings.State.ClusterSettings = &commonv1.Config{Data: map[string]any{"indices.recovery.max_bytes_per_sec": "100mb"}}
		settingsBytes, err := json.Marshal(settings)
		require.NoError(t, err)
		secret.Data[SettingsSecretKey] = settingsBytes
		secret.Annotations[commonannotation.SettingsHashAnnotationName] = settings.hash()
		err = fakeClient.Update(context.Background(), &secret)
		require.NoError(t, err)

		// ReconcileClusterSecrets should only update cluster_secrets without touching cluster_settings.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		var updatedSecret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &updatedSecret)
		require.NoError(t, err)
		updatedSettings := parseSettings(t, updatedSecret)

		// cluster_secrets should be set
		require.NotNil(t, updatedSettings.State.ClusterSecrets)
		assert.Equal(t, clusterSecrets.Data, updatedSettings.State.ClusterSecrets.Data)

		// cluster_settings should be preserved
		require.NotNil(t, updatedSettings.State.ClusterSettings)
		assert.Equal(t, "100mb", updatedSettings.State.ClusterSettings.Data["indices.recovery.max_bytes_per_sec"])
	})

	t.Run("clears cluster_secrets when set to nil", func(t *testing.T) {
		fakeClient := k8s.NewFakeClient()
		err := ReconcileEmptyFileSettingsSecret(context.Background(), fakeClient, es, true)
		require.NoError(t, err)

		// Set cluster_secrets first.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		// Verify they are set.
		var secret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &secret)
		require.NoError(t, err)
		settings := parseSettings(t, secret)
		require.NotNil(t, settings.State.ClusterSecrets)

		// Now reconcile with nil to clear them.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, nil)
		require.NoError(t, err)

		var clearedSecret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &clearedSecret)
		require.NoError(t, err)
		clearedSettings := parseSettings(t, clearedSecret)
		assert.Nil(t, clearedSettings.State.ClusterSecrets)
	})

	t.Run("clears cluster_secrets when set to empty", func(t *testing.T) {
		fakeClient := k8s.NewFakeClient()
		err := ReconcileEmptyFileSettingsSecret(context.Background(), fakeClient, es, true)
		require.NoError(t, err)

		// Set cluster_secrets first.
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, clusterSecrets)
		require.NoError(t, err)

		// Reconcile with empty Config (as buildClusterSecrets returns when no secrets exist).
		emptySecrets := &commonv1.Config{Data: map[string]any{}}
		err = ReconcileClusterSecrets(context.Background(), fakeClient, es, emptySecrets)
		require.NoError(t, err)

		var clearedSecret corev1.Secret
		err = fakeClient.Get(context.Background(), secretNsn, &clearedSecret)
		require.NoError(t, err)
		clearedSettings := parseSettings(t, clearedSecret)
		require.NotNil(t, clearedSettings.State.ClusterSecrets)
		assert.Empty(t, clearedSettings.State.ClusterSecrets.Data)
	})
}
