// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package filesettings

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	policyv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/stackconfigpolicy/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
)

func Test_NewSettingsSecret(t *testing.T) {
	es := types.NamespacedName{
		Namespace: "esNs",
		Name:      "esName",
	}
	policy := policyv1alpha1.StackConfigPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "policyNs",
			Name:      "policyName",
		},
		Spec: policyv1alpha1.StackConfigPolicySpec{
			Elasticsearch: policyv1alpha1.ElasticsearchConfigPolicySpec{
				ClusterSettings: &commonv1.Config{Data: map[string]any{"a": "b"}},
			},
		},
	}

	// no policy
	expectedVersion := int64(1)
	secret, reconciledVersion, err := newSettingsSecret(expectedVersion, false, es, nil, nil, nil, metadata.Metadata{})
	assert.NoError(t, err)
	assert.Equal(t, "esNs", secret.Namespace)
	assert.Equal(t, "esName-es-file-settings", secret.Name)
	assert.Equal(t, 0, len(parseSettings(t, secret).State.ClusterSettings.Data))
	assert.Equal(t, expectedVersion, reconciledVersion)

	// policy
	expectedVersion = int64(2)
	secret, reconciledVersion, err = newSettingsSecret(expectedVersion, false, es, &secret, &policy.Spec.Elasticsearch, policy.GetElasticsearchNamespacedSecureSettings(), metadata.Metadata{})
	assert.NoError(t, err)
	assert.Equal(t, "esNs", secret.Namespace)
	assert.Equal(t, "esName-es-file-settings", secret.Name)
	assert.Equal(t, 1, len(parseSettings(t, secret).State.ClusterSettings.Data))
	assert.Equal(t, expectedVersion, reconciledVersion)
}

func Test_SettingsSecret_hasChanged(t *testing.T) {
	es := types.NamespacedName{
		Namespace: "esNs",
		Name:      "esName",
	}
	policy := policyv1alpha1.StackConfigPolicy{ObjectMeta: metav1.ObjectMeta{
		Namespace: "policyNs",
		Name:      "policyName",
	}}
	otherPolicy := policyv1alpha1.StackConfigPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "otherPolicyNs",
			Name:      "otherPolicyName",
		},
		Spec: policyv1alpha1.StackConfigPolicySpec{
			Elasticsearch: policyv1alpha1.ElasticsearchConfigPolicySpec{
				ClusterSettings: &commonv1.Config{Data: map[string]any{"a": "b"}},
			},
		}}

	expectedVersion := int64(1)
	expectedEmptySettings := NewEmptySettings(expectedVersion, false)

	// no policy -> emptySettings
	secret, reconciledVersion, err := newSettingsSecret(expectedVersion, false, es, nil, nil, nil, metadata.Metadata{})
	assert.NoError(t, err)
	assert.Equal(t, false, hasChanged(secret, expectedEmptySettings))
	assert.Equal(t, expectedVersion, reconciledVersion)

	// policy without settings -> emptySettings
	sameSettings := NewEmptySettings(expectedVersion, false)
	err = sameSettings.updateState(es, policy.Spec.Elasticsearch)
	assert.NoError(t, err)
	assert.Equal(t, false, hasChanged(secret, sameSettings))
	assert.Equal(t, strconv.FormatInt(expectedVersion, 10), sameSettings.Metadata.Version)

	// new policy -> settings changed
	newVersion := int64(2)
	newSettings := NewEmptySettings(newVersion, false)

	err = newSettings.updateState(es, otherPolicy.Spec.Elasticsearch)
	assert.NoError(t, err)
	assert.Equal(t, true, hasChanged(secret, newSettings))
	assert.Equal(t, strconv.FormatInt(newVersion, 10), newSettings.Metadata.Version)
}

func Test_SettingsSecret_setSecureSettings_getSecureSettings(t *testing.T) {
	es := types.NamespacedName{
		Namespace: "esNs",
		Name:      "esName",
	}
	policy := policyv1alpha1.StackConfigPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "policyNs",
			Name:      "policyName",
		},
		Spec: policyv1alpha1.StackConfigPolicySpec{
			Elasticsearch: policyv1alpha1.ElasticsearchConfigPolicySpec{
				SecureSettings: nil,
			},
		}}
	otherPolicy := policyv1alpha1.StackConfigPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "otherPolicyNs",
			Name:      "otherPolicyName",
		},
		Spec: policyv1alpha1.StackConfigPolicySpec{
			Elasticsearch: policyv1alpha1.ElasticsearchConfigPolicySpec{
				SecureSettings: []commonv1.SecretSource{{SecretName: "secure-settings-secret"}},
			},
		}}
	
	secret, _, err := NewSettingsSecretWithVersion(es, false, nil, nil, nil, metadata.Metadata{})
	assert.NoError(t, err)

	secureSettings, err := getSecureSettings(secret)
	assert.NoError(t, err)
	assert.Equal(t, []commonv1.NamespacedSecretSource{}, secureSettings)

	err = setSecureSettings(&secret, policy.GetElasticsearchNamespacedSecureSettings())
	assert.NoError(t, err)
	secureSettings, err = getSecureSettings(secret)
	assert.NoError(t, err)
	assert.Equal(t, []commonv1.NamespacedSecretSource{}, secureSettings)

	err = setSecureSettings(&secret, otherPolicy.GetElasticsearchNamespacedSecureSettings())
	assert.NoError(t, err)
	secureSettings, err = getSecureSettings(secret)
	assert.NoError(t, err)
	assert.Equal(t, []commonv1.NamespacedSecretSource{{Namespace: otherPolicy.Namespace, SecretName: "secure-settings-secret"}}, secureSettings)
}

func Test_newSettingsSecret_preservesClusterSecrets(t *testing.T) {
	es := types.NamespacedName{
		Namespace: "esNs",
		Name:      "esName",
	}

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

	// Create an initial stateless secret that has cluster_secrets set (as ReconcileClusterSecrets would do).
	initialVersion := int64(1)
	initialSecret, _, err := newSettingsSecret(initialVersion, true, es, nil, nil, nil, metadata.Metadata{})
	assert.NoError(t, err)
	// Simulate ReconcileClusterSecrets having set cluster_secrets on the secret.
	var initialSettings Settings
	err = json.Unmarshal(initialSecret.Data[SettingsSecretKey], &initialSettings)
	assert.NoError(t, err)
	initialSettings.State.ClusterSecrets = clusterSecrets
	settingsBytes, err := json.Marshal(initialSettings)
	assert.NoError(t, err)
	initialSecret.Data[SettingsSecretKey] = settingsBytes

	// Rebuild the secret (as the SCP controller would) with no explicit clusterSecrets.
	// The existing cluster_secrets should be preserved from the current secret.
	rebuiltVersion := int64(2)
	rebuiltSecret, _, err := newSettingsSecret(rebuiltVersion, true, es, &initialSecret, nil, nil, metadata.Metadata{})
	assert.NoError(t, err)
	rebuiltSettings := parseSettings(t, rebuiltSecret)
	assert.NotNil(t, rebuiltSettings.State.ClusterSecrets)
	assert.Equal(t, clusterSecrets.Data, rebuiltSettings.State.ClusterSecrets.Data)
}

func Test_newSettingsSecret_preservesClusterSecrets_withPolicy(t *testing.T) {
	es := types.NamespacedName{
		Namespace: "esNs",
		Name:      "esName",
	}
	policy := policyv1alpha1.ElasticsearchConfigPolicySpec{
		ClusterSettings: &commonv1.Config{Data: map[string]any{"indices.recovery.max_bytes_per_sec": "100mb"}},
	}

	clusterSecrets := &commonv1.Config{Data: map[string]any{
		"string_secrets": map[string]any{
			"xpack": map[string]any{"security": "value"},
		},
	}}

	// Create an initial stateless secret with cluster_secrets and policy settings.
	initialVersion := int64(1)
	initialSecret, _, err := newSettingsSecret(initialVersion, true, es, nil, &policy, nil, metadata.Metadata{})
	assert.NoError(t, err)
	// Simulate ReconcileClusterSecrets having set cluster_secrets.
	var initialSettings Settings
	err = json.Unmarshal(initialSecret.Data[SettingsSecretKey], &initialSettings)
	assert.NoError(t, err)
	initialSettings.State.ClusterSecrets = clusterSecrets
	settingsBytes, err := json.Marshal(initialSettings)
	assert.NoError(t, err)
	initialSecret.Data[SettingsSecretKey] = settingsBytes

	// Rebuild with a policy (as SCP controller would). cluster_secrets should be preserved,
	// and the policy settings should still be applied.
	rebuiltVersion := int64(2)
	rebuiltSecret, _, err := newSettingsSecret(rebuiltVersion, true, es, &initialSecret, &policy, nil, metadata.Metadata{})
	assert.NoError(t, err)
	rebuiltSettings := parseSettings(t, rebuiltSecret)

	// cluster_secrets preserved
	assert.NotNil(t, rebuiltSettings.State.ClusterSecrets)
	assert.Equal(t, clusterSecrets.Data, rebuiltSettings.State.ClusterSecrets.Data)

	// policy settings still applied
	assert.Equal(t, 1, len(rebuiltSettings.State.ClusterSettings.Data))
	assert.Equal(t, "100mb", rebuiltSettings.State.ClusterSettings.Data["indices.recovery.max_bytes_per_sec"])
}

func Test_newSettingsSecret_noClusterSecrets_forStateful(t *testing.T) {
	es := types.NamespacedName{
		Namespace: "esNs",
		Name:      "esName",
	}

	// Create a stateful secret — cluster_secrets should not be set.
	version := int64(1)
	secret, _, err := newSettingsSecret(version, false, es, nil, nil, nil, metadata.Metadata{})
	assert.NoError(t, err)
	settings := parseSettings(t, secret)
	assert.Nil(t, settings.State.ClusterSecrets)
}

func parseSettings(t *testing.T, secret corev1.Secret) Settings {
	t.Helper()
	var settings Settings
	err := json.Unmarshal(secret.Data[SettingsSecretKey], &settings)
	assert.NoError(t, err)
	return settings
}
