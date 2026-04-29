// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

func TestBuildStatelessImmutableConfigSecret(t *testing.T) {
	es := types.NamespacedName{Namespace: "ns", Name: "my-es"}
	esForCfg := esv1.Elasticsearch{
		Spec: esv1.ElasticsearchSpec{
			Mode: esv1.ElasticsearchModeStateless,
			ObjectStore: &esv1.ObjectStoreConfig{
				Type: esv1.ObjectStoreTypeS3, Bucket: "b",
			},
		},
	}
	baseCfg, err := NewStatelessConfig(esForCfg, esv1.IndexTier)
	require.NoError(t, err)

	t.Run("secret is content-addressed and carries expected metadata", func(t *testing.T) {
		secret, err := BuildStatelessImmutableConfigSecret(es, "my-es-es-index-a", esv1.IndexTier, CanonicalConfig{baseCfg.CanonicalConfig})
		require.NoError(t, err)

		assert.Equal(t, "ns", secret.Namespace)
		assert.True(t, strings.HasPrefix(secret.Name, "my-es-es-index-a-es-config-"),
			"name %q should start with <ConfigSecretName(deployment)>-", secret.Name)
		require.NotNil(t, secret.Immutable)
		assert.True(t, *secret.Immutable, "stateless config Secrets must be immutable")

		require.Contains(t, secret.Data, ConfigFileName)
		assert.NotEmpty(t, secret.Data[ConfigFileName])

		// Labels: cluster + deployment + tier + config-type/hash tags
		assert.Equal(t, "my-es", secret.Labels[label.ClusterNameLabelName])
		assert.Equal(t, "my-es-es-index-a", secret.Labels[label.DeploymentNameLabelName])
		assert.Equal(t, "index", secret.Labels[label.TierLabelName])
		assert.Equal(t, immutableconfig.ConfigTypeImmutable, secret.Labels[immutableconfig.ConfigTypeLabelName])
		assert.NotEmpty(t, secret.Labels[immutableconfig.ConfigHashLabelName])
	})

	t.Run("same content produces the same name (idempotent)", func(t *testing.T) {
		s1, err := BuildStatelessImmutableConfigSecret(es, "dep-a", esv1.IndexTier, CanonicalConfig{baseCfg.CanonicalConfig})
		require.NoError(t, err)
		s2, err := BuildStatelessImmutableConfigSecret(es, "dep-a", esv1.IndexTier, CanonicalConfig{baseCfg.CanonicalConfig})
		require.NoError(t, err)
		assert.Equal(t, s1.Name, s2.Name)
		assert.Equal(t, s1.Labels[immutableconfig.ConfigHashLabelName], s2.Labels[immutableconfig.ConfigHashLabelName])
	})

	t.Run("different content produces a different name", func(t *testing.T) {
		esOther := esv1.Elasticsearch{
			Spec: esv1.ElasticsearchSpec{
				Mode: esv1.ElasticsearchModeStateless,
				ObjectStore: &esv1.ObjectStoreConfig{
					Type: esv1.ObjectStoreTypeS3, Bucket: "different-bucket",
				},
			},
		}
		other, err := NewStatelessConfig(esOther, esv1.IndexTier)
		require.NoError(t, err)

		s1, err := BuildStatelessImmutableConfigSecret(es, "dep-a", esv1.IndexTier, CanonicalConfig{baseCfg.CanonicalConfig})
		require.NoError(t, err)
		s2, err := BuildStatelessImmutableConfigSecret(es, "dep-a", esv1.IndexTier, CanonicalConfig{other.CanonicalConfig})
		require.NoError(t, err)
		assert.NotEqual(t, s1.Name, s2.Name,
			"config changes must produce a new content-addressed Secret to force pod rotation")
	})
}
