// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
)

func TestTierRoles(t *testing.T) {
	tests := []struct {
		name    string
		tier    esv1.StatelessTier
		want    []string
		wantErr bool
	}{
		{
			name: "index",
			tier: esv1.IndexTier,
			want: []string{"master", "index", "ingest", "remote_cluster_client"},
		},
		{
			name: "search",
			tier: esv1.SearchTier,
			want: []string{"search", "remote_cluster_client", "transform"},
		},
		{
			name: "master",
			tier: esv1.MasterTier,
			want: []string{"master", "remote_cluster_client"},
		},
		{
			name: "ml",
			tier: esv1.MLTier,
			want: []string{"ml", "remote_cluster_client"},
		},
		{
			name:    "unknown tier returns error",
			tier:    esv1.StatelessTier("not-a-tier"),
			wantErr: true,
		},
		{
			name:    "empty tier returns error",
			tier:    esv1.StatelessTier(""),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TierRoles(tt.tier)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// renderedConfig is a lightweight YAML view of the stateless settings relevant to
// tests. Fields we do not currently assert on are omitted on purpose to keep the
// test intent obvious and the struct small.
type renderedConfig struct {
	Node struct {
		Roles []string `yaml:"roles"`
	} `yaml:"node"`
	Discovery struct {
		SeedProviders string `yaml:"seed_providers"`
	} `yaml:"discovery"`
	HTTP struct {
		PublishHost string `yaml:"publish_host"`
	} `yaml:"http"`
	Stateless struct {
		Enabled     bool `yaml:"enabled"`
		ObjectStore struct {
			Type     string `yaml:"type"`
			Bucket   string `yaml:"bucket"`
			Client   string `yaml:"client"`
			BasePath string `yaml:"base_path,omitempty"`
		} `yaml:"object_store"`
	} `yaml:"stateless"`
}

// renderStateless is a test helper that renders the given CanonicalConfig and
// unmarshals it into renderedConfig for easy field-by-field assertions.
// It also returns the raw YAML so tests can make presence/absence assertions
// that do not fit the static struct shape.
func renderStateless(t *testing.T, cfg *CanonicalConfig) (renderedConfig, []byte) {
	t.Helper()
	raw, err := cfg.Render()
	require.NoError(t, err)
	out := renderedConfig{}
	require.NoError(t, yaml.Unmarshal(raw, &out))
	return out, raw
}

func TestNewStatelessConfig(t *testing.T) {
	t.Run("index tier happy path with S3 object store", func(t *testing.T) {
		cfg, err := NewStatelessConfig(esv1.IndexTier, esv1.ObjectStoreConfig{
			Type:     esv1.ObjectStoreTypeS3,
			Bucket:   "my-bucket",
			Client:   "custom-client",
			BasePath: "prefix/path",
		})
		require.NoError(t, err)

		out, _ := renderStateless(t, cfg)

		assert.Equal(t, "0", out.HTTP.PublishHost, "http.publish_host must be '0' in stateless mode")
		assert.Equal(t, "file", out.Discovery.SeedProviders)
		assert.Equal(t, []string{"master", "index", "ingest", "remote_cluster_client"}, out.Node.Roles)

		assert.True(t, out.Stateless.Enabled)
		assert.Equal(t, "s3", out.Stateless.ObjectStore.Type)
		assert.Equal(t, "my-bucket", out.Stateless.ObjectStore.Bucket)
		assert.Equal(t, "custom-client", out.Stateless.ObjectStore.Client)
		assert.Equal(t, "prefix/path", out.Stateless.ObjectStore.BasePath)
	})

	t.Run("empty client defaults to 'default'", func(t *testing.T) {
		cfg, err := NewStatelessConfig(esv1.SearchTier, esv1.ObjectStoreConfig{
			Type:   esv1.ObjectStoreTypeGCS,
			Bucket: "b",
		})
		require.NoError(t, err)
		out, _ := renderStateless(t, cfg)
		assert.Equal(t, "default", out.Stateless.ObjectStore.Client)
	})

	t.Run("empty base_path is not emitted", func(t *testing.T) {
		cfg, err := NewStatelessConfig(esv1.SearchTier, esv1.ObjectStoreConfig{
			Type:   esv1.ObjectStoreTypeAzure,
			Bucket: "b",
		})
		require.NoError(t, err)
		_, raw := renderStateless(t, cfg)
		assert.NotContains(t, string(raw), "base_path",
			"base_path must be omitted from rendered YAML when ObjectStoreConfig.BasePath is empty")
	})

	t.Run("unknown tier returns error", func(t *testing.T) {
		_, err := NewStatelessConfig(esv1.StatelessTier("bogus"), esv1.ObjectStoreConfig{
			Type: esv1.ObjectStoreTypeS3, Bucket: "b",
		})
		require.Error(t, err)
	})
}
