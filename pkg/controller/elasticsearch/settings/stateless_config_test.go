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

// statelessES builds a minimal Elasticsearch fixture for role/config tests.
// Only the fields consulted by TierRolesFor and NewStatelessConfig are set;
// NodeSet count defaults to 1 so tiers count as present unless the test
// explicitly passes 0.
func statelessES(nodeSets ...esv1.NodeSet) esv1.Elasticsearch {
	return esv1.Elasticsearch{
		Spec: esv1.ElasticsearchSpec{
			Mode: esv1.ElasticsearchModeStateless,
			ObjectStore: &esv1.ObjectStoreConfig{
				Type:   esv1.ObjectStoreTypeS3,
				Bucket: "b",
			},
			NodeSets: nodeSets,
		},
	}
}

func ns(name string, count int32, tier esv1.StatelessTier) esv1.NodeSet {
	return esv1.NodeSet{Name: name, Count: count, Tier: tier}
}

func TestHasDedicatedMasterTier(t *testing.T) {
	tests := []struct {
		name string
		es   esv1.Elasticsearch
		want bool
	}{
		{
			name: "no master tier",
			es:   statelessES(ns("index-a", 3, esv1.IndexTier), ns("search-a", 2, esv1.SearchTier)),
			want: false,
		},
		{
			name: "master tier with count > 0",
			es: statelessES(
				ns("master-a", 3, esv1.MasterTier),
				ns("index-a", 3, esv1.IndexTier),
				ns("search-a", 2, esv1.SearchTier),
			),
			want: true,
		},
		{
			name: "master tier with count = 0 is ignored (same as absent)",
			es: statelessES(
				ns("master-a", 0, esv1.MasterTier),
				ns("index-a", 3, esv1.IndexTier),
			),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasDedicatedMasterTier(tt.es))
		})
	}
}

func TestTierRolesFor(t *testing.T) {
	indexOnly := statelessES(
		ns("index-a", 3, esv1.IndexTier),
		ns("search-a", 2, esv1.SearchTier),
	)
	withMasters := statelessES(
		ns("master-a", 3, esv1.MasterTier),
		ns("index-a", 3, esv1.IndexTier),
		ns("search-a", 2, esv1.SearchTier),
	)

	tests := []struct {
		name    string
		es      esv1.Elasticsearch
		tier    esv1.StatelessTier
		want    []string
		wantErr bool
	}{
		{
			name: "index tier keeps master role when no dedicated master tier",
			es:   indexOnly,
			tier: esv1.IndexTier,
			want: []string{"master", "index", "ingest", "remote_cluster_client"},
		},
		{
			name: "index tier sheds master role when dedicated master tier is present",
			es:   withMasters,
			tier: esv1.IndexTier,
			want: []string{"index", "ingest", "remote_cluster_client"},
		},
		{
			name: "master tier always carries master role",
			es:   withMasters,
			tier: esv1.MasterTier,
			want: []string{"master", "remote_cluster_client"},
		},
		{
			name: "search tier unaffected by master tier presence",
			es:   withMasters,
			tier: esv1.SearchTier,
			want: []string{"search", "remote_cluster_client", "transform"},
		},
		{
			name: "ml tier unaffected",
			es:   withMasters,
			tier: esv1.MLTier,
			want: []string{"ml", "remote_cluster_client"},
		},
		{
			name:    "unknown tier returns error",
			es:      indexOnly,
			tier:    esv1.StatelessTier("not-a-tier"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TierRolesFor(tt.es, tt.tier)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTierHasMasterRole(t *testing.T) {
	indexOnly := statelessES(
		ns("index-a", 3, esv1.IndexTier),
		ns("search-a", 2, esv1.SearchTier),
	)
	withMasters := statelessES(
		ns("master-a", 3, esv1.MasterTier),
		ns("index-a", 3, esv1.IndexTier),
		ns("search-a", 2, esv1.SearchTier),
	)

	tests := []struct {
		name string
		es   esv1.Elasticsearch
		tier esv1.StatelessTier
		want bool
	}{
		{name: "index-only: index carries master", es: indexOnly, tier: esv1.IndexTier, want: true},
		{name: "index-only: search does not", es: indexOnly, tier: esv1.SearchTier, want: false},
		{name: "with masters: index does NOT carry master", es: withMasters, tier: esv1.IndexTier, want: false},
		{name: "with masters: master tier carries it", es: withMasters, tier: esv1.MasterTier, want: true},
		{name: "with masters: search does not", es: withMasters, tier: esv1.SearchTier, want: false},
		{name: "unknown tier returns false", es: indexOnly, tier: "bogus", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TierHasMasterRole(tt.es, tt.tier))
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
		es := statelessES(ns("index-a", 3, esv1.IndexTier), ns("search-a", 2, esv1.SearchTier))
		es.Spec.ObjectStore = &esv1.ObjectStoreConfig{
			Type:     esv1.ObjectStoreTypeS3,
			Bucket:   "my-bucket",
			Client:   "custom-client",
			BasePath: "prefix/path",
		}
		cfg, err := NewStatelessConfig(es, esv1.IndexTier)
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

	t.Run("index tier sheds master role when dedicated master tier is present", func(t *testing.T) {
		es := statelessES(
			ns("master-a", 3, esv1.MasterTier),
			ns("index-a", 3, esv1.IndexTier),
			ns("search-a", 2, esv1.SearchTier),
		)
		cfg, err := NewStatelessConfig(es, esv1.IndexTier)
		require.NoError(t, err)
		out, _ := renderStateless(t, cfg)
		assert.Equal(t, []string{"index", "ingest", "remote_cluster_client"}, out.Node.Roles)
	})

	t.Run("empty client defaults to 'default'", func(t *testing.T) {
		es := statelessES(ns("search-a", 2, esv1.SearchTier))
		es.Spec.ObjectStore = &esv1.ObjectStoreConfig{Type: esv1.ObjectStoreTypeGCS, Bucket: "b"}
		cfg, err := NewStatelessConfig(es, esv1.SearchTier)
		require.NoError(t, err)
		out, _ := renderStateless(t, cfg)
		assert.Equal(t, "default", out.Stateless.ObjectStore.Client)
	})

	t.Run("empty base_path is not emitted", func(t *testing.T) {
		es := statelessES(ns("search-a", 2, esv1.SearchTier))
		es.Spec.ObjectStore = &esv1.ObjectStoreConfig{Type: esv1.ObjectStoreTypeAzure, Bucket: "b"}
		cfg, err := NewStatelessConfig(es, esv1.SearchTier)
		require.NoError(t, err)
		_, raw := renderStateless(t, cfg)
		assert.NotContains(t, string(raw), "base_path",
			"base_path must be omitted from rendered YAML when ObjectStoreConfig.BasePath is empty")
	})

	t.Run("missing objectStore returns error", func(t *testing.T) {
		es := statelessES(ns("index-a", 3, esv1.IndexTier))
		es.Spec.ObjectStore = nil
		_, err := NewStatelessConfig(es, esv1.IndexTier)
		require.Error(t, err)
	})

	t.Run("unknown tier returns error", func(t *testing.T) {
		es := statelessES(ns("index-a", 3, esv1.IndexTier))
		_, err := NewStatelessConfig(es, esv1.StatelessTier("bogus"))
		require.Error(t, err)
	})
}
