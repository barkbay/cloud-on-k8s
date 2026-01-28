// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/compare"
)

func TestConfig_RoleDefaults(t *testing.T) {
	type args struct {
		c   commonv1.Config
		ver version.Version
	}
	tests := []struct {
		name    string
		args    args
		wantCfg escommon.Node
	}{
		{
			name: "empty equals defaults",
			args: args{},
			wantCfg: escommon.Node{
				Master:    ptr.To[bool](true),
				Data:      ptr.To[bool](true),
				Ingest:    ptr.To[bool](true),
				ML:        ptr.To[bool](true),
				Transform: ptr.To[bool](false),
			},
		},
		{
			name: "set node.master=true",
			args: args{
				c: commonv1.Config{
					Data: map[string]interface{}{
						escommon.NodeMaster: true,
					},
				},
			},
			wantCfg: escommon.Node{
				Master:    ptr.To[bool](true),
				Data:      ptr.To[bool](true),
				Ingest:    ptr.To[bool](true),
				ML:        ptr.To[bool](true),
				Transform: ptr.To[bool](false),
			},
		},
		{
			name: "set node.data=false",
			args: args{
				c: commonv1.Config{
					Data: map[string]interface{}{
						escommon.NodeData: false,
					},
				},
			},
			wantCfg: escommon.Node{
				Master:    ptr.To[bool](true),
				Data:      ptr.To[bool](false),
				Ingest:    ptr.To[bool](true),
				ML:        ptr.To[bool](true),
				Transform: ptr.To[bool](false),
			},
		},
		{
			name: "defaults for versions above 7.7.0",
			args: args{
				ver: version.From(7, 7, 0),
			},
			wantCfg: escommon.Node{
				Master: ptr.To[bool](true),
				Data:   ptr.To[bool](true),
				Ingest: ptr.To[bool](true),
				ML:     ptr.To[bool](true),
			},
		},
		{
			name: "set node.data=false on 7.7.0",
			args: args{
				c: commonv1.Config{
					Data: map[string]interface{}{
						"node": map[string]interface{}{
							"data": false,
						},
					},
				},
				ver: version.From(7, 7, 0),
			},
			wantCfg: escommon.Node{
				Master: ptr.To[bool](true),
				Data:   ptr.To[bool](false),
				Ingest: ptr.To[bool](true),
				ML:     ptr.To[bool](true),
			},
		},
		{
			name: "set node.transform=true and node.data=false on 7.7.0",
			args: args{
				c: commonv1.Config{
					Data: map[string]interface{}{
						escommon.NodeData:      false,
						escommon.NodeTransform: true,
					},
				},
				ver: version.From(7, 7, 0),
			},
			wantCfg: escommon.Node{
				Master:    ptr.To[bool](true),
				Data:      ptr.To[bool](false),
				Ingest:    ptr.To[bool](true),
				ML:        ptr.To[bool](true),
				Transform: ptr.To[bool](true),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DefaultCfg(tt.args.ver)
			err := UnpackConfig(&tt.args.c, tt.args.ver, &got)
			require.NoError(t, err)
			compare.JSONEqual(t, tt.wantCfg, got.Node)
		})
	}
}

var testFixture = commonv1.Config{
	Data: map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": 1.0,
			},
			"d": 1,
		},
		"a.b.foo": "bar",
		"e":       []interface{}{1, 2, 3},
		"f":       true,
	},
}

var expectedJSONized = commonv1.Config{
	Data: map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": 1.0,
			},
			"d": float64(1),
		},
		"a.b.foo": "bar",
		"e":       []interface{}{float64(1), float64(2), float64(3)},
		"f":       true,
	},
}

var (
	// roles that are applied by default (everything except voting_only)
	defaultRoles = []escommon.NodeRole{
		escommon.DataColdRole,
		escommon.DataContentRole,
		escommon.DataHotRole,
		escommon.DataRole,
		escommon.DataWarmRole,
		escommon.IngestRole,
		escommon.MLRole,
		escommon.MasterRole,
		escommon.RemoteClusterClientRole,
		escommon.TransformRole,
	}

	allRoles = append([]escommon.NodeRole{escommon.VotingOnlyRole}, defaultRoles...)
)

func TestConfig_HasRole(t *testing.T) {
	testCases := []struct {
		name      string
		node      *escommon.Node
		wantRoles []escommon.NodeRole
	}{
		{
			name:      "master and data",
			node:      &escommon.Node{Roles: []string{"master", "data"}},
			wantRoles: []escommon.NodeRole{escommon.MasterRole, escommon.DataContentRole, escommon.DataRole, escommon.DataHotRole, escommon.DataWarmRole, escommon.DataColdRole, escommon.DataFrozenRole},
		},
		{
			name:      "master and data_content",
			node:      &escommon.Node{Roles: []string{"master", "data_content"}},
			wantRoles: []escommon.NodeRole{escommon.MasterRole, escommon.DataContentRole},
		},
		{
			name:      "data_hot and data_warm only",
			node:      &escommon.Node{Roles: []string{"data_hot", "data_warm"}},
			wantRoles: []escommon.NodeRole{escommon.DataHotRole, escommon.DataWarmRole},
		},
		{
			name:      "node.roles (ingest only)",
			node:      &escommon.Node{Roles: []string{"ingest"}},
			wantRoles: []escommon.NodeRole{escommon.IngestRole},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wantRolesSet := make(map[escommon.NodeRole]struct{}, len(tc.wantRoles))

			// check that the node has the required roles
			for _, r := range tc.wantRoles {
				wantRolesSet[r] = struct{}{}

				require.True(t, tc.node.HasRole(r), "Missing wanted role [%s]", r)
			}

			// check that the node does not have any other roles
			for _, r := range allRoles {
				if _, exists := wantRolesSet[r]; exists {
					continue
				}

				require.False(t, tc.node.HasRole(r), "Unexpected role [%s]", r)
			}
		})
	}
}

func TestConfig_IsConfiguredWithRole(t *testing.T) {
	testCases := []struct {
		name      string
		node      *escommon.Node
		wantRoles []escommon.NodeRole
	}{
		{
			name:      "nil node",
			wantRoles: defaultRoles,
		},
		{
			name:      "empty node",
			node:      &escommon.Node{},
			wantRoles: defaultRoles,
		},
		{
			name: "node role attributes (all)",
			node: &escommon.Node{
				Master:              ptr.To[bool](true),
				Data:                ptr.To[bool](true),
				Ingest:              ptr.To[bool](true),
				ML:                  ptr.To[bool](true),
				Transform:           ptr.To[bool](true),
				RemoteClusterClient: ptr.To[bool](true),
			},
			wantRoles: defaultRoles,
		},
		{
			name: "node role attributes (no data)",
			node: &escommon.Node{
				Data: ptr.To[bool](false),
			},
			wantRoles: []escommon.NodeRole{escommon.IngestRole, escommon.MLRole, escommon.MasterRole, escommon.RemoteClusterClientRole},
		},
		{
			name: "node role attributes (ingest only)",
			node: &escommon.Node{
				Master:     ptr.To[bool](false),
				Data:       ptr.To[bool](false),
				Ingest:     ptr.To[bool](true),
				ML:         ptr.To[bool](false),
				Transform:  ptr.To[bool](false),
				VotingOnly: ptr.To[bool](false),
			},
			wantRoles: []escommon.NodeRole{escommon.IngestRole, escommon.RemoteClusterClientRole},
		},
		{
			name: "mixed node.roles and node role attributes",
			node: &escommon.Node{
				Master:     ptr.To[bool](false),
				Data:       ptr.To[bool](false),
				Ingest:     ptr.To[bool](true),
				ML:         ptr.To[bool](false),
				Transform:  ptr.To[bool](false),
				VotingOnly: ptr.To[bool](false),
				Roles:      []string{"master"},
			},
			wantRoles: []escommon.NodeRole{escommon.MasterRole},
		},
		{
			name: "node.roles (all)",
			node: &escommon.Node{
				Roles: []string{
					"master",
					"data",
					"data_cold",
					"data_content",
					"data_hot",
					"data_warm",
					"ingest",
					"ml",
					"remote_cluster_client",
					"transform",
				},
			},
			wantRoles: defaultRoles,
		},
		{
			name:      "node.roles (master and data)",
			node:      &escommon.Node{Roles: []string{"master", "data"}},
			wantRoles: []escommon.NodeRole{escommon.MasterRole, escommon.DataRole},
		},
		{
			name:      "node.roles (ingest only)",
			node:      &escommon.Node{Roles: []string{"ingest"}},
			wantRoles: []escommon.NodeRole{escommon.IngestRole},
		},
		{
			name: "node.roles (no roles)",
			node: &escommon.Node{Roles: []string{}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wantRolesSet := make(map[escommon.NodeRole]struct{}, len(tc.wantRoles))

			// check that the node has the required roles
			for _, r := range tc.wantRoles {
				wantRolesSet[r] = struct{}{}

				require.True(t, tc.node.IsConfiguredWithRole(r), "Missing wanted role [%s]", r)
			}

			// check that the node does not have any other roles
			for _, r := range allRoles {
				if _, exists := wantRolesSet[r]; exists {
					continue
				}

				require.False(t, tc.node.IsConfiguredWithRole(r), "Unexpected role [%s]", r)
			}
		})
	}
}

func TestConfig_DeepCopyInto(t *testing.T) {
	tests := []struct {
		name     string
		in       commonv1.Config
		expected commonv1.Config
	}{
		{
			name:     "deep copy via JSON roundtrip changes some types",
			in:       testFixture,
			expected: expectedJSONized,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out commonv1.Config
			tt.in.DeepCopyInto(&out)
			if diff := deep.Equal(out, tt.expected); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestConfig_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		in   commonv1.Config
		want commonv1.Config
	}{
		{
			name: "deep copy via JSON roundtrip changes some types",
			in:   testFixture,
			want: expectedJSONized,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := deep.Equal(tt.in.DeepCopy(), &tt.want); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestConfig_Unpack(t *testing.T) {
	ver := version.From(7, 7, 0)
	tests := []struct {
		name    string
		args    *commonv1.Config
		want    ElasticsearchSettings
		wantErr bool
	}{
		{
			name: "no node config",
			args: &commonv1.Config{
				Data: map[string]interface{}{
					"cluster": map[string]interface{}{
						"initial_master_nodes": []string{"a", "b"},
					},
				},
			},
			want: ElasticsearchSettings{
				Cluster: ClusterSettings{
					InitialMasterNodes: []string{"a", "b"},
				},
			},
			wantErr: false,
		},
		{
			name: "happy path",
			args: &commonv1.Config{
				Data: map[string]interface{}{
					"node": map[string]interface{}{
						"master": false,
						"data":   true,
					},
					"cluster": map[string]interface{}{
						"initial_master_nodes": []string{"a", "b"},
					},
				},
			},
			want: ElasticsearchSettings{
				Node: &escommon.Node{
					Master: ptr.To[bool](false),
					Data:   ptr.To[bool](true),
				},
				Cluster: ClusterSettings{
					InitialMasterNodes: []string{"a", "b"},
				},
			},
			wantErr: false,
		},
		{
			name: "happy path with node roles",
			args: &commonv1.Config{
				Data: map[string]interface{}{
					"node": map[string]interface{}{
						"roles": []string{"master", "data"},
					},
					"cluster": map[string]interface{}{
						"initial_master_nodes": []string{"a", "b"},
					},
				},
			},
			want: ElasticsearchSettings{
				Node: &escommon.Node{
					Roles: []string{"master", "data"},
				},
				Cluster: ClusterSettings{
					InitialMasterNodes: []string{"a", "b"},
				},
			},
			wantErr: false,
		},
		{
			name:    "Unpack is nil safe",
			args:    nil,
			want:    ElasticsearchSettings{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ElasticsearchSettings{}
			err := UnpackConfig(tt.args, ver, &got)
			if tt.wantErr {
				require.Error(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
