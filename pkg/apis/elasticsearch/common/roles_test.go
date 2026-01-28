// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

var (
	// roles that are applied by default (everything except voting_only)
	defaultRoles = []NodeRole{
		DataColdRole,
		DataContentRole,
		DataHotRole,
		DataRole,
		DataWarmRole,
		IngestRole,
		MLRole,
		MasterRole,
		RemoteClusterClientRole,
		TransformRole,
	}

	allRoles = append([]NodeRole{VotingOnlyRole}, defaultRoles...)
)

func TestNode_HasRole(t *testing.T) {
	testCases := []struct {
		name      string
		node      *Node
		wantRoles []NodeRole
	}{
		{
			name:      "master and data",
			node:      &Node{Roles: []string{"master", "data"}},
			wantRoles: []NodeRole{MasterRole, DataContentRole, DataRole, DataHotRole, DataWarmRole, DataColdRole, DataFrozenRole},
		},
		{
			name:      "master and data_content",
			node:      &Node{Roles: []string{"master", "data_content"}},
			wantRoles: []NodeRole{MasterRole, DataContentRole},
		},
		{
			name:      "data_hot and data_warm only",
			node:      &Node{Roles: []string{"data_hot", "data_warm"}},
			wantRoles: []NodeRole{DataHotRole, DataWarmRole},
		},
		{
			name:      "node.roles (ingest only)",
			node:      &Node{Roles: []string{"ingest"}},
			wantRoles: []NodeRole{IngestRole},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wantRolesSet := make(map[NodeRole]struct{}, len(tc.wantRoles))

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

func TestNode_IsConfiguredWithRole(t *testing.T) {
	testCases := []struct {
		name      string
		node      *Node
		wantRoles []NodeRole
	}{
		{
			name:      "nil node",
			wantRoles: defaultRoles,
		},
		{
			name:      "empty node",
			node:      &Node{},
			wantRoles: defaultRoles,
		},
		{
			name: "node role attributes (all)",
			node: &Node{
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
			node: &Node{
				Data: ptr.To[bool](false),
			},
			wantRoles: []NodeRole{IngestRole, MLRole, MasterRole, RemoteClusterClientRole},
		},
		{
			name: "node role attributes (ingest only)",
			node: &Node{
				Master:     ptr.To[bool](false),
				Data:       ptr.To[bool](false),
				Ingest:     ptr.To[bool](true),
				ML:         ptr.To[bool](false),
				Transform:  ptr.To[bool](false),
				VotingOnly: ptr.To[bool](false),
			},
			wantRoles: []NodeRole{IngestRole, RemoteClusterClientRole},
		},
		{
			name: "mixed node.roles and node role attributes",
			node: &Node{
				Master:     ptr.To[bool](false),
				Data:       ptr.To[bool](false),
				Ingest:     ptr.To[bool](true),
				ML:         ptr.To[bool](false),
				Transform:  ptr.To[bool](false),
				VotingOnly: ptr.To[bool](false),
				Roles:      []string{"master"},
			},
			wantRoles: []NodeRole{MasterRole},
		},
		{
			name: "node.roles (all)",
			node: &Node{
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
			node:      &Node{Roles: []string{"master", "data"}},
			wantRoles: []NodeRole{MasterRole, DataRole},
		},
		{
			name:      "node.roles (ingest only)",
			node:      &Node{Roles: []string{"ingest"}},
			wantRoles: []NodeRole{IngestRole},
		},
		{
			name: "node.roles (no roles)",
			node: &Node{Roles: []string{}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wantRolesSet := make(map[NodeRole]struct{}, len(tc.wantRoles))

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

func TestNode_CanContainData(t *testing.T) {
	testCases := []struct {
		name     string
		node     *Node
		expected bool
	}{
		{
			name:     "nil node (defaults to all roles)",
			node:     nil,
			expected: true,
		},
		{
			name:     "data role",
			node:     &Node{Roles: []string{"data"}},
			expected: true,
		},
		{
			name:     "data_hot role",
			node:     &Node{Roles: []string{"data_hot"}},
			expected: true,
		},
		{
			name:     "data_warm role",
			node:     &Node{Roles: []string{"data_warm"}},
			expected: true,
		},
		{
			name:     "data_cold role",
			node:     &Node{Roles: []string{"data_cold"}},
			expected: true,
		},
		{
			name:     "data_frozen role",
			node:     &Node{Roles: []string{"data_frozen"}},
			expected: true,
		},
		{
			name:     "data_content role",
			node:     &Node{Roles: []string{"data_content"}},
			expected: true,
		},
		{
			name:     "master only",
			node:     &Node{Roles: []string{"master"}},
			expected: false,
		},
		{
			name:     "ingest only",
			node:     &Node{Roles: []string{"ingest"}},
			expected: false,
		},
		{
			name:     "ml only",
			node:     &Node{Roles: []string{"ml"}},
			expected: false,
		},
		{
			name:     "coordinating only (empty roles)",
			node:     &Node{Roles: []string{}},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.node.CanContainData()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestNode_DependsOn(t *testing.T) {
	testCases := []struct {
		name     string
		node     *Node
		other    *Node
		expected bool
	}{
		{
			name:     "hot depends on warm",
			node:     &Node{Roles: []string{"data_hot"}},
			other:    &Node{Roles: []string{"data_warm"}},
			expected: true,
		},
		{
			name:     "hot depends on cold",
			node:     &Node{Roles: []string{"data_hot"}},
			other:    &Node{Roles: []string{"data_cold"}},
			expected: true,
		},
		{
			name:     "hot depends on frozen",
			node:     &Node{Roles: []string{"data_hot"}},
			other:    &Node{Roles: []string{"data_frozen"}},
			expected: true,
		},
		{
			name:     "warm depends on cold",
			node:     &Node{Roles: []string{"data_warm"}},
			other:    &Node{Roles: []string{"data_cold"}},
			expected: true,
		},
		{
			name:     "warm depends on frozen",
			node:     &Node{Roles: []string{"data_warm"}},
			other:    &Node{Roles: []string{"data_frozen"}},
			expected: true,
		},
		{
			name:     "cold depends on frozen",
			node:     &Node{Roles: []string{"data_cold"}},
			other:    &Node{Roles: []string{"data_frozen"}},
			expected: true,
		},
		{
			name:     "frozen has no dependency",
			node:     &Node{Roles: []string{"data_frozen"}},
			other:    &Node{Roles: []string{"data_hot"}},
			expected: false,
		},
		{
			name:     "content has no dependency",
			node:     &Node{Roles: []string{"data_content"}},
			other:    &Node{Roles: []string{"data_hot"}},
			expected: false,
		},
		{
			name:     "non-master does not depend on master (avoiding deadlock)",
			node:     &Node{Roles: []string{"data_hot"}},
			other:    &Node{Roles: []string{"master", "data_warm"}},
			expected: false,
		},
		{
			name:     "master hot depends on non-master warm",
			node:     &Node{Roles: []string{"master", "data_hot"}},
			other:    &Node{Roles: []string{"data_warm"}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.node.DependsOn(tc.other)
			require.Equal(t, tc.expected, result)
		})
	}
}
