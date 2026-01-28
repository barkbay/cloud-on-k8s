// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package common

import (
	"k8s.io/utils/ptr"

	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/stringsutil"
)

// NodeRole represents an Elasticsearch node role.
type NodeRole string

const (
	CoordinatingRole        NodeRole = ""
	DataColdRole            NodeRole = "data_cold"
	DataContentRole         NodeRole = "data_content"
	DataFrozenRole          NodeRole = "data_frozen"
	DataHotRole             NodeRole = "data_hot"
	DataRole                NodeRole = "data"
	DataWarmRole            NodeRole = "data_warm"
	IngestRole              NodeRole = "ingest"
	MLRole                  NodeRole = "ml"
	MasterRole              NodeRole = "master"
	RemoteClusterClientRole NodeRole = "remote_cluster_client"
	TransformRole           NodeRole = "transform"
	VotingOnlyRole          NodeRole = "voting_only"
)

// Stateless specific roles.
const (
	IndexRole  NodeRole = "index"
	SearchRole NodeRole = "search"
)

// Node configuration key constants.
const (
	NodeData                = "node.data"
	NodeIngest              = "node.ingest"
	NodeMaster              = "node.master"
	NodeML                  = "node.ml"
	NodeTransform           = "node.transform"
	NodeVotingOnly          = "node.voting_only"
	NodeRemoteClusterClient = "node.remote_cluster_client"
	NodeRoles               = "node.roles"
)

// Node is the node section in elasticsearch.yml.
type Node struct {
	Master              *bool    `config:"master"`
	Data                *bool    `config:"data"`
	Ingest              *bool    `config:"ingest"`
	ML                  *bool    `config:"ml"`
	Transform           *bool    `config:"transform"`             // available as of 7.7.0
	RemoteClusterClient *bool    `config:"remote_cluster_client"` // available as of 7.7.0
	Roles               []string `config:"roles"`                 // available as of 7.9.0, takes priority over the other fields if non-nil
	VotingOnly          *bool    `config:"voting_only"`           // available as of 7.3.0

	// Serverless
	Index  *bool `config:"index"`
	Search *bool `config:"search"`
}

// CanContainData returns true if a node can contain data, it returns false otherwise.
func (n *Node) CanContainData() bool {
	return n.HasRole(DataRole) ||
		n.HasRole(DataHotRole) ||
		n.HasRole(DataWarmRole) ||
		n.HasRole(DataColdRole) ||
		n.HasRole(DataFrozenRole) ||
		n.HasRole(DataContentRole)
}

// HasRole returns true if the node runs with the given role.
func (n *Node) HasRole(role NodeRole) bool {
	switch role {
	case DataContentRole, DataHotRole, DataWarmRole, DataColdRole, DataFrozenRole:
		return n.IsConfiguredWithRole(DataRole) || n.IsConfiguredWithRole(role)
	default:
		return n.IsConfiguredWithRole(role)
	}
}

// DependsOn returns true if a tier should be upgraded before another one.
func (n *Node) DependsOn(other *Node) bool {
	switch {
	case !n.HasRole(MasterRole) && other.HasRole(MasterRole):
		// other might be a dependency, but it is also a master node. We don't want to enter a deadlock where other is
		// the last master node, while the candidate is not and must be upgraded first.
		return false
	case n.HasRole(DataHotRole):
		// hot tier must be upgraded after warm, cold and frozen
		return other.HasRole(DataWarmRole) || other.HasRole(DataColdRole) || other.HasRole(DataFrozenRole)
	case n.HasRole(DataWarmRole):
		// warm tier must be upgraded after cold and frozen
		return other.HasRole(DataColdRole) || other.HasRole(DataFrozenRole)
	case n.HasRole(DataColdRole):
		// cold tier must be upgraded after frozen
		return other.HasRole(DataFrozenRole)
	}
	// frozen and content have no dependency
	return false
}

// IsConfiguredWithRole returns true if the node has the given role in its configuration.
func (n *Node) IsConfiguredWithRole(role NodeRole) bool {
	if n == nil {
		// Nodes have all the roles by default except for the voting_only role.
		return role != VotingOnlyRole
	}

	if n.Roles != nil {
		return stringsutil.StringInSlice(string(role), n.Roles)
	}

	switch role {
	case DataRole:
		return ptr.Deref(n.Data, true)
	case DataFrozenRole, DataColdRole, DataContentRole, DataHotRole, DataWarmRole:
		// These roles should really be defined in node.roles. Since they were not, assume they are enabled unless node.data is set to false.
		return ptr.Deref(n.Data, true)
	case IngestRole:
		return ptr.Deref(n.Ingest, true)
	case MLRole:
		return ptr.Deref(n.ML, true)
	case MasterRole:
		return ptr.Deref(n.Master, true)
	case RemoteClusterClientRole:
		return ptr.Deref(n.RemoteClusterClient, true)
	case TransformRole:
		// all data nodes are transform nodes by default as well.
		return ptr.Deref(n.Transform, n.IsConfiguredWithRole(DataRole))
	case VotingOnlyRole:
		return ptr.Deref(n.VotingOnly, false)
	case CoordinatingRole:
		return n.Roles != nil && len(n.Roles) == 0
	case IndexRole:
		return ptr.Deref(n.Index, true)
	case SearchRole:
		return ptr.Deref(n.Search, true)
	}

	// This point should never be reached. The default is to assume that a node has all roles except voting_only.
	return role != VotingOnlyRole
}

// DeepCopyInto is a deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Node) DeepCopyInto(out *Node) {
	*out = *in
	if in.Master != nil {
		in, out := &in.Master, &out.Master
		*out = new(bool)
		**out = **in
	}
	if in.Data != nil {
		in, out := &in.Data, &out.Data
		*out = new(bool)
		**out = **in
	}
	if in.Ingest != nil {
		in, out := &in.Ingest, &out.Ingest
		*out = new(bool)
		**out = **in
	}
	if in.ML != nil {
		in, out := &in.ML, &out.ML
		*out = new(bool)
		**out = **in
	}
	if in.Transform != nil {
		in, out := &in.Transform, &out.Transform
		*out = new(bool)
		**out = **in
	}
	if in.RemoteClusterClient != nil {
		in, out := &in.RemoteClusterClient, &out.RemoteClusterClient
		*out = new(bool)
		**out = **in
	}
	if in.Roles != nil {
		in, out := &in.Roles, &out.Roles
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.VotingOnly != nil {
		in, out := &in.VotingOnly, &out.VotingOnly
		*out = new(bool)
		**out = **in
	}
}

// DeepCopy is a deepcopy function, copying the receiver, creating a new Node.
func (in *Node) DeepCopy() *Node {
	if in == nil {
		return nil
	}
	out := new(Node)
	in.DeepCopyInto(out)
	return out
}
