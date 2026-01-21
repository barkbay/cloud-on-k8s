// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1

import (
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/optional"
)

var (
	RemoteClusterAPIKeysMinVersion = version.MinFor(8, 10, 0)
)

// SupportsRemoteClusterAPIKeys returns true if this cluster supports connecting to a remote cluster using API keys.
func (es *Elasticsearch) SupportsRemoteClusterAPIKeys() (*optional.Bool, error) {
	if es == nil {
		return nil, nil
	}
	if es.Status.Version == "" {
		// This cluster is not reconciled yet.
		return nil, nil
	}
	esVersion, err := version.Parse(es.Status.Version)
	if err != nil {
		return nil, err
	}
	return optional.NewBool(esVersion.GTE(RemoteClusterAPIKeysMinVersion)), nil
}

// HasRemoteClusterAPIKey returns true if this cluster is connecting to a remote cluster using API keys.
func (es *Elasticsearch) HasRemoteClusterAPIKey() bool {
	if es == nil {
		return false
	}
	for _, remoteCluster := range es.Spec.RemoteClusters {
		if remoteCluster.APIKey != nil {
			return true
		}
	}
	return false
}

// RemoteClustersCount returns the number of remote clusters using only certificates and API keys.
func (es *Elasticsearch) RemoteClustersCount() (int32, int32) {
	if es == nil {
		return 0, 0
	}
	var withoutAPIKeys, withAPIKeys int32
	for _, remoteCLuster := range es.Spec.RemoteClusters {
		if remoteCLuster.APIKey == nil {
			withoutAPIKeys++
			continue
		}
		withAPIKeys++
	}
	return withoutAPIKeys, withAPIKeys
}

// RemoteClusterAPIKey is an alias to the common type for interface compatibility.
type RemoteClusterAPIKey = escommon.RemoteClusterAPIKey

// RemoteClusterAccess is an alias to the common type for interface compatibility.
type RemoteClusterAccess = escommon.RemoteClusterAccess

// Search is an alias to the common type for interface compatibility.
type Search = escommon.Search

// FieldSecurity is an alias to the common type for interface compatibility.
type FieldSecurity = escommon.FieldSecurity

// Replication is an alias to the common type for interface compatibility.
type Replication = escommon.Replication
