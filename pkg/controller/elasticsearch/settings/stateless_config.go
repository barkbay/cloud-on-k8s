// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	common "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
)

// DefaultTierRoles maps each stateless tier to its default node.roles.
var DefaultTierRoles = map[esv1.StatelessTier][]esv1.NodeRole{
	esv1.IndexTier:  {esv1.MasterRole, esv1.IndexRole, esv1.IngestRole, esv1.RemoteClusterClientRole},
	esv1.SearchTier: {esv1.SearchRole, esv1.RemoteClusterClientRole, esv1.TransformRole},
	esv1.MasterTier: {esv1.MasterRole, esv1.RemoteClusterClientRole},
	esv1.MLTier:     {esv1.MLRole, esv1.RemoteClusterClientRole},
}

// TierRoles returns the node.roles for a given stateless tier.
func TierRoles(tier esv1.StatelessTier) ([]string, error) {
	roles, ok := DefaultTierRoles[tier]
	if !ok {
		return nil, fmt.Errorf("unknown stateless tier: %s", tier)
	}
	result := make([]string, len(roles))
	for i, r := range roles {
		result[i] = string(r)
	}
	return result, nil
}

// NewStatelessConfig builds the stateless-specific Elasticsearch configuration for a given tier.
// This includes object store settings, node roles, and stateless operational settings.
func NewStatelessConfig(tier esv1.StatelessTier, objectStore esv1.ObjectStoreConfig) (*CanonicalConfig, error) {
	roles, err := TierRoles(tier)
	if err != nil {
		return nil, err
	}

	client := objectStore.Client
	if client == "" {
		client = "default"
	}
	objectStoreConfigAsMap := map[string]any{
		"enabled":             true,
		"object_store.type":   string(objectStore.Type),
		"object_store.bucket": objectStore.Bucket,
		"object_store.client": client,
	}
	if objectStore.BasePath != "" {
		objectStoreConfigAsMap["object_store.base_path"] = objectStore.BasePath
	}

	cfg := map[string]any{
		"stateless":                                        objectStoreConfigAsMap,
		esv1.NodeRoles:                                     roles,
		esv1.DiscoverySeedProviders:                        "file",
		esv1.DiscoverySeedHosts:                            []string{},
		esv1.HTTPPublishHost:                               "0", // no headless service per NodeSet in stateless mode
		"health.periodic_logger.enabled":                   "true",
		"health.periodic_logger.poll_interval":             "60s",
		"health.master_history.identity_changes_threshold": "30",
	}

	canonicalCfg := common.MustCanonicalConfig(cfg)
	return &CanonicalConfig{canonicalCfg}, nil
}
