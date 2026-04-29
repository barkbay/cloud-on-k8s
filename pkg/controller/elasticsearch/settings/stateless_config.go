// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	common "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
)

// DefaultTierRoles maps each stateless tier to its default node.roles in the
// single-tier (no dedicated master) topology. The effective roles for a given
// cluster are computed by TierRolesFor, which may trim this default when a
// dedicated master tier is present.
var DefaultTierRoles = map[esv1.StatelessTier][]esv1.NodeRole{
	esv1.IndexTier:  {esv1.MasterRole, esv1.IndexRole, esv1.IngestRole, esv1.RemoteClusterClientRole},
	esv1.SearchTier: {esv1.SearchRole, esv1.RemoteClusterClientRole, esv1.TransformRole},
	esv1.MasterTier: {esv1.MasterRole, esv1.RemoteClusterClientRole},
	esv1.MLTier:     {esv1.MLRole, esv1.RemoteClusterClientRole},
}

// HasDedicatedMasterTier reports whether the spec declares a master tier
// NodeSet with a positive replica count. When true, the index tier sheds the
// master role so masters are isolated on dedicated pods.
func HasDedicatedMasterTier(es esv1.Elasticsearch) bool {
	for _, ns := range es.Spec.NodeSets {
		tier, err := ns.ResolvedTier()
		if err != nil {
			continue
		}
		if tier == esv1.MasterTier && ns.Count > 0 {
			return true
		}
	}
	return false
}

// TierRolesFor returns the node.roles for a given stateless tier in the
// context of the given Elasticsearch spec. The IndexTier sheds MasterRole
// when the spec contains a non-empty MasterTier NodeSet.
//
// Role assignment is purely spec-derived. Transitional "keep master on
// index" semantics are NOT encoded here: the stateless driver preserves
// quorum by holding specific observed Deployments in place (see
// stagePriorityTier in the driver), not by rewriting roles mid-flight.
func TierRolesFor(es esv1.Elasticsearch, tier esv1.StatelessTier) ([]string, error) {
	base, ok := DefaultTierRoles[tier]
	if !ok {
		return nil, fmt.Errorf("unknown stateless tier: %s", tier)
	}
	roles := make([]esv1.NodeRole, len(base))
	copy(roles, base)

	if tier == esv1.IndexTier && HasDedicatedMasterTier(es) {
		roles = removeRole(roles, esv1.MasterRole)
	}

	result := make([]string, len(roles))
	for i, r := range roles {
		result[i] = string(r)
	}
	return result, nil
}

// TierHasMasterRole reports whether the given stateless tier carries the
// master role under the current spec. Derived from the same source of
// truth as TierRolesFor so the config, labels, and seed-host selection
// stay consistent.
func TierHasMasterRole(es esv1.Elasticsearch, tier esv1.StatelessTier) bool {
	roles, err := TierRolesFor(es, tier)
	if err != nil {
		return false
	}
	for _, r := range roles {
		if r == string(esv1.MasterRole) {
			return true
		}
	}
	return false
}

func removeRole(roles []esv1.NodeRole, target esv1.NodeRole) []esv1.NodeRole {
	out := roles[:0]
	for _, r := range roles {
		if r != target {
			out = append(out, r)
		}
	}
	return out
}

// NewStatelessConfig builds the stateless-specific Elasticsearch configuration
// for a given tier in the context of the given Elasticsearch spec. Includes
// object store settings, node roles, and stateless operational settings.
func NewStatelessConfig(es esv1.Elasticsearch, tier esv1.StatelessTier) (*CanonicalConfig, error) {
	if es.Spec.ObjectStore == nil {
		return nil, fmt.Errorf("objectStore is required for stateless mode")
	}
	objectStore := *es.Spec.ObjectStore

	roles, err := TierRolesFor(es, tier)
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
