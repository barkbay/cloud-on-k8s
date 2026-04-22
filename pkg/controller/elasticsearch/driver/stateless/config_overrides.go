// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	common "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
)

// statelessListValuedDefaults lists the dotted-path keys for which
// settings.NewStatelessConfig injects a LIST-valued default AND for which a
// user-supplied value on a NodeSet.Config must REPLACE — not be appended
// to — the tier default.
//
// Today this is just node.roles. discovery.seed_hosts is technically
// list-valued too, but the stateless baseline sets it to an empty list, so
// append-semantics and replace-semantics are observationally identical;
// including it here would be pure ceremony and is intentionally skipped.
//
// Add a key here if and only if a new list-valued stateless default is
// introduced AND "user value replaces default" is the intended contract for
// that key.
var statelessListValuedDefaults = []string{esv1.NodeRoles}

// applyUserConfigOverrides merges the NodeSet-level user-supplied
// configuration on top of the stateless baseline and returns the merged
// CanonicalConfig. Semantics:
//
//   - Scalars: user value wins (standard ucfg merge behaviour).
//   - Maps: keys are merged, user value wins on overlapping leaves.
//   - Lists in statelessListValuedDefaults: user value REPLACES the stateless
//     default (this is the extra guarantee this helper adds).
//   - Any other list: standard MergeWith behaviour (ucfg.AppendValues, i.e.
//     user values are appended to the baseline's). Not exercised today since
//     the stateless baseline's only user-facing list is node.roles.
//
// # Why this helper exists
//
// CanonicalConfig.MergeWith is configured project-wide with
// ucfg.AppendValues, which APPENDS list values instead of replacing them.
// Without this helper, a user setting node.roles on a NodeSet would end up
// with the UNION of tier defaults and their own value, not the override
// they asked for.
//
// Example — index-tier NodeSet with user config { node.roles: [master] }:
//
//	stateless baseline : node.roles = [master, index, ingest, remote_cluster_client]
//	user config        : node.roles = [master]
//
//	without helper (ucfg append): node.roles = [master, index, ingest, remote_cluster_client, master]
//	with    helper (user wins) : node.roles = [master]
//
// # How the workaround works
//
// For each key in statelessListValuedDefaults:
//   - If the user has NOT set it, the baseline keeps its tier-derived value.
//   - If the user HAS set it, we strip the key from the baseline before
//     merging (via settings.RemoveKey). There is then nothing for ucfg to
//     append to, and the user-supplied list becomes the sole value.
//
// Scalars and untouched keys flow through the ordinary MergeWith path.
//
// # Contract for callers
//
// baseline is mutated in place (matching the existing in-place semantics of
// CanonicalConfig.MergeWith); the returned value is the same pointer for
// callers that prefer the functional-style signature.
func applyUserConfigOverrides(baseline *settings.CanonicalConfig, userCfg *common.CanonicalConfig) (*settings.CanonicalConfig, error) {
	if baseline == nil {
		return nil, nil
	}
	if userCfg == nil {
		return baseline, nil
	}
	for _, key := range statelessListValuedDefaults {
		if len(userCfg.HasKeys([]string{key})) == 0 {
			continue
		}
		cleaned, err := settings.RemoveKey(*baseline, key)
		if err != nil {
			return nil, err
		}
		*baseline = cleaned
	}
	if err := baseline.MergeWith(userCfg); err != nil {
		return nil, err
	}
	return baseline, nil
}
