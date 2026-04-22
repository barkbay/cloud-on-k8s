// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"strings"

	"gopkg.in/yaml.v3"

	common "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
)

// RemoveKey returns a new CanonicalConfig with the given dotted key removed.
//
// ucfg (the library backing CanonicalConfig) does not expose a key-deletion
// primitive, so this function round-trips the config through YAML: render to
// bytes, unmarshal to a map, delete the path, rebuild. Callers that need to
// remove several keys should accept the cost of multiple round-trips or
// batch their deletions into a single post-processing step.
//
// The function is used today by the stateless driver to implement
// "user-wins-for-lists" semantics on top of CanonicalConfig.MergeWith (see
// applyUserConfigOverrides in the stateless package). It is deliberately
// placed in the settings package so the same mechanism remains available if
// another subsystem needs to punch a hole in a baseline before merging.
func RemoveKey(cfg CanonicalConfig, key string) (CanonicalConfig, error) {
	rendered, err := cfg.Render()
	if err != nil {
		return CanonicalConfig{}, err
	}
	var data map[string]any
	if err := yaml.Unmarshal(rendered, &data); err != nil {
		return CanonicalConfig{}, err
	}

	deleteNestedKey(data, strings.Split(key, "."))

	rebuilt, err := common.NewCanonicalConfigFrom(data)
	if err != nil {
		return CanonicalConfig{}, err
	}
	return CanonicalConfig{rebuilt}, nil
}

// deleteNestedKey removes a key from a nested map following the path segments.
// Empty parent maps are cleaned up after deletion to avoid null-valued nodes in YAML output.
func deleteNestedKey(m map[string]any, path []string) {
	if len(path) == 0 {
		return
	}
	if len(path) == 1 {
		delete(m, path[0])
		return
	}
	next, ok := m[path[0]].(map[string]any)
	if !ok {
		return
	}
	deleteNestedKey(next, path[1:])
	if len(next) == 0 {
		delete(m, path[0])
	}
}
