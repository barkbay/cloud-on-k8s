// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless/fixtures"
)

// newDriver wraps fixtures.NewDriverParameters into a *Driver. It lives in
// the stateless test package (rather than fixtures) because the fixtures
// subpackage cannot import the stateless package without creating an import
// cycle with the in-package test files.
func newDriver(t *testing.T, es esv1.Elasticsearch, seed ...client.Object) *Driver {
	t.Helper()
	return &Driver{BaseDriver: driver.BaseDriver{Parameters: fixtures.NewDriverParameters(t, es, seed...)}}
}

// renderedNodeRoles extracts the node.roles list from the rendered
// elasticsearch.yml of the given nodeSetResources. It lives in the main
// stateless test package (rather than the fixtures subpackage) because
// nodeSetResources is unexported.
func renderedNodeRoles(t *testing.T, res nodeSetResources) []string {
	t.Helper()
	raw, err := res.config.Render()
	require.NoError(t, err)

	type shape struct {
		Node struct {
			Roles []string `yaml:"roles"`
		} `yaml:"node"`
	}
	out := shape{}
	require.NoError(t, yaml.Unmarshal(raw, &out))
	return out.Node.Roles
}
