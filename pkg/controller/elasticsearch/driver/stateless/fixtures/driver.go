// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package fixtures

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	commonoperator "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// NewDriverParameters returns a driver.Parameters value populated with
// everything reconcileNodeSets needs: a fake client seeded with the
// provided objects, the fixture's DefaultTestVersion, IPv4, and a fresh
// Expectations tracker scoped to Deployments.
//
// It intentionally returns driver.Parameters (not *stateless.Driver) so the
// fixtures subpackage does not have to import stateless, which would create
// an import cycle with the in-package test files. The stateless test
// package wraps this into a *Driver via a tiny helper.
func NewDriverParameters(t *testing.T, es esv1.Elasticsearch, seed ...client.Object) driver.Parameters {
	t.Helper()
	c := k8s.NewFakeClient(seed...)
	return driver.Parameters{
		Client:             c,
		ES:                 es,
		Version:            DefaultTestVersion,
		Expectations:       expectations.NewExpectations(c, &appsv1.Deployment{}),
		OperatorParameters: commonoperator.Parameters{IPFamily: corev1.IPv4Protocol},
	}
}
