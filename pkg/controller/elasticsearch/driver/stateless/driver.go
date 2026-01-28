// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package stateless implements the stateless Elasticsearch driver.
package stateless

import (
	"context"

	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	commondriver "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/shared"
)

// Driver is the stateless Elasticsearch driver implementation.
type Driver struct {
	driver.BaseDriver
	ES v1alpha1.ElasticsearchStateless
}

// NewDriver returns a new stateless driver implementation.
// The ess parameter is the concrete ElasticsearchStateless type for stateless-specific operations,
// while parameters.ES holds the interface for shared reconciliation code.
func NewDriver(parameters driver.Parameters, ess v1alpha1.ElasticsearchStateless) driver.Driver {
	return &Driver{
		BaseDriver: driver.BaseDriver{Parameters: parameters},
		ES:         ess,
	}
}

var _ commondriver.Interface = &Driver{}

// Reconcile fulfills the Driver interface and reconciles the cluster resources.
func (d *Driver) Reconcile(ctx context.Context) *reconciler.Results {
	// One call does all shared work
	shared, results := shared.ReconcileSharedResources(ctx, d, d.Parameters)
	if results.HasError() {
		return results
	}
	defer shared.ESClient.Close()

	// reconcile Deployments and nodes configuration
	return results.WithResults(d.reconcileTiers(ctx, d.Expectations, shared.Meta, shared.KeystoreResources))
}
