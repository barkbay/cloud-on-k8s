// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package stateless implements the stateless Elasticsearch driver.
package stateless

import (
	"context"

	commondriver "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/shared"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/hints"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/optional"
)

// Driver is the stateless Elasticsearch driver implementation.
type Driver struct {
	driver.BaseDriver
}

// NewDriver returns a new stateless driver implementation.
func NewDriver(parameters driver.Parameters) driver.Driver {
	return &Driver{BaseDriver: driver.BaseDriver{Parameters: parameters}}
}

var _ commondriver.Interface = &Driver{}

// Reconcile fulfills the Driver interface and reconciles the cluster resources.
func (d *Driver) Reconcile(ctx context.Context) *reconciler.Results {
	// Reconcile resources which are common to all drivers.
	shared, results := shared.ReconcileSharedResources(ctx, d, d.Parameters)
	if results.HasError() {
		return results
	}
	defer shared.ESClient.Close()

	// Stateless: service accounts are always supported (no rolling upgrade)
	d.ReconcileState.UpdateOrchestrationHints(
		d.ReconcileState.OrchestrationHints().Merge(hints.OrchestrationsHints{ServiceAccounts: optional.NewBool(true)}),
	)

	// Stateless-specific: reconcile Deployments and tiers
	// Stateless does not use the keystore init container; secure settings are applied via file settings cluster_secrets.
	return results.WithResults(d.reconcileTiers(ctx, d.Expectations, shared.Meta, nil))
}
