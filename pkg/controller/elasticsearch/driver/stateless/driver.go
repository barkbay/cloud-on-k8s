// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package stateless implements the stateless Elasticsearch driver.
package stateless

import (
	"context"

	"k8s.io/client-go/tools/record"

	commondriver "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/shared"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// Driver is the stateless Elasticsearch driver implementation.
type Driver struct {
	Parameters
}

// NewDriver returns a new stateless driver implementation.
func NewDriver(parameters Parameters) *Driver {
	return &Driver{Parameters: parameters}
}

var _ commondriver.Interface = &Driver{}

// K8sClient returns the Kubernetes client. Implements commondriver.Interface.
func (d *Driver) K8sClient() k8s.Client {
	return d.Client
}

// DynamicWatches returns the dynamic watches. Implements commondriver.Interface.
func (d *Driver) DynamicWatches() watches.DynamicWatches {
	return d.Parameters.DynamicWatches
}

// Recorder returns the event recorder. Implements commondriver.Interface.
func (d *Driver) Recorder() record.EventRecorder {
	return d.Parameters.Recorder
}

// Reconcile fulfills the Driver interface and reconciles the cluster resources.
func (d *Driver) Reconcile(ctx context.Context) *reconciler.Results {
	// One call does all shared work
	sharedState, results := shared.ReconcileSharedResources(ctx, d, &d.ES, d.Parameters.Parameters)
	if results.HasError() {
		return results
	}
	defer sharedState.ESClient.Close()

	// STATELESS-SPECIFIC: Future implementation will go here
	// e.g., d.reconcileStatelessResources(ctx, sharedState)

	return results
}
