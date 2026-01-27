// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package shared

import (
	"k8s.io/client-go/tools/record"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	commonlicense "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/observer"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/reconcile"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// Parameters contains common parameters for all driver implementations.
// These are shared between stateful and stateless drivers.
type Parameters struct {
	// OperatorParameters contain global parameters about the operator.
	OperatorParameters operator.Parameters

	// SupportedVersions verifies whether we can support upgrading from the current pods.
	SupportedVersions version.MinMaxVersion

	// Version is the version of Elasticsearch we want to reconcile towards.
	Version version.Version

	// Client is used to access the Kubernetes API.
	Client   k8s.Client
	Recorder record.EventRecorder

	// LicenseChecker is used for some features to check if an appropriate license is setup
	LicenseChecker commonlicense.Checker

	// State holds the accumulated state during the reconcile loop
	ReconcileState *reconcile.State

	// Observers that observe es clusters state.
	Observers *observer.Manager

	// DynamicWatches are handles to currently registered dynamic watches.
	DynamicWatches watches.DynamicWatches

	// Expectations control some expectations set on resources in the cache, in order to
	// avoid doing certain operations if the cache hasn't seen an up-to-date resource yet.
	Expectations *expectations.Expectations
}
