// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"

	v1 "k8s.io/api/apps/v1"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/maps"
)

func (d *Driver) reconcileTiers(
	ctx context.Context,
	expectations *expectations.Expectations,
	meta metadata.Metadata,
	keystoreResources *keystore.Resources,
) *reconciler.Results {
	results := reconciler.NewResult(ctx)

	// check if actual Deployments match our expectations before applying any change
	ok, reason, err := d.expectationsSatisfied(ctx)
	if err != nil {
		return results.WithError(err)
	}
	if !ok {
		return results.WithReconciliationState(reconciler.Requeue.WithReason(reason))
	}

	ver, err := version.Parse(d.ES.Spec.Version)
	if err != nil {
		return results.WithError(err)
	}

	buildTierResources, err := d.buildTierResources(ctx, meta, ver, keystoreResources)
	if err != nil {
		return results.WithError(err)
	}

	for _, tierResources := range buildTierResources {
		// Reconcile the config first
		if err := settings.ReconcileConfig(
			ctx,
			d.Client,
			&d.ES,
			tierResources.deployment.Name,
			tierResources.config,
			tierResources.meta,
		); err != nil {
			results.WithError(err)
			continue
		}

		// Then reconcile the Deployment
		reconciled := &v1.Deployment{}
		expected := tierResources.deployment
		results.WithError(reconciler.ReconcileResource(reconciler.Params{
			Context:    ctx,
			Client:     d.Client,
			Owner:      &d.ES,
			Expected:   expected,
			Reconciled: reconciled,
			NeedsUpdate: func() bool {
				// expected labels or annotations not there
				return !maps.IsSubset(expected.Labels, reconciled.Labels) ||
					!maps.IsSubset(expected.Annotations, reconciled.Annotations) ||
					// different spec
					!(expected.Labels[hash.TemplateHashLabelName] == reconciled.Labels[hash.TemplateHashLabelName])
			},
			UpdateReconciled: func() {
				// set expected annotations and labels, but don't remove existing ones
				// that may have been defaulted or set by a user/admin on the existing resource
				reconciled.Labels = maps.Merge(reconciled.Labels, expected.Labels)
				reconciled.Annotations = maps.Merge(reconciled.Annotations, expected.Annotations)
				// overwrite the spec but leave the status intact
				reconciled.Spec = expected.Spec
			},
			PostUpdate: func() {
				if expectations != nil {
					// expect the reconciled StatefulSet to be there in the cache for next reconciliations,
					// to prevent assumptions based on the wrong replica count
					expectations.ExpectGeneration(reconciled)
				}
			},
		}))
	}
	return results
}
