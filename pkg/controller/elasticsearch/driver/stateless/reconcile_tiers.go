// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/configmap"
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

	// Track reconciled immutable resource names for GC
	reconciledSecretNames := sets.New[string]()
	reconciledConfigMapNames := sets.New[string]()

	// Reconcile the immutable scripts ConfigMap once (shared across all tiers)
	immutableScriptsConfigMapName, err := configmap.ReconcileStatelessImmutableScriptsConfigMap(ctx, d.Client, d.ES, meta)
	if err != nil {
		return results.WithError(err)
	}
	reconciledConfigMapNames.Insert(immutableScriptsConfigMapName)

	for _, tierResources := range buildTierResources {
		// Reconcile the immutable config secret first
		immutableSecretName, err := settings.ReconcileStatelessImmutableConfig(
			ctx,
			d.Client,
			d.ES,
			tierResources.deployment.Name,
			tierResources.config,
			tierResources.meta,
			tierResources.operatorPrivilegesSettings,
		)
		if err != nil {
			results.WithError(err)
			continue
		}
		reconciledSecretNames.Insert(immutableSecretName)

		// Patch the deployment volumes to reference the immutable resources
		settings.PatchStatelessConfigVolumes(tierResources.deployment, immutableSecretName)
		configmap.PatchStatelessScriptsVolumes(tierResources.deployment, immutableScriptsConfigMapName)

		// Recompute template hash after patching volumes
		expected := WithTemplateHash(*tierResources.deployment)

		// Then reconcile the Deployment
		reconciled := &appsv1.Deployment{}
		results.WithError(reconciler.ReconcileResource(reconciler.Params{
			Context:    ctx,
			Client:     d.Client,
			Owner:      &d.ES,
			Expected:   &expected,
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

	// Garbage collect unreferenced immutable config resources
	if err := settings.GCStatelessImmutableConfigSecrets(ctx, d.Client, d.ES, reconciledSecretNames); err != nil {
		results.WithError(err)
	}
	if err := configmap.GCStatelessImmutableScriptsConfigMaps(ctx, d.Client, d.ES, reconciledConfigMapNames); err != nil {
		results.WithError(err)
	}

	return results
}
