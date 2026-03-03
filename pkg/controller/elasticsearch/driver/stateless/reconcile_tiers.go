// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/configmap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/maps"
)

func (d *Driver) reconcileTiers(
	ctx context.Context,
	expectations *expectations.Expectations,
	meta metadata.Metadata,
	keystoreResources *keystore.Resources,
) *reconciler.Results {
	results := reconciler.NewResult(ctx)

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

	revisions, err := immutableconfig.NewRevisions(d.Client, &d.ES, d.ES.Namespace).
		WithGCLabels(client.MatchingLabels{
			label.ClusterNameLabelName:          d.ES.Name,
			immutableconfig.ConfigTypeLabelName: immutableconfig.ConfigTypeImmutable,
		}).
		WithReplicaSetLabels(client.MatchingLabels{
			label.ClusterNameLabelName: d.ES.Name,
		}).
		Build()
	if err != nil {
		return results.WithError(err)
	}
	// Each tier's immutable config data is stored in a content-addressed Secret.
	secretRevision := revisions.ForSecretVolume(settings.ConfigVolumeName)
	// Scripts are stored in a content-addressed ConfigMap shared across all tiers.
	cmRevision := revisions.ForConfigMapVolume(volume.ScriptsVolumeName)

	// Reconcile the immutable scripts ConfigMap once (shared across all tiers)
	scriptsCM, err := configmap.BuildStatelessImmutableScriptsConfigMap(d.ES, meta)
	if err != nil {
		return results.WithError(err)
	}
	scriptsName, err := cmRevision.Reconcile(ctx, &scriptsCM)
	if err != nil {
		return results.WithError(err)
	}

	for _, tierResources := range buildTierResources {
		secret, err := settings.BuildStatelessImmutableConfigSecret(
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

		secretName, err := secretRevision.Reconcile(ctx, &secret)
		if err != nil {
			results.WithError(err)
			continue
		}

		secretRevision.PatchVolumes(tierResources.deployment.Spec.Template.Spec.Volumes, secretName)
		cmRevision.PatchVolumes(tierResources.deployment.Spec.Template.Spec.Volumes, scriptsName)

		expected := WithTemplateHash(*tierResources.deployment)

		reconciled := &appsv1.Deployment{}
		results.WithError(reconciler.ReconcileResource(reconciler.Params{
			Context:    ctx,
			Client:     d.Client,
			Owner:      &d.ES,
			Expected:   &expected,
			Reconciled: reconciled,
			NeedsUpdate: func() bool {
				return !maps.IsSubset(expected.Labels, reconciled.Labels) ||
					!maps.IsSubset(expected.Annotations, reconciled.Annotations) ||
					!(expected.Labels[hash.TemplateHashLabelName] == reconciled.Labels[hash.TemplateHashLabelName])
			},
			UpdateReconciled: func() {
				reconciled.Labels = maps.Merge(reconciled.Labels, expected.Labels)
				reconciled.Annotations = maps.Merge(reconciled.Annotations, expected.Annotations)
				reconciled.Spec = expected.Spec
			},
			PostUpdate: func() {
				if expectations != nil {
					expectations.ExpectGeneration(reconciled)
				}
			},
		}))
	}

	if err := immutableconfig.GCAll(ctx, secretRevision, cmRevision); err != nil {
		results.WithError(err)
	}

	return results
}
