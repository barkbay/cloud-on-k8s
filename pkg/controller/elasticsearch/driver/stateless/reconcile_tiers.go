// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/configmap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/shared"
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

	allExpectedTierResources := expectedTierResources(buildTierResources)
	groupedExpectedTierResources := [][]*TierResources{allExpectedTierResources}
	observedDeployments, err := d.observedDeployments(ctx)
	if err != nil {
		return results.WithError(err)
	}
	if d.shouldGroupDeploymentReconciliation(ctx, observedDeployments, expectedDeployments(allExpectedTierResources)) {
		groupedExpectedTierResources = groupExpectedTierResources(allExpectedTierResources)
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

	log := crlog.FromContext(ctx)
	for i, expectedResources := range groupedExpectedTierResources {
		groupReconciledDeployments := make([]*appsv1.Deployment, 0, len(expectedResources))
		for _, tierResources := range expectedResources {
			secret, err := settings.BuildStatelessImmutableConfigSecret(
				d.ES,
				tierResources.deployment.Name,
				tierResources.config,
				tierResources.meta,
				tierResources.operatorPrivilegesSettings,
			)
			if err != nil {
				return results.WithError(err)
			}

			secretName, err := secretRevision.Reconcile(ctx, &secret)
			if err != nil {
				return results.WithError(err)
			}

			secretRevision.PatchVolumes(tierResources.deployment.Spec.Template.Spec.Volumes, secretName)
			cmRevision.PatchVolumes(tierResources.deployment.Spec.Template.Spec.Volumes, scriptsName)

			expected := WithTemplateHash(*tierResources.deployment)

			reconciled := &appsv1.Deployment{}
			if err := reconciler.ReconcileResource(reconciler.Params{
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
			}); err != nil {
				return results.WithError(err)
			}
			groupReconciledDeployments = append(groupReconciledDeployments, reconciled)
		}

		// When reconciling grouped tiers, wait for each group to complete before proceeding
		// to avoid upgrading writers before readers are fully rolled out.
		if len(groupedExpectedTierResources) > 1 {
			groupPods, err := podsForTierResources(d.Client, d.ES.Namespace, expectedResources)
			if err != nil {
				return results.WithError(err)
			}
			if !isDeploymentGroupFullyReconciled(ctx, groupReconciledDeployments, groupPods) {
				msg := fmt.Sprintf(
					"Deployment group not yet complete: group %d/%d tiers=%v",
					i+1,
					len(groupedExpectedTierResources),
					tierNames(expectedResources),
				)
				log.V(1).Info(msg)
				return results.WithReconciliationState(shared.DefaultRequeue.WithReason(msg))
			}
		}
	}

	if err := immutableconfig.GCAll(ctx, secretRevision, cmRevision); err != nil {
		results.WithError(err)
	}

	return results
}

func expectedTierResources(buildTierResources map[esv1.ElasticsearchTierName]*TierResources) []*TierResources {
	expected := make([]*TierResources, 0, len(buildTierResources))
	for _, tier := range esv1.AllElasticsearchTierNames {
		tierResources, exists := buildTierResources[tier]
		if !exists {
			continue
		}
		expected = append(expected, tierResources)
	}
	return expected
}

func expectedDeployments(resources []*TierResources) []*appsv1.Deployment {
	expected := make([]*appsv1.Deployment, 0, len(resources))
	for _, tierResources := range resources {
		expected = append(expected, tierResources.deployment)
	}
	return expected
}

func groupExpectedTierResources(resources []*TierResources) [][]*TierResources {
	// Reader tier goes first to smooth mixed-version rollouts.
	search := make([]*TierResources, 0, len(resources))
	others := make([]*TierResources, 0, len(resources))
	for _, resource := range resources {
		if resource.deployment.Labels[label.TierLabelName] == string(esv1.SearchTierName) {
			search = append(search, resource)
			continue
		}
		others = append(others, resource)
	}
	return [][]*TierResources{search, others}
}

func tierNames(resources []*TierResources) []string {
	tiers := make([]string, 0, len(resources))
	for _, resource := range resources {
		tiers = append(tiers, resource.deployment.Labels[label.TierLabelName])
	}
	return tiers
}
