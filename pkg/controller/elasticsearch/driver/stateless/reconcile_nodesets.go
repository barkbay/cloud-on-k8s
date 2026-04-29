// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commondeployment "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/configmap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

// reconcileNodeSets is the Plan interpreter. All orchestration decisions
// (what to push, what to keep, whether to requeue) live in plan.go as a
// pure function over (observed Deployments, ES spec). This function just
// carries out the Plan:
//
//  1. Compute the Plan for this pass.
//  2. Reconcile every NodeSet in Apply (config secret + Deployment upsert).
//  3. GC anything observed that is neither in Apply nor named in Keep.
//  4. Requeue iff the Plan is Transitioning.
//
// There is no master-tier state machine, no group loop, and no
// wait-between-groups rollout gate here: those are encoded as different
// Plan shapes returned by plan().
func (d *Driver) reconcileNodeSets(ctx context.Context, meta metadata.Metadata) *reconciler.Results {
	results := reconciler.NewResult(ctx)
	log := ulog.FromContext(ctx)

	satisfied, reason, err := d.Expectations.Satisfied()
	if err != nil {
		return results.WithError(err)
	}
	if !satisfied {
		log.Info("Expectations not yet satisfied, requeuing", "reason", reason)
		return results.WithReconciliationState(reconciler.Requeue.WithReason(reason))
	}

	plan, _, err := d.planForReconcile(ctx, meta)
	if err != nil {
		return results.WithError(err)
	}
	if plan.Reason != "" {
		log.Info("Reconciling stateless Deployments",
			"reason", plan.Reason,
			"apply", tierNames(plan.Apply),
			"keep", plan.Keep)
	}

	esNsn := k8s.ExtractNamespacedName(&d.ES)
	clusterLabels := label.NewLabels(esNsn)
	revisions, err := immutableconfig.NewRevisions(d.Client, &d.ES, d.ES.Namespace).
		WithConfigResourceSelector(client.MatchingLabels(clusterLabels)).
		WithPodTemplateSource(immutableconfig.NewReplicaSetExtractor(client.MatchingLabels(clusterLabels))).
		Build()
	if err != nil {
		return results.WithError(err)
	}
	configRevisions := revisions.ForSecretVolumes(settings.StatelessSecretVolumeClassifier)
	scriptsRevisions := revisions.ForConfigMapVolumes(settings.StatelessConfigMapVolumeClassifier)

	// Scripts ConfigMap is shared across NodeSets and is safe to reconcile
	// on every pass regardless of what Plan tells us.
	scriptsName, err := d.reconcileImmutableScripts(ctx, scriptsRevisions)
	if err != nil {
		return results.WithError(err)
	}

	if err := d.applyPlanResources(ctx, plan.Apply, configRevisions, scriptsRevisions, scriptsName); err != nil {
		return results.WithError(err)
	}

	protected := sets.New[string]()
	for i := range plan.Apply {
		protected.Insert(plan.Apply[i].deployment.Name)
	}
	protected.Insert(plan.Keep...)
	gcOpts := []client.ListOption{
		label.NewLabelSelectorForElasticsearchClusterName(d.ES.Name),
		client.HasLabels{label.DeploymentNameLabelName},
	}
	if err := deployment.GC(ctx, d.Client, d.ES.Namespace, protected, gcOpts...); err != nil {
		return results.WithError(err)
	}

	if err := immutableconfig.GCAll(ctx, configRevisions, scriptsRevisions); err != nil {
		return results.WithError(err)
	}

	if plan.Transitioning {
		return results.WithReconciliationState(
			reconciler.Requeue.WithReason(fmt.Sprintf("transitioning: %s", plan.Reason)),
		)
	}
	return results
}

// tierNames returns the distinct tier identifiers of the given resources,
// in sorted order, for logging. Multiple NodeSets in the same tier
// contribute a single entry.
func tierNames(resources []nodeSetResources) []string {
	names := sets.New[string]()
	for i := range resources {
		names.Insert(string(resources[i].tier))
	}
	return sets.List(names)
}

// applyPlanResources reconciles the immutable config secret and the
// Deployment for every NodeSet in the Apply set. Each upsert is
// idempotent: identical template → no-op.
func (d *Driver) applyPlanResources(
	ctx context.Context,
	apply []nodeSetResources,
	configRevisions, scriptsRevisions *immutableconfig.RevisionManager,
	scriptsName string,
) error {
	esNsn := k8s.ExtractNamespacedName(&d.ES)
	for i := range apply {
		res := &apply[i]

		configSecret, err := settings.BuildStatelessImmutableConfigSecret(esNsn, res.deployment.Name, res.tier, res.config)
		if err != nil {
			return err
		}
		configName, err := configRevisions.Reconcile(ctx, &configSecret)
		if err != nil {
			return err
		}

		// Patch volume references to the content-addressed names. The
		// deployment reconciler computes the template hash itself, so
		// the template must be finalised first.
		configRevisions.PatchVolumes(res.deployment.Spec.Template.Spec.Volumes, configName)
		scriptsRevisions.PatchVolumes(res.deployment.Spec.Template.Spec.Volumes, scriptsName)

		if _, err := commondeployment.Reconcile(ctx, d.Client, res.deployment, &d.ES); err != nil {
			return err
		}
	}
	return nil
}

// reconcileImmutableScripts builds and reconciles the shared immutable
// scripts ConfigMap.
func (d *Driver) reconcileImmutableScripts(
	ctx context.Context,
	scriptsRevisions *immutableconfig.RevisionManager,
) (string, error) {
	scriptsData, err := configmap.BuildScriptsData(d.ES)
	if err != nil {
		return "", err
	}
	esNsn := k8s.ExtractNamespacedName(&d.ES)
	scriptsLabels := label.NewLabels(esNsn)
	baseName := esv1.ScriptsConfigMap(d.ES.Name)
	cm := immutableconfig.BuildImmutableConfigMap(baseName, d.ES.Namespace, scriptsData, scriptsLabels)
	return scriptsRevisions.Reconcile(ctx, &cm)
}
