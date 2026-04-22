// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commondeployment "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/bootstrap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/configmap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

// reconcileNodeSets orchestrates reconciliation of all NodeSet Deployments.
//
// When a version upgrade is in progress or pending we reconcile Deployments in
// tier-ordered groups (search first, index/master/ml second) so the reader
// tier is rolled out before the writer tiers. See
// deployment.ShouldGroupDeploymentReconciliation for the full rule set.
func (d *Driver) reconcileNodeSets(ctx context.Context, meta metadata.Metadata) *reconciler.Results {
	results := reconciler.NewResult(ctx)
	log := ulog.FromContext(ctx)

	// Check expectations are satisfied before proceeding
	satisfied, reason, err := d.Expectations.Satisfied()
	if err != nil {
		return results.WithError(err)
	}
	if !satisfied {
		log.Info("Expectations not yet satisfied, requeuing", "reason", reason)
		return results.WithReconciliationState(reconciler.Requeue.WithReason(reason))
	}

	// Build resources for each NodeSet
	allResources, err := d.buildAllNodeSetResources(ctx, meta)
	if err != nil {
		return results.WithError(err)
	}

	// Set up immutable config revision managers
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

	// Reconcile shared immutable scripts ConfigMap (once for all NodeSets)
	scriptsName, err := d.reconcileImmutableScripts(ctx, scriptsRevisions)
	if err != nil {
		return results.WithError(err)
	}

	// Decide whether we need to reconcile Deployments in tier-ordered groups.
	observed, err := d.observedDeployments(ctx)
	if err != nil {
		return results.WithError(err)
	}
	expectedDeployments := make([]appsv1.Deployment, 0, len(allResources))
	for i := range allResources {
		expectedDeployments = append(expectedDeployments, allResources[i].deployment)
	}
	groups := [][]nodeSetResources{allResources}
	shouldGroup := deployment.ShouldGroupDeploymentReconciliation(
		ctx,
		observed,
		expectedDeployments,
		bootstrap.AnnotatedForBootstrap(d.ES),
		statelessHasMasterRole,
	)
	if shouldGroup {
		groups = deployment.GroupByTier(allResources, nodeSetTier)
		log.Info("Reconciling stateless Deployments in tier-ordered groups",
			"group_0", tierNames(groups[0]), "group_1", tierNames(groups[1]))
	}

	expectedDeploymentNames := sets.New[string]()
	for i := range allResources {
		expectedDeploymentNames.Insert(allResources[i].deployment.Name)
	}

	// Reconcile each group in order. Before moving on to the next group,
	// wait for the current one to be fully rolled out.
	requeueReason := ""
	for groupIdx, group := range groups {
		if len(group) == 0 {
			continue
		}
		if err := d.reconcileGroup(ctx, group, configRevisions, scriptsRevisions, scriptsName); err != nil {
			return results.WithError(err)
		}
		// Only gate progress when there is another group still to reconcile.
		if groupIdx < len(groups)-1 {
			rolledOut, err := d.isGroupRolledOut(ctx, group)
			if err != nil {
				return results.WithError(err)
			}
			if !rolledOut {
				requeueReason = fmt.Sprintf("waiting for tiers %v to roll out before reconciling next group", tierNames(group))
				log.Info(requeueReason)
				break
			}
		}
	}

	// GC old Deployments that no longer correspond to a NodeSet.
	gcOpts := []client.ListOption{
		label.NewLabelSelectorForElasticsearchClusterName(d.ES.Name),
		client.HasLabels{label.DeploymentNameLabelName},
	}
	if err := deployment.GC(ctx, d.Client, d.ES.Namespace, expectedDeploymentNames, gcOpts...); err != nil {
		return results.WithError(err)
	}

	// GC old immutable config revisions
	if err := immutableconfig.GCAll(ctx, configRevisions, scriptsRevisions); err != nil {
		return results.WithError(err)
	}

	if requeueReason != "" {
		return results.WithReconciliationState(reconciler.Requeue.WithReason(requeueReason))
	}
	return results
}

// reconcileGroup reconciles the immutable config secret and the Deployment for
// every NodeSet in the group.
func (d *Driver) reconcileGroup(
	ctx context.Context,
	group []nodeSetResources,
	configRevisions, scriptsRevisions *immutableconfig.RevisionManager,
	scriptsName string,
) error {
	esNsn := k8s.ExtractNamespacedName(&d.ES)
	for i := range group {
		res := &group[i]

		// Reconcile immutable config Secret for this NodeSet
		configSecret, err := settings.BuildStatelessImmutableConfigSecret(esNsn, res.deployment.Name, res.tier, res.config)
		if err != nil {
			return err
		}
		configName, err := configRevisions.Reconcile(ctx, &configSecret)
		if err != nil {
			return err
		}

		// Patch volume references to the content-addressed names. The deployment
		// reconciler computes the template hash itself, so the template must be
		// finalised first.
		configRevisions.PatchVolumes(res.deployment.Spec.Template.Spec.Volumes, configName)
		scriptsRevisions.PatchVolumes(res.deployment.Spec.Template.Spec.Volumes, scriptsName)

		if _, err := commondeployment.Reconcile(ctx, d.Client, res.deployment, &d.ES); err != nil {
			return err
		}
	}
	return nil
}

// isGroupRolledOut fetches the live state of every Deployment in the group
// from the API and reports whether they have all finished rolling out.
func (d *Driver) isGroupRolledOut(ctx context.Context, group []nodeSetResources) (bool, error) {
	for i := range group {
		existing := &appsv1.Deployment{}
		key := client.ObjectKey{Namespace: d.ES.Namespace, Name: group[i].deployment.Name}
		if err := d.Client.Get(ctx, key, existing); err != nil {
			return false, err
		}
		if !deployment.IsRolledOut(existing) {
			return false, nil
		}
	}
	return true, nil
}

// buildAllNodeSetResources builds resources for all NodeSets.
func (d *Driver) buildAllNodeSetResources(ctx context.Context, meta metadata.Metadata) ([]nodeSetResources, error) {
	policyConfig, err := nodespec.GetPolicyConfig(ctx, d.Client, d.ES)
	if err != nil {
		return nil, fmt.Errorf("failed to get policy config: %w", err)
	}

	// Read the restart-trigger annotation from the current pods so we preserve
	// it across reconciliations when the user removes it from the ES spec.
	// This matches the stateful behavior (see nodespec.BuildExpectedResources).
	actualPodsRestartTriggerAnnotationValue, err := sset.GetActualPodsRestartTriggerAnnotationForCluster(d.Client, d.ES)
	if err != nil {
		return nil, fmt.Errorf("failed to get restart-trigger annotation from pods: %w", err)
	}

	allResources := make([]nodeSetResources, 0, len(d.ES.Spec.NodeSets))
	for _, nodeSet := range d.ES.Spec.NodeSets {
		res, err := buildNodeSetResources(
			ctx,
			d.Client,
			d.ES,
			nodeSet,
			d.Version,
			d.OperatorParameters.IPFamily,
			d.OperatorParameters.SetDefaultSecurityContext,
			policyConfig,
			meta,
			actualPodsRestartTriggerAnnotationValue,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to build resources for NodeSet %s: %w", nodeSet.Name, err)
		}
		allResources = append(allResources, res)
	}
	return allResources, nil
}

// reconcileImmutableScripts builds and reconciles the shared immutable scripts ConfigMap.
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
