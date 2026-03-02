// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package configmap

import (
	"context"
	"reflect"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/maps"

	"go.elastic.co/apm/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/initcontainer"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// ReconcileScriptsConfigMap reconciles a configmap containing scripts and related configuration used by
// init containers and readiness probe.
// For stateless Elasticsearch, this is a no-op as the stateless driver handles immutable ConfigMap reconciliation.
func ReconcileScriptsConfigMap(ctx context.Context, c k8s.Client, es esv1.Elasticsearch, meta metadata.Metadata) error {
	// Stateless uses immutable ConfigMaps with content-hash naming, handled in the stateless driver.
	if es.IsStateless() {
		return nil
	}

	span, ctx := apm.StartSpan(ctx, "reconcile_scripts", tracing.SpanTypeApp)
	defer span.End()

	fsScript, err := initcontainer.RenderPrepareFsScript(es.IsStateless(), es.DownwardNodeLabels())
	if err != nil {
		return err
	}

	preStopScript, err := nodespec.RenderPreStopHookScript(services.InternalServiceURL(es))
	if err != nil {
		return err
	}

	nsn := types.NamespacedName{Name: esv1.ScriptsConfigMap(es.Name), Namespace: es.Namespace}
	scriptsConfigMap := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        nsn.Name,
			Namespace:   nsn.Namespace,
			Labels:      meta.Labels,
			Annotations: meta.Annotations,
		},
		Data: map[string]string{
			nodespec.LegacyReadinessProbeScriptConfigKey: nodespec.LegacyReadinessProbeScript,
			nodespec.ReadinessPortProbeScriptConfigKey:   nodespec.ReadinessPortProbeScript,
			nodespec.PreStopHookScriptConfigKey:          preStopScript,
			initcontainer.PrepareFsScriptConfigKey:       fsScript,
			initcontainer.SuspendScriptConfigKey:         initcontainer.SuspendScript,
			initcontainer.SuspendedHostsFile:             initcontainer.RenderSuspendConfiguration(es),
		},
	}
	return reconcileConfigMap(ctx, c, es, scriptsConfigMap)
}

// reconcileConfigMap checks for an existing config map and updates it or creates one if it does not exist.
func reconcileConfigMap(
	ctx context.Context,
	c k8s.Client,
	es esv1.Elasticsearch,
	expected corev1.ConfigMap,
) error {
	reconciled := &corev1.ConfigMap{}
	return reconciler.ReconcileResource(
		reconciler.Params{
			Context:    ctx,
			Client:     c,
			Owner:      &es,
			Expected:   &expected,
			Reconciled: reconciled,
			NeedsUpdate: func() bool {
				return !maps.IsSubset(expected.Labels, reconciled.Labels) ||
					!maps.IsSubset(expected.Annotations, reconciled.Annotations) ||
					!reflect.DeepEqual(expected.Data, reconciled.Data)
			},
			UpdateReconciled: func() {
				reconciled.Labels = maps.Merge(reconciled.Labels, expected.Labels)
				reconciled.Annotations = maps.Merge(reconciled.Annotations, expected.Annotations)
				reconciled.Data = expected.Data
			},
		},
	)
}

// StatelessScriptsVolumeNames are the volume names that reference the scripts ConfigMap.
var StatelessScriptsVolumeNames = map[string]bool{volume.ScriptsVolumeName: true}

// BuildStatelessImmutableScriptsConfigMap builds the scripts ConfigMap with a content-hash suffix
// for stateless Elasticsearch deployments.
func BuildStatelessImmutableScriptsConfigMap(es esv1.Elasticsearch, meta metadata.Metadata) (corev1.ConfigMap, error) {
	fsScript, err := initcontainer.RenderPrepareFsScript(es.IsStateless(), es.DownwardNodeLabels())
	if err != nil {
		return corev1.ConfigMap{}, err
	}

	preStopScript, err := nodespec.RenderPreStopHookScript(services.InternalServiceURL(es))
	if err != nil {
		return corev1.ConfigMap{}, err
	}

	data := map[string]string{
		nodespec.LegacyReadinessProbeScriptConfigKey: nodespec.LegacyReadinessProbeScript,
		nodespec.ReadinessPortProbeScriptConfigKey:   nodespec.ReadinessPortProbeScript,
		nodespec.PreStopHookScriptConfigKey:          preStopScript,
		initcontainer.PrepareFsScriptConfigKey:       fsScript,
		initcontainer.SuspendScriptConfigKey:         initcontainer.SuspendScript,
		initcontainer.SuspendedHostsFile:             initcontainer.RenderSuspendConfiguration(es),
	}

	baseName := esv1.ScriptsConfigMap(es.Name)
	labels := maps.Merge(meta.Labels, label.NewLabels(k8s.ExtractNamespacedName(&es)))

	return immutableconfig.BuildImmutableConfigMap(baseName, es.Namespace, data, labels), nil
}

// ReconcileStatelessImmutableScriptsConfigMap reconciles the immutable scripts ConfigMap
// for stateless Elasticsearch and returns the name of the created ConfigMap.
// The Elasticsearch CR is set as the owner so the ConfigMap is garbage-collected on CR deletion.
func ReconcileStatelessImmutableScriptsConfigMap(ctx context.Context, c k8s.Client, es esv1.Elasticsearch, meta metadata.Metadata) (string, error) {
	span, ctx := apm.StartSpan(ctx, "reconcile_stateless_scripts", tracing.SpanTypeApp)
	defer span.End()

	cm, err := BuildStatelessImmutableScriptsConfigMap(es, meta)
	if err != nil {
		return "", err
	}

	if err := immutableconfig.ReconcileImmutableConfigMap(ctx, c, cm, &es); err != nil {
		return "", err
	}

	return cm.Name, nil
}

// PatchStatelessScriptsVolumes patches the volumes in the Deployment to reference the given immutable scripts ConfigMap.
func PatchStatelessScriptsVolumes(deployment *appsv1.Deployment, immutableConfigMapName string) {
	immutableconfig.PatchConfigMapVolumes(deployment.Spec.Template.Spec.Volumes, StatelessScriptsVolumeNames, immutableConfigMapName)
}

// GCStatelessImmutableScriptsConfigMaps garbage collects old immutable scripts ConfigMaps
// that are no longer referenced by any existing ReplicaSet.
func GCStatelessImmutableScriptsConfigMaps(ctx context.Context, c k8s.Client, es esv1.Elasticsearch, reconciledNames sets.Set[string]) error {
	// List all ReplicaSets for this Elasticsearch cluster
	var rsList appsv1.ReplicaSetList
	if err := c.List(ctx, &rsList,
		client.InNamespace(es.Namespace),
		client.MatchingLabels{label.ClusterNameLabelName: es.Name},
	); err != nil {
		return err
	}

	// Protect ConfigMaps referenced by existing ReplicaSets in addition to the just-reconciled names
	protectedNames := reconciledNames.Clone()
	for i := range rsList.Items {
		if name := immutableconfig.ImmutableConfigMapNameFromVolumes(
			rsList.Items[i].Spec.Template.Spec.Volumes,
			volume.ScriptsVolumeName,
		); name != "" {
			protectedNames.Insert(name)
		}
	}

	labelSelector := client.MatchingLabels{
		label.ClusterNameLabelName:          es.Name,
		immutableconfig.ConfigTypeLabelName: immutableconfig.ConfigTypeImmutable,
	}

	return immutableconfig.GCUnreferencedConfigMaps(ctx, c, es.Namespace, labelSelector, protectedNames)
}
