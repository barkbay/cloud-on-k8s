// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"context"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// StatelessConfigClassifier defines how config files are classified for stateless Elasticsearch.
// All config files for stateless are immutable since there's no hot-reload mechanism.
var StatelessConfigClassifier = immutableconfig.MapClassifier{
	ConfigFileName:                 immutableconfig.Immutable, // elasticsearch.yml
	OperatorUsersSettingsFileName:  immutableconfig.Immutable, // operator_users.yml
}

// StatelessConfigVolumeNames defines which volume names should be patched to use immutable secrets.
var StatelessConfigVolumeNames = map[string]bool{
	ConfigVolumeName: true,
}

// BuildStatelessImmutableConfigSecret builds an immutable, content-addressed config secret
// for stateless Elasticsearch. Returns the secret ready to be reconciled.
func BuildStatelessImmutableConfigSecret(
	es esv1.Elasticsearch,
	deploymentName string,
	config CanonicalConfig,
	meta metadata.Metadata,
	operatorPrivilegesSettings OperatorPrivilegesSettings,
) (corev1.Secret, error) {
	rendered, err := config.Render()
	if err != nil {
		return corev1.Secret{}, err
	}

	operatorSettingsData, err := yaml.Marshal(&operatorPrivilegesSettings)
	if err != nil {
		return corev1.Secret{}, err
	}

	data := map[string][]byte{
		ConfigFileName:                rendered,
		OperatorUsersSettingsFileName: operatorSettingsData,
	}

	// Base name follows the existing convention
	baseName := ConfigSecretName(deploymentName)

	// Merge labels
	mergedMeta := meta.Merge(metadata.Metadata{Labels: label.NewConfigLabels(k8s.ExtractNamespacedName(&es), deploymentName)})

	return immutableconfig.BuildImmutableSecret(baseName, es.Namespace, data, mergedMeta.Labels), nil
}

// ReconcileStatelessImmutableConfig creates an immutable config secret if it doesn't exist.
// The Elasticsearch CR is set as the owner so the secret is garbage-collected on CR deletion.
// Returns the name of the immutable secret for use in volume patching.
func ReconcileStatelessImmutableConfig(
	ctx context.Context,
	c k8s.Client,
	es esv1.Elasticsearch,
	deploymentName string,
	config CanonicalConfig,
	meta metadata.Metadata,
	operatorPrivilegesSettings OperatorPrivilegesSettings,
) (string, error) {
	secret, err := BuildStatelessImmutableConfigSecret(es, deploymentName, config, meta, operatorPrivilegesSettings)
	if err != nil {
		return "", err
	}

	if err := immutableconfig.ReconcileImmutableSecret(ctx, c, secret, &es); err != nil {
		return "", err
	}

	return secret.Name, nil
}

// PatchStatelessConfigVolumes updates the deployment's pod template volumes to reference
// the immutable config secret.
func PatchStatelessConfigVolumes(deployment *appsv1.Deployment, immutableSecretName string) {
	immutableconfig.PatchSecretVolumes(
		deployment.Spec.Template.Spec.Volumes,
		StatelessConfigVolumeNames,
		immutableSecretName,
	)
}

// GCStatelessImmutableConfigSecrets deletes unreferenced immutable config secrets.
// It protects secrets referenced by the given reconciledNames and any existing ReplicaSets.
func GCStatelessImmutableConfigSecrets(
	ctx context.Context,
	c k8s.Client,
	es esv1.Elasticsearch,
	reconciledNames sets.Set[string],
) error {
	// List all ReplicaSets for this Elasticsearch cluster
	var rsList appsv1.ReplicaSetList
	if err := c.List(ctx, &rsList,
		client.InNamespace(es.Namespace),
		client.MatchingLabels{label.ClusterNameLabelName: es.Name},
	); err != nil {
		return err
	}

	// Collect immutable secret names referenced by existing ReplicaSets
	protectedNames := reconciledNames.Clone()
	for i := range rsList.Items {
		if name := immutableconfig.ImmutableSecretNameFromVolumes(
			rsList.Items[i].Spec.Template.Spec.Volumes,
			ConfigVolumeName,
		); name != "" {
			protectedNames.Insert(name)
		}
	}

	// Delete unreferenced immutable secrets
	labelSelector := client.MatchingLabels{
		label.ClusterNameLabelName:            es.Name,
		immutableconfig.ConfigTypeLabelName:   immutableconfig.ConfigTypeImmutable,
	}

	return immutableconfig.GCUnreferencedSecrets(ctx, c, es.Namespace, labelSelector, protectedNames)
}
