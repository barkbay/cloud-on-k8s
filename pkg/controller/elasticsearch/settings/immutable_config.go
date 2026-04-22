// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
)

// StatelessSecretVolumeClassifier classifies Secret-backed volumes for immutable config management.
// The config volume is immutable: changes produce a new content-addressed Secret name, triggering pod rotation.
var StatelessSecretVolumeClassifier = immutableconfig.MapClassifier{
	ConfigVolumeName: immutableconfig.Immutable,
}

// StatelessConfigMapVolumeClassifier classifies ConfigMap-backed volumes for immutable config management.
// The scripts volume is immutable: changes produce a new content-addressed ConfigMap name.
var StatelessConfigMapVolumeClassifier = immutableconfig.MapClassifier{
	volume.ScriptsVolumeName: immutableconfig.Immutable,
}

// BuildStatelessImmutableConfigSecret creates an immutable, content-addressed Secret containing
// the rendered Elasticsearch configuration for a single Deployment (NodeSet).
func BuildStatelessImmutableConfigSecret(
	es types.NamespacedName,
	deploymentName string,
	tier esv1.StatelessTier,
	config CanonicalConfig,
) (corev1.Secret, error) {
	rendered, err := config.Render()
	if err != nil {
		return corev1.Secret{}, err
	}

	data := map[string][]byte{
		ConfigFileName: rendered,
	}

	labels := label.NewDeploymentLabels(es, deploymentName, tier)
	baseName := ConfigSecretName(deploymentName)

	return immutableconfig.BuildImmutableSecret(baseName, es.Namespace, data, labels), nil
}
