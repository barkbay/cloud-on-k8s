// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"

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

// BuildStatelessImmutableConfigSecret builds an immutable, content-addressed config secret
// for stateless Elasticsearch. Returns the secret ready to be passed to a Revision.
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

	baseName := ConfigSecretName(deploymentName)
	mergedMeta := meta.Merge(metadata.Metadata{Labels: label.NewConfigLabels(k8s.ExtractNamespacedName(&es), deploymentName)})

	return immutableconfig.BuildImmutableSecret(baseName, es.Namespace, data, mergedMeta.Labels), nil
}

