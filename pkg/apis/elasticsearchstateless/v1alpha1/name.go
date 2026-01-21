// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	common_name "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/name"
)

const (
	configSecretSuffix         = "config"
	secureSettingsSecretSuffix = "secure-settings"
	httpServiceSuffix          = "http"
	transportServiceSuffix     = "transport"
	elasticUserSecretSuffix    = "elastic-user"
	internalUsersSecretSuffix  = "internal-users"
	scriptsConfigMapSuffix     = "scripts"
	defaultPodDisruptionBudget = "default"

	// Tier suffixes
	indexTierSuffix  = "index"
	searchTierSuffix = "search"
	mlTierSuffix     = "ml"
)

var (
	// ESSNamer is a Namer that is configured with the defaults for resources related to an ElasticsearchStateless cluster.
	ESSNamer = common_name.NewNamer("ess")

	suffixes = []string{
		configSecretSuffix,
		secureSettingsSecretSuffix,
		httpServiceSuffix,
		transportServiceSuffix,
		elasticUserSecretSuffix,
		internalUsersSecretSuffix,
		scriptsConfigMapSuffix,
		defaultPodDisruptionBudget,
	}
)

// Deployment returns the name of the Deployment corresponding to the given tier.
func Deployment(essName string, tierName string) string {
	return ESSNamer.Suffix(essName, tierName)
}

// IndexTierDeployment returns the name of the index tier Deployment.
func IndexTierDeployment(essName string) string {
	return Deployment(essName, indexTierSuffix)
}

// SearchTierDeployment returns the name of the search tier Deployment.
func SearchTierDeployment(essName string) string {
	return Deployment(essName, searchTierSuffix)
}

// MLTierDeployment returns the name of the ML tier Deployment.
func MLTierDeployment(essName string) string {
	return Deployment(essName, mlTierSuffix)
}

// ConfigSecret returns the name of the config secret.
func ConfigSecret(essName string) string {
	return ESSNamer.Suffix(essName, configSecretSuffix)
}

// SecureSettingsSecret returns the name of the secure settings secret.
func SecureSettingsSecret(essName string) string {
	return ESSNamer.Suffix(essName, secureSettingsSecretSuffix)
}

// TransportService returns the name of the transport service.
func TransportService(essName string) string {
	return ESSNamer.Suffix(essName, transportServiceSuffix)
}

// HTTPService returns the name of the HTTP service.
func HTTPService(essName string) string {
	return ESSNamer.Suffix(essName, httpServiceSuffix)
}

// ElasticUserSecret returns the name of the elastic user secret.
func ElasticUserSecret(essName string) string {
	return ESSNamer.Suffix(essName, elasticUserSecretSuffix)
}

// InternalUsersSecret returns the name of the internal users secret.
func InternalUsersSecret(essName string) string {
	return ESSNamer.Suffix(essName, internalUsersSecretSuffix)
}

// ScriptsConfigMap returns the name of the scripts configmap.
func ScriptsConfigMap(essName string) string {
	return ESSNamer.Suffix(essName, scriptsConfigMapSuffix)
}

// DefaultPodDisruptionBudget returns the name of the default PDB.
func DefaultPodDisruptionBudget(essName string) string {
	return ESSNamer.Suffix(essName, defaultPodDisruptionBudget)
}
