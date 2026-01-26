// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package common

import (
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	common_name "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/name"
)

const (
	ConfigSecretSuffix           = "config"
	SecureSettingsSecretSuffix   = "secure-settings"
	FileSettingsSecretSuffix     = "file-settings"
	PolicyEsConfigSecretSuffix   = "policy-config" //nolint:gosec
	HTTPServiceSuffix            = "http"
	InternalHTTPServiceSuffix    = "internal-http"
	RemoteClusterServiceSuffix   = "remote-cluster"
	TransportServiceSuffix       = "transport"
	ElasticUserSecretSuffix      = "elastic-user"
	InternalUsersSecretSuffix    = "internal-users"
	UnicastHostsConfigMapSuffix  = "unicast-hosts"
	LicenseSecretSuffix          = "license"
	DefaultPodDisruptionBudget   = "default"
	ScriptsConfigMapSuffix       = "scripts"
	RolesAndFileRealmSecretSuffix = "xpack-file-realm" //nolint:gosec
	RemoteCaNameSuffix           = "remote-ca"
	RemoteAPIKeysNameSuffix      = "remote-api-keys"
)

var (
	// StatefulNamer is a Namer configured for stateful Elasticsearch resources.
	// Uses "es" prefix for backward compatibility.
	StatefulNamer = common_name.NewNamer("es")

	// StatelessNamer is a Namer configured for stateless Elasticsearch resources.
	// Uses "ess" prefix to ensure unique names from stateful resources.
	StatelessNamer = common_name.NewNamer("ess")
)

// NamerFor returns the appropriate namer based on the cluster type.
func NamerFor(cluster ElasticsearchCluster) common_name.Namer {
	if cluster.IsStateless() {
		return StatelessNamer
	}
	return StatefulNamer
}

// ConfigSecret returns the name of the config secret for the given cluster and sset/tier name.
func ConfigSecret(cluster ElasticsearchCluster, name string) string {
	return NamerFor(cluster).Suffix(name, ConfigSecretSuffix)
}

// SecureSettingsSecret returns the name of the secure settings secret for the given cluster.
func SecureSettingsSecret(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), SecureSettingsSecretSuffix)
}

// TransportService returns the name of the transport service for the given cluster.
func TransportService(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), TransportServiceSuffix)
}

// InternalHTTPService returns the name of the internal HTTP service for the given cluster.
func InternalHTTPService(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), InternalHTTPServiceSuffix)
}

// RemoteClusterService returns the name of the remote cluster service for the given cluster.
func RemoteClusterService(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), RemoteClusterServiceSuffix)
}

// HTTPService returns the name of the HTTP service for the given cluster.
func HTTPService(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), HTTPServiceSuffix)
}

// ElasticUserSecret returns the name of the elastic user secret for the given cluster.
func ElasticUserSecret(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), ElasticUserSecretSuffix)
}

// RolesAndFileRealmSecret returns the name of the roles and file realm secret for the given cluster.
func RolesAndFileRealmSecret(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), RolesAndFileRealmSecretSuffix)
}

// InternalUsersSecret returns the name of the internal users secret for the given cluster.
func InternalUsersSecret(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), InternalUsersSecretSuffix)
}

// UnicastHostsConfigMap returns the name of the ConfigMap that holds the list of seed nodes for a given cluster.
func UnicastHostsConfigMap(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), UnicastHostsConfigMapSuffix)
}

// ScriptsConfigMap returns the name of the scripts ConfigMap for the given cluster.
func ScriptsConfigMap(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), ScriptsConfigMapSuffix)
}

// LicenseSecretName returns the name of the license secret for the given cluster.
func LicenseSecretName(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), LicenseSecretSuffix)
}

// DefaultPodDisruptionBudgetName returns the name of the default PodDisruptionBudget for the given cluster.
func DefaultPodDisruptionBudgetName(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), DefaultPodDisruptionBudget)
}

// RemoteCaSecretName returns the name of the remote CA secret for the given cluster.
func RemoteCaSecretName(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), RemoteCaNameSuffix)
}

// RemoteAPIKeysSecretName returns the name of the remote API keys secret for the given cluster.
func RemoteAPIKeysSecretName(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), RemoteAPIKeysNameSuffix)
}

// FileSettingsSecretName returns the name of the file settings secret for the given cluster.
func FileSettingsSecretName(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), FileSettingsSecretSuffix)
}

// StackConfigElasticsearchConfigSecretName returns the name of the stack config policy ES config secret for the given cluster.
func StackConfigElasticsearchConfigSecretName(cluster ElasticsearchCluster) string {
	return NamerFor(cluster).Suffix(cluster.GetName(), PolicyEsConfigSecretSuffix)
}

// StackConfigAdditionalSecretName returns the name of the stack config policy Secret suffixed with a hash to prevent conflicts.
// This also helps keep the secret name size to within kubernetes name limits even if the secret name created by the user is long.
func StackConfigAdditionalSecretName(cluster ElasticsearchCluster, secretName string) string {
	secretNameHash := hash.HashObject(secretName)
	return NamerFor(cluster).Suffix(cluster.GetName(), "scp", secretNameHash)
}

// PodDisruptionBudgetNameForRole returns the name of the PodDisruptionBudget for a given cluster and role.
func PodDisruptionBudgetNameForRole(cluster ElasticsearchCluster, role string) string {
	// For coordinating nodes (no roles), append "coordinating" to the name
	if role == "" {
		role = "coordinating"
	}
	return NamerFor(cluster).Suffix(cluster.GetName(), DefaultPodDisruptionBudget, role)
}

// Namer type alias for use by other packages.
type Namer = common_name.Namer

// DeploymentTransportCertificatesSecret returns the name of the Secret containing transport certificates
// for a given Deployment in a stateless cluster.
func DeploymentTransportCertificatesSecret(clusterName, deploymentName string) string {
	return StatelessNamer.Suffix(clusterName, deploymentName, "transport-certs")
}
