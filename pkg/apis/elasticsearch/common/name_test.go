// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/optional"
)

// mockCluster implements ElasticsearchCluster for testing.
type mockCluster struct {
	metav1.ObjectMeta
	stateless bool
}

var _ ElasticsearchCluster = &mockCluster{}

func (m *mockCluster) GetObjectKind() schema.ObjectKind { return nil }
func (m *mockCluster) DeepCopyObject() runtime.Object  { return nil }

func (m *mockCluster) GetVersion() string                                    { return "8.0.0" }
func (m *mockCluster) GetImage() string                                      { return "" }
func (m *mockCluster) GetHTTP() commonv1.HTTPConfig                          { return commonv1.HTTPConfig{} }
func (m *mockCluster) GetTransport() TransportConfig                         { return TransportConfig{} }
func (m *mockCluster) GetAuth() Auth                                         { return Auth{} }
func (m *mockCluster) GetSecureSettings() []commonv1.SecretSource            { return nil }
func (m *mockCluster) SecureSettings() []commonv1.SecretSource               { return nil }
func (m *mockCluster) GetServiceAccountName() string                         { return "" }
func (m *mockCluster) GetRemoteClusterServer() RemoteClusterServer           { return RemoteClusterServer{} }
func (m *mockCluster) GetRemoteClusters() []RemoteCluster                    { return nil }
func (m *mockCluster) SupportsRemoteClusterAPIKeys() (*optional.Bool, error) { return optional.NewBool(true), nil }
func (m *mockCluster) IsStateless() bool                                     { return m.stateless }
func (m *mockCluster) DownwardNodeLabels() []string                          { return nil }
func (m *mockCluster) HasDownwardNodeLabels() bool                           { return false }
func (m *mockCluster) IsConfiguredToAllowDowngrades() bool                   { return false }
func (m *mockCluster) GetAssociations() []commonv1.Association               { return nil }

func newMockStatefulCluster(name string) *mockCluster {
	return &mockCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		stateless:  false,
	}
}

func newMockStatelessCluster(name string) *mockCluster {
	return &mockCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"},
		stateless:  true,
	}
}

func TestNamerFor(t *testing.T) {
	tests := []struct {
		name     string
		cluster  ElasticsearchCluster
		expected string
	}{
		{
			name:     "stateful cluster uses 'es' prefix",
			cluster:  newMockStatefulCluster("my-cluster"),
			expected: "my-cluster-es-http",
		},
		{
			name:     "stateless cluster uses 'ess' prefix",
			cluster:  newMockStatelessCluster("my-cluster"),
			expected: "my-cluster-ess-http",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HTTPService(tt.cluster)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHTTPService(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-http", HTTPService(stateful))
	assert.Equal(t, "test-es-ess-http", HTTPService(stateless))
}

func TestTransportService(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-transport", TransportService(stateful))
	assert.Equal(t, "test-es-ess-transport", TransportService(stateless))
}

func TestSecureSettingsSecret(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-secure-settings", SecureSettingsSecret(stateful))
	assert.Equal(t, "test-es-ess-secure-settings", SecureSettingsSecret(stateless))
}

func TestElasticUserSecret(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-elastic-user", ElasticUserSecret(stateful))
	assert.Equal(t, "test-es-ess-elastic-user", ElasticUserSecret(stateless))
}

func TestConfigSecret(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-default-es-config", ConfigSecret(stateful, "test-es-default"))
	assert.Equal(t, "test-es-index-ess-config", ConfigSecret(stateless, "test-es-index"))
}

func TestInternalHTTPService(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-internal-http", InternalHTTPService(stateful))
	assert.Equal(t, "test-es-ess-internal-http", InternalHTTPService(stateless))
}

func TestRemoteClusterService(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-remote-cluster", RemoteClusterService(stateful))
	assert.Equal(t, "test-es-ess-remote-cluster", RemoteClusterService(stateless))
}

func TestRolesAndFileRealmSecret(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-xpack-file-realm", RolesAndFileRealmSecret(stateful))
	assert.Equal(t, "test-es-ess-xpack-file-realm", RolesAndFileRealmSecret(stateless))
}

func TestInternalUsersSecret(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-internal-users", InternalUsersSecret(stateful))
	assert.Equal(t, "test-es-ess-internal-users", InternalUsersSecret(stateless))
}

func TestUnicastHostsConfigMap(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-unicast-hosts", UnicastHostsConfigMap(stateful))
	assert.Equal(t, "test-es-ess-unicast-hosts", UnicastHostsConfigMap(stateless))
}

func TestScriptsConfigMap(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-scripts", ScriptsConfigMap(stateful))
	assert.Equal(t, "test-es-ess-scripts", ScriptsConfigMap(stateless))
}

func TestLicenseSecretName(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-license", LicenseSecretName(stateful))
	assert.Equal(t, "test-es-ess-license", LicenseSecretName(stateless))
}

func TestDefaultPodDisruptionBudgetName(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-default", DefaultPodDisruptionBudgetName(stateful))
	assert.Equal(t, "test-es-ess-default", DefaultPodDisruptionBudgetName(stateless))
}

func TestRemoteCaSecretName(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-remote-ca", RemoteCaSecretName(stateful))
	assert.Equal(t, "test-es-ess-remote-ca", RemoteCaSecretName(stateless))
}

func TestRemoteAPIKeysSecretName(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-remote-api-keys", RemoteAPIKeysSecretName(stateful))
	assert.Equal(t, "test-es-ess-remote-api-keys", RemoteAPIKeysSecretName(stateless))
}

func TestFileSettingsSecretName(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-file-settings", FileSettingsSecretName(stateful))
	assert.Equal(t, "test-es-ess-file-settings", FileSettingsSecretName(stateless))
}

func TestStackConfigElasticsearchConfigSecretName(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	assert.Equal(t, "test-es-es-policy-config", StackConfigElasticsearchConfigSecretName(stateful))
	assert.Equal(t, "test-es-ess-policy-config", StackConfigElasticsearchConfigSecretName(stateless))
}

func TestPodDisruptionBudgetNameForRole(t *testing.T) {
	stateful := newMockStatefulCluster("test-es")
	stateless := newMockStatelessCluster("test-es")

	// With role
	assert.Equal(t, "test-es-es-default-data", PodDisruptionBudgetNameForRole(stateful, "data"))
	assert.Equal(t, "test-es-ess-default-data", PodDisruptionBudgetNameForRole(stateless, "data"))

	// Without role (coordinating)
	assert.Equal(t, "test-es-es-default-coordinating", PodDisruptionBudgetNameForRole(stateful, ""))
	assert.Equal(t, "test-es-ess-default-coordinating", PodDisruptionBudgetNameForRole(stateless, ""))
}

func TestSameNameDifferentTypes(t *testing.T) {
	// This test verifies that stateful and stateless clusters with the same name
	// produce different resource names
	stateful := newMockStatefulCluster("shared-name")
	stateless := newMockStatelessCluster("shared-name")

	// All resource names should be different
	assert.NotEqual(t, HTTPService(stateful), HTTPService(stateless))
	assert.NotEqual(t, TransportService(stateful), TransportService(stateless))
	assert.NotEqual(t, SecureSettingsSecret(stateful), SecureSettingsSecret(stateless))
	assert.NotEqual(t, ElasticUserSecret(stateful), ElasticUserSecret(stateless))
	assert.NotEqual(t, InternalUsersSecret(stateful), InternalUsersSecret(stateless))
	assert.NotEqual(t, RolesAndFileRealmSecret(stateful), RolesAndFileRealmSecret(stateless))
}

// Ensure mockCluster satisfies the client.Object interface required by ElasticsearchCluster
var _ client.Object = &mockCluster{}

func (m *mockCluster) GetUID() types.UID                       { return "" }
func (m *mockCluster) SetUID(uid types.UID)                    {}
func (m *mockCluster) GetResourceVersion() string              { return "" }
func (m *mockCluster) SetResourceVersion(version string)       {}
func (m *mockCluster) GetGeneration() int64                    { return 0 }
func (m *mockCluster) SetGeneration(generation int64)          {}
func (m *mockCluster) GetSelfLink() string                     { return "" }
func (m *mockCluster) SetSelfLink(selfLink string)             {}
func (m *mockCluster) GetCreationTimestamp() metav1.Time       { return metav1.Time{} }
func (m *mockCluster) SetCreationTimestamp(timestamp metav1.Time) {}
func (m *mockCluster) GetDeletionTimestamp() *metav1.Time      { return nil }
func (m *mockCluster) SetDeletionTimestamp(timestamp *metav1.Time) {}
func (m *mockCluster) GetDeletionGracePeriodSeconds() *int64   { return nil }
func (m *mockCluster) SetDeletionGracePeriodSeconds(*int64)    {}
func (m *mockCluster) GetLabels() map[string]string            { return m.Labels }
func (m *mockCluster) SetLabels(labels map[string]string)      { m.Labels = labels }
func (m *mockCluster) GetAnnotations() map[string]string       { return m.Annotations }
func (m *mockCluster) SetAnnotations(annotations map[string]string) { m.Annotations = annotations }
func (m *mockCluster) GetFinalizers() []string                 { return m.Finalizers }
func (m *mockCluster) SetFinalizers(finalizers []string)       { m.Finalizers = finalizers }
func (m *mockCluster) GetOwnerReferences() []metav1.OwnerReference { return m.OwnerReferences }
func (m *mockCluster) SetOwnerReferences(references []metav1.OwnerReference) { m.OwnerReferences = references }
func (m *mockCluster) GetManagedFields() []metav1.ManagedFieldsEntry { return m.ManagedFields }
func (m *mockCluster) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) { m.ManagedFields = managedFields }
