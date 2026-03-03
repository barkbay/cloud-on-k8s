// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package configmap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/initcontainer"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

func TestReconcileScriptsConfigMap(t *testing.T) {
	namespace := "ns1"
	esName := "test-es"
	configMapName := "test-es-es-scripts"
	es := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Name:      esName,
			Namespace: namespace,
		},
	}

	tests := []struct {
		name           string
		initialObjects []client.Object
		meta           metadata.Metadata
		wantErr        bool
		validate       func(t *testing.T, client k8s.Client)
	}{
		{
			name:           "creates a new config map when it doesn't exist",
			initialObjects: []client.Object{},
			meta: metadata.Metadata{
				Labels:      map[string]string{"label1": "value1"},
				Annotations: map[string]string{"annotation1": "value1"},
			},
			validate: func(t *testing.T, client k8s.Client) {
				t.Helper()
				var createdConfigMap corev1.ConfigMap
				err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configMapName}, &createdConfigMap)
				assert.NoError(t, err)
				assert.Equal(t, configMapName, createdConfigMap.Name)
				assert.Equal(t, namespace, createdConfigMap.Namespace)
				assert.Equal(t, map[string]string{"label1": "value1"}, createdConfigMap.Labels)
				assert.Equal(t, map[string]string{"annotation1": "value1"}, createdConfigMap.Annotations)

				assert.Contains(t, createdConfigMap.Data, nodespec.LegacyReadinessProbeScriptConfigKey)
				assert.Contains(t, createdConfigMap.Data, nodespec.ReadinessPortProbeScriptConfigKey)
				assert.Contains(t, createdConfigMap.Data, nodespec.PreStopHookScriptConfigKey)
				assert.Contains(t, createdConfigMap.Data, initcontainer.PrepareFsScriptConfigKey)
				assert.Contains(t, createdConfigMap.Data, initcontainer.SuspendScriptConfigKey)
				assert.Contains(t, createdConfigMap.Data, initcontainer.SuspendedHostsFile)
			},
		},
		{
			name: "updates existing config map",
			initialObjects: []client.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:        configMapName,
						Namespace:   namespace,
						Labels:      map[string]string{"existing-label": "old-value"},
						Annotations: map[string]string{"existing-annotation": "old-value"},
					},
					Data: map[string]string{
						"existing-key": "existing-value",
					},
				},
			},
			meta: metadata.Metadata{
				Labels:      map[string]string{"label1": "value1"},
				Annotations: map[string]string{"annotation1": "value1"},
			},
			validate: func(t *testing.T, client k8s.Client) {
				t.Helper()
				var updatedConfigMap corev1.ConfigMap
				err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configMapName}, &updatedConfigMap)
				assert.NoError(t, err)

				assert.Equal(t, map[string]string{
					"existing-label": "old-value",
					"label1":         "value1",
				}, updatedConfigMap.Labels)
				assert.Equal(t, map[string]string{
					"existing-annotation": "old-value",
					"annotation1":         "value1",
				}, updatedConfigMap.Annotations)

				assert.NotContains(t, updatedConfigMap.Data, "old-key")
				assert.Contains(t, updatedConfigMap.Data, nodespec.PreStopHookScriptConfigKey)
				assert.Contains(t, updatedConfigMap.Data, initcontainer.PrepareFsScriptConfigKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := k8s.NewFakeClient(tt.initialObjects...)
			err := ReconcileScriptsConfigMap(context.Background(), mockClient, es, tt.meta)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.validate(t, mockClient)
			}
		})
	}
}

func TestReconcileScriptsConfigMap_StatelessNoOp(t *testing.T) {
	es := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-es",
			Namespace: "ns1",
		},
		Spec: esv1.ElasticsearchSpec{
			StatelessSpec: &esv1.StatelessSpec{},
		},
	}

	mockClient := k8s.NewFakeClient()
	err := ReconcileScriptsConfigMap(context.Background(), mockClient, es, metadata.Metadata{})
	require.NoError(t, err)

	var cmList corev1.ConfigMapList
	require.NoError(t, mockClient.List(context.Background(), &cmList))
	assert.Empty(t, cmList.Items)
}

func newStatelessES(name, namespace string) esv1.Elasticsearch {
	return esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: esv1.ElasticsearchSpec{
			StatelessSpec: &esv1.StatelessSpec{},
		},
	}
}

func TestBuildStatelessImmutableScriptsConfigMap(t *testing.T) {
	es := newStatelessES("test-es", "ns1")
	meta := metadata.Metadata{Labels: map[string]string{"custom": "label"}}

	cm, err := BuildStatelessImmutableScriptsConfigMap(es, meta)
	require.NoError(t, err)

	assert.Equal(t, immutableconfig.ConfigTypeImmutable, cm.Labels[immutableconfig.ConfigTypeLabelName])
	assert.Contains(t, cm.Name, esv1.ScriptsConfigMap("test-es"))
	assert.Equal(t, "ns1", cm.Namespace)

	assert.Contains(t, cm.Data, nodespec.LegacyReadinessProbeScriptConfigKey)
	assert.Contains(t, cm.Data, nodespec.ReadinessPortProbeScriptConfigKey)
	assert.Contains(t, cm.Data, nodespec.PreStopHookScriptConfigKey)
	assert.Contains(t, cm.Data, initcontainer.PrepareFsScriptConfigKey)
	assert.Contains(t, cm.Data, initcontainer.SuspendScriptConfigKey)
	assert.Contains(t, cm.Data, initcontainer.SuspendedHostsFile)

	assert.Equal(t, immutableconfig.ConfigTypeImmutable, cm.Labels[immutableconfig.ConfigTypeLabelName])
	assert.NotEmpty(t, cm.Labels[immutableconfig.ConfigHashLabelName])
	assert.Equal(t, "label", cm.Labels["custom"])
	assert.Equal(t, "test-es", cm.Labels[label.ClusterNameLabelName])

	require.NotNil(t, cm.Immutable)
	assert.True(t, *cm.Immutable)
}

func TestBuildStatelessImmutableScriptsConfigMap_Deterministic(t *testing.T) {
	es := newStatelessES("test-es", "ns1")
	meta := metadata.Metadata{}

	cm1, err := BuildStatelessImmutableScriptsConfigMap(es, meta)
	require.NoError(t, err)

	cm2, err := BuildStatelessImmutableScriptsConfigMap(es, meta)
	require.NoError(t, err)

	assert.Equal(t, cm1.Name, cm2.Name, "same input should produce same content-hash name")
}

