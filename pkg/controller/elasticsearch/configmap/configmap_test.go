// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package configmap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/immutableconfig"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/initcontainer"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
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

				// Verify content of the config map
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

				// Labels should be updated
				assert.Equal(t, map[string]string{
					"existing-label": "old-value",
					"label1":         "value1",
				}, updatedConfigMap.Labels)
				// Annotations should be updated
				assert.Equal(t, map[string]string{
					"existing-annotation": "old-value",
					"annotation1":         "value1",
				}, updatedConfigMap.Annotations)

				// Data should be updated
				assert.NotContains(t, updatedConfigMap.Data, "old-key")
				assert.Contains(t, updatedConfigMap.Data, nodespec.PreStopHookScriptConfigKey)
				assert.Contains(t, updatedConfigMap.Data, initcontainer.PrepareFsScriptConfigKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock K8s client with initial objects
			mockClient := k8s.NewFakeClient(tt.initialObjects...)

			// Run the function
			err := ReconcileScriptsConfigMap(context.Background(), mockClient, es, tt.meta)

			// Check error
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Run validation
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

	// Should not have created any ConfigMap
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

	// Name should have a content-hash suffix
	assert.True(t, immutableconfig.IsImmutableName(cm.Name))
	assert.Contains(t, cm.Name, esv1.ScriptsConfigMap("test-es"))

	// Namespace
	assert.Equal(t, "ns1", cm.Namespace)

	// Data keys
	assert.Contains(t, cm.Data, nodespec.LegacyReadinessProbeScriptConfigKey)
	assert.Contains(t, cm.Data, nodespec.ReadinessPortProbeScriptConfigKey)
	assert.Contains(t, cm.Data, nodespec.PreStopHookScriptConfigKey)
	assert.Contains(t, cm.Data, initcontainer.PrepareFsScriptConfigKey)
	assert.Contains(t, cm.Data, initcontainer.SuspendScriptConfigKey)
	assert.Contains(t, cm.Data, initcontainer.SuspendedHostsFile)

	// Labels should include immutable config labels, custom labels, and cluster labels
	assert.Equal(t, immutableconfig.ConfigTypeImmutable, cm.Labels[immutableconfig.ConfigTypeLabelName])
	assert.NotEmpty(t, cm.Labels[immutableconfig.ConfigHashLabelName])
	assert.Equal(t, "label", cm.Labels["custom"])
	assert.Equal(t, "test-es", cm.Labels[label.ClusterNameLabelName])

	// Immutable flag
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

func TestReconcileStatelessImmutableScriptsConfigMap(t *testing.T) {
	es := newStatelessES("test-es", "ns1")
	meta := metadata.Metadata{}
	ctx := context.Background()

	t.Run("creates ConfigMap and returns name", func(t *testing.T) {
		mockClient := k8s.NewFakeClient()

		name, err := ReconcileStatelessImmutableScriptsConfigMap(ctx, mockClient, es, meta)
		require.NoError(t, err)
		assert.True(t, immutableconfig.IsImmutableName(name))

		// Verify it was created
		var cm corev1.ConfigMap
		require.NoError(t, mockClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "ns1"}, &cm))
		assert.Equal(t, name, cm.Name)
	})

	t.Run("idempotent on second call", func(t *testing.T) {
		mockClient := k8s.NewFakeClient()

		name1, err := ReconcileStatelessImmutableScriptsConfigMap(ctx, mockClient, es, meta)
		require.NoError(t, err)

		name2, err := ReconcileStatelessImmutableScriptsConfigMap(ctx, mockClient, es, meta)
		require.NoError(t, err)

		assert.Equal(t, name1, name2, "same content should return same name")
	})
}

func TestPatchStatelessScriptsVolumes(t *testing.T) {
	deployment := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: volume.ScriptsVolumeName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "test-es-es-scripts",
									},
								},
							},
						},
						{
							Name: "other-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "other-configmap",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	PatchStatelessScriptsVolumes(deployment, "test-es-es-scripts-a1b2c3d4")

	// Scripts volume should be patched
	assert.Equal(t, "test-es-es-scripts-a1b2c3d4", deployment.Spec.Template.Spec.Volumes[0].ConfigMap.Name)

	// Other volume should be untouched
	assert.Equal(t, "other-configmap", deployment.Spec.Template.Spec.Volumes[1].ConfigMap.Name)
}

func TestGCStatelessImmutableScriptsConfigMaps(t *testing.T) {
	esName := "test-es"
	namespace := "ns1"
	es := newStatelessES(esName, namespace)

	gcLabels := map[string]string{
		label.ClusterNameLabelName:          esName,
		immutableconfig.ConfigTypeLabelName: immutableconfig.ConfigTypeImmutable,
	}

	t.Run("deletes unreferenced ConfigMaps", func(t *testing.T) {
		currentCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-es-es-scripts-aabbccdd", Namespace: namespace, Labels: gcLabels,
			},
		}
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-es-es-scripts-11223344", Namespace: namespace, Labels: gcLabels,
			},
		}
		unrelatedCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "other-configmap", Namespace: namespace,
				Labels: map[string]string{"unrelated": "true"},
			},
		}

		mockClient := k8s.NewFakeClient(currentCM, oldCM, unrelatedCM)
		reconciledNames := sets.New[string]("test-es-es-scripts-aabbccdd")

		err := GCStatelessImmutableScriptsConfigMaps(context.Background(), mockClient, es, reconciledNames)
		require.NoError(t, err)

		// Current ConfigMap should still exist
		var cm corev1.ConfigMap
		require.NoError(t, mockClient.Get(context.Background(), types.NamespacedName{Name: "test-es-es-scripts-aabbccdd", Namespace: namespace}, &cm))

		// Old ConfigMap should be deleted
		err = mockClient.Get(context.Background(), types.NamespacedName{Name: "test-es-es-scripts-11223344", Namespace: namespace}, &cm)
		assert.Error(t, err)

		// Unrelated ConfigMap should be untouched
		require.NoError(t, mockClient.Get(context.Background(), types.NamespacedName{Name: "other-configmap", Namespace: namespace}, &cm))
	})

	t.Run("protects ConfigMaps referenced by ReplicaSets", func(t *testing.T) {
		currentCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-es-es-scripts-aabbccdd", Namespace: namespace, Labels: gcLabels,
			},
		}
		oldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-es-es-scripts-11223344", Namespace: namespace, Labels: gcLabels,
			},
		}
		veryOldCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-es-es-scripts-00000000", Namespace: namespace, Labels: gcLabels,
			},
		}

		// Old ReplicaSet still references oldCM
		oldRS := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "old-rs", Namespace: namespace,
				Labels: map[string]string{label.ClusterNameLabelName: esName},
			},
			Spec: appsv1.ReplicaSetSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: volume.ScriptsVolumeName,
								VolumeSource: corev1.VolumeSource{
									ConfigMap: &corev1.ConfigMapVolumeSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "test-es-es-scripts-11223344",
										},
									},
								},
							},
						},
					},
				},
			},
		}

		mockClient := k8s.NewFakeClient(currentCM, oldCM, veryOldCM, oldRS)
		reconciledNames := sets.New[string]("test-es-es-scripts-aabbccdd")

		err := GCStatelessImmutableScriptsConfigMaps(context.Background(), mockClient, es, reconciledNames)
		require.NoError(t, err)

		var cm corev1.ConfigMap

		// Current ConfigMap should still exist (protected by reconciledNames)
		require.NoError(t, mockClient.Get(context.Background(), types.NamespacedName{Name: "test-es-es-scripts-aabbccdd", Namespace: namespace}, &cm))

		// Old ConfigMap should still exist (protected by ReplicaSet reference)
		require.NoError(t, mockClient.Get(context.Background(), types.NamespacedName{Name: "test-es-es-scripts-11223344", Namespace: namespace}, &cm))

		// Very old ConfigMap should be deleted (not referenced by anyone)
		err = mockClient.Get(context.Background(), types.NamespacedName{Name: "test-es-es-scripts-00000000", Namespace: namespace}, &cm)
		assert.Error(t, err)
	})

	t.Run("no-op when no unreferenced ConfigMaps", func(t *testing.T) {
		currentCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-es-es-scripts-aabbccdd", Namespace: namespace, Labels: gcLabels,
			},
		}

		mockClient := k8s.NewFakeClient(currentCM)
		reconciledNames := sets.New[string]("test-es-es-scripts-aabbccdd")

		err := GCStatelessImmutableScriptsConfigMaps(context.Background(), mockClient, es, reconciledNames)
		require.NoError(t, err)

		// ConfigMap should still exist
		var cm corev1.ConfigMap
		require.NoError(t, mockClient.Get(context.Background(), types.NamespacedName{Name: "test-es-es-scripts-aabbccdd", Namespace: namespace}, &cm))
	})
}
