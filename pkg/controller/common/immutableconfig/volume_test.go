// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestImmutableSecretNameFromVolumes(t *testing.T) {
	tests := []struct {
		name       string
		volumes    []corev1.Volume
		volumeName string
		want       string
	}{
		{
			name: "finds immutable secret",
			volumes: []corev1.Volume{
				{
					Name: "config-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "my-config-a1b2c3d4",
						},
					},
				},
			},
			volumeName: "config-volume",
			want:       "my-config-a1b2c3d4",
		},
		{
			name: "returns empty for non-immutable secret",
			volumes: []corev1.Volume{
				{
					Name: "config-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "my-config",
						},
					},
				},
			},
			volumeName: "config-volume",
			want:       "",
		},
		{
			name: "returns empty for non-existent volume",
			volumes: []corev1.Volume{
				{
					Name: "other-volume",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "my-config-a1b2c3d4",
						},
					},
				},
			},
			volumeName: "config-volume",
			want:       "",
		},
		{
			name: "returns empty for non-secret volume",
			volumes: []corev1.Volume{
				{
					Name: "config-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "my-config-a1b2c3d4",
							},
						},
					},
				},
			},
			volumeName: "config-volume",
			want:       "",
		},
		{
			name:       "empty volumes",
			volumes:    []corev1.Volume{},
			volumeName: "config-volume",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ImmutableSecretNameFromVolumes(tt.volumes, tt.volumeName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestImmutableConfigMapNameFromVolumes(t *testing.T) {
	tests := []struct {
		name       string
		volumes    []corev1.Volume
		volumeName string
		want       string
	}{
		{
			name: "finds immutable configmap",
			volumes: []corev1.Volume{
				{
					Name: "scripts-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "my-scripts-a1b2c3d4",
							},
						},
					},
				},
			},
			volumeName: "scripts-volume",
			want:       "my-scripts-a1b2c3d4",
		},
		{
			name: "returns empty for non-immutable configmap",
			volumes: []corev1.Volume{
				{
					Name: "scripts-volume",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "my-scripts",
							},
						},
					},
				},
			},
			volumeName: "scripts-volume",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ImmutableConfigMapNameFromVolumes(tt.volumes, tt.volumeName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPatchSecretVolumes(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "config-volume",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: "old-config",
				},
			},
		},
		{
			Name: "other-volume",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: "other-secret",
				},
			},
		},
		{
			Name: "non-secret-volume",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	volumeNames := map[string]bool{"config-volume": true}
	PatchSecretVolumes(volumes, volumeNames, "new-config-a1b2c3d4")

	assert.Equal(t, "new-config-a1b2c3d4", volumes[0].Secret.SecretName)
	assert.Equal(t, "other-secret", volumes[1].Secret.SecretName)
}

func TestPatchConfigMapVolumes(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "scripts-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "old-scripts",
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
	}

	volumeNames := map[string]bool{"scripts-volume": true}
	PatchConfigMapVolumes(volumes, volumeNames, "new-scripts-a1b2c3d4")

	assert.Equal(t, "new-scripts-a1b2c3d4", volumes[0].ConfigMap.Name)
	assert.Equal(t, "other-configmap", volumes[1].ConfigMap.Name)
}
