// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import corev1 "k8s.io/api/core/v1"

// ImmutableSecretNameFromVolumes returns the immutable (hash-suffixed) secret name
// referenced by the volume with the given volumeName, or "" if not found or not an immutable name.
func ImmutableSecretNameFromVolumes(volumes []corev1.Volume, volumeName string) string {
	for _, vol := range volumes {
		if vol.Name == volumeName && vol.Secret != nil {
			if IsImmutableName(vol.Secret.SecretName) {
				return vol.Secret.SecretName
			}
		}
	}
	return ""
}

// ImmutableConfigMapNameFromVolumes returns the immutable (hash-suffixed) ConfigMap name
// referenced by the volume with the given volumeName, or "" if not found or not an immutable name.
func ImmutableConfigMapNameFromVolumes(volumes []corev1.Volume, volumeName string) string {
	for _, vol := range volumes {
		if vol.Name == volumeName && vol.ConfigMap != nil {
			if IsImmutableName(vol.ConfigMap.Name) {
				return vol.ConfigMap.Name
			}
		}
	}
	return ""
}

// PatchSecretVolumes updates volume secret references in-place for volumes that should use
// the immutable secret. The volumeNames set specifies which volume names should be patched.
func PatchSecretVolumes(volumes []corev1.Volume, volumeNames map[string]bool, immutableSecretName string) {
	for i := range volumes {
		v := &volumes[i]
		if v.Secret == nil {
			continue
		}
		if volumeNames[v.Name] {
			v.Secret.SecretName = immutableSecretName
		}
	}
}

// PatchConfigMapVolumes updates volume ConfigMap references in-place for volumes that should use
// the immutable ConfigMap. The volumeNames set specifies which volume names should be patched.
func PatchConfigMapVolumes(volumes []corev1.Volume, volumeNames map[string]bool, immutableConfigMapName string) {
	for i := range volumes {
		v := &volumes[i]
		if v.ConfigMap == nil {
			continue
		}
		if volumeNames[v.Name] {
			v.ConfigMap.Name = immutableConfigMapName
		}
	}
}
