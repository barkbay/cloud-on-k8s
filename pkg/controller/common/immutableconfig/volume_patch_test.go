// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestPatchSecretVolumesByClassification(t *testing.T) {
	classifier := MapClassifier{
		"immutable-volume": Immutable,
		"dynamic-volume":   Dynamic,
	}
	volumes := []corev1.Volume{
		{
			Name: "immutable-volume",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "old"},
			},
		},
		{
			Name: "dynamic-volume",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "old"},
			},
		},
		{
			Name: "other-volume",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	PatchSecretVolumesByClassification(volumes, classifier, Immutable, "new")

	assert.Equal(t, "new", volumes[0].Secret.SecretName)
	assert.Equal(t, "old", volumes[1].Secret.SecretName)
	assert.Nil(t, volumes[2].Secret)
}
