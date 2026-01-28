// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
)

type VolumeClaimTemplate struct {
	Metadata SimpleMetadata `json:"metadata,omitempty"`

	// spec defines the desired characteristics of a volume requested by a pod author.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +kubebuilder:validation:Required
	Spec corev1.PersistentVolumeClaimSpec `json:"spec,omitempty"`
}

// DefaultStatelessPersistentVolume returns the default stateless persistent volume definition.
func DefaultStatelessPersistentVolume() corev1.Volume {
	defaultSpec := escommon.DefaultDataVolumeClaim.Spec.DeepCopy()
	return corev1.Volume{
		Name: escommon.ElasticsearchDataVolumeName,
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
					Spec: *defaultSpec,
				},
			},
		},
	}
}

type SimpleMetadata struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}
