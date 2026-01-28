// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package common

import corev1 "k8s.io/api/core/v1"

// NodeSetSpec helps abstract over NodeSet in stateful, and TierSpec in stateless specs.
// They both define similar concepts even though their implementation can differ.
// +kubebuilder:object:generate=false
type NodeSetSpec interface {
	GetName() string
	GetPodTemplate() corev1.PodTemplateSpec
	GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim
}
