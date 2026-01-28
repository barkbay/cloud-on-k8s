// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package common

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Volume name and path constants used by both API types and controllers.
const (
	// ElasticsearchDataVolumeName is the name of the Elasticsearch data volume.
	ElasticsearchDataVolumeName = "elasticsearch-data"
	// ElasticsearchDataMountPath is the mount path for the Elasticsearch data volume.
	ElasticsearchDataMountPath = "/usr/share/elasticsearch/data"
)

var (
	// DefaultPersistentVolumeSize is the default size for Elasticsearch data volumes.
	DefaultPersistentVolumeSize = resource.MustParse("1Gi")

	// DefaultDataVolumeClaim is the default data volume claim for Elasticsearch pods.
	// We default to a 1GB persistent volume, using the default storage class.
	DefaultDataVolumeClaim = corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: ElasticsearchDataVolumeName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: DefaultPersistentVolumeSize,
				},
			},
		},
	}
)
