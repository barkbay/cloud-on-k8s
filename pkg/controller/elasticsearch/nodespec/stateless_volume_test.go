// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package nodespec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	esvolume "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
)

// TestDefaultStatelessDataVolume verifies that the helper honors a
// user-provided VolumeClaimTemplate named "elasticsearch-cache" and falls back
// to the ECK default otherwise.
func TestDefaultStatelessDataVolume(t *testing.T) {
	customSpec := corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("50Gi"),
			},
		},
	}

	tests := []struct {
		name                  string
		vcts                  []corev1.PersistentVolumeClaim
		wantOverriddenByUser  bool
		wantStorageIfOverride string
	}{
		{
			name: "no VCT provided -> default ephemeral volume",
			vcts: nil,
		},
		{
			name: "unrelated VCT provided -> default ephemeral volume",
			vcts: []corev1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "something-else"}, Spec: customSpec},
			},
		},
		{
			name: "VCT named elasticsearch-cache -> user spec is honored",
			vcts: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: esvolume.ElasticsearchCacheVolumeName},
					Spec:       customSpec,
				},
			},
			wantOverriddenByUser:  true,
			wantStorageIfOverride: "50Gi",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := esv1.NodeSet{VolumeClaimTemplates: tt.vcts}

			vol := defaultStatelessDataVolume(ns)

			assert.Equal(t, esvolume.ElasticsearchCacheVolumeName, vol.Name)
			require.NotNil(t, vol.VolumeSource.Ephemeral, "stateless data volume must be ephemeral")
			require.NotNil(t, vol.VolumeSource.Ephemeral.VolumeClaimTemplate)

			gotSpec := vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec
			if tt.wantOverriddenByUser {
				want := resource.MustParse(tt.wantStorageIfOverride)
				assert.True(t, gotSpec.Resources.Requests.Storage().Equal(want),
					"expected storage request %s, got %s", want.String(), gotSpec.Resources.Requests.Storage().String())
			} else {
				assert.Equal(t, esvolume.DefaultDataVolumeClaim.Spec.Resources.Requests.Storage().String(),
					gotSpec.Resources.Requests.Storage().String())
			}
		})
	}
}
