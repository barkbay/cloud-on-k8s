// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
)

type ElasticsearchTierName string

const (
	MLTierName     ElasticsearchTierName = "ml"
	SearchTierName ElasticsearchTierName = "search"
	IndexTierName  ElasticsearchTierName = "index"
)

// TiersWithMasterRole
// Index nodes have master role only in the absence of a dedicated master tier (this is the default in serverless).
var TiersWithMasterRole = sets.New(escommon.MasterRole, escommon.IndexRole)

var AllElasticsearchTierNames = []ElasticsearchTierName{
	IndexTierName,
	SearchTierName,
	MLTierName,
}

type StatelessConfig struct {
	// +kubebuilder:validation:Required
	ObjectStore ObjectStoreConfig `json:"object_store,omitempty"`
}

type ObjectStoreConfig struct {
	// +kubebuilder:validation:Optional
	BasePath string `json:"base_path,omitempty"`

	// +kubebuilder:validation:Required
	Bucket string `json:"bucket,omitempty"`

	// +kubebuilder:validation:Required
	Client string `json:"client,omitempty"`

	// +kubebuilder:validation:Required
	Type string `json:"type,omitempty"`
}

type Tiers struct {
	Index  TierSpec `json:"index"`
	Search TierSpec `json:"search"`
	ML     TierSpec `json:"ml"`
}

func (es *ElasticsearchStateless) GetTierSpec(tierName ElasticsearchTierName) (*TierSpec, error) {
	switch tierName {
	case IndexTierName:
		return es.Spec.Tiers.Index, nil
	case SearchTierName:
		return es.Spec.Tiers.Search, nil
	case MLTierName:
		return es.Spec.Tiers.ML, nil
	default:
		return nil, fmt.Errorf("unknown tier name: %s", tierName)
	}
}

// +kubebuilder:object:generate=false
type NamedTierSpec struct {
	Name string
	*TierSpec
}

func (nt *NamedTierSpec) GetName() string {
	if nt == nil || nt.TierSpec == nil {
		return ""
	}
	return nt.Name
}

func (nt *NamedTierSpec) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	if nt == nil || nt.TierSpec == nil {
		return nil
	}
	// ToPersistentVolumeClaim returns a default claim when VolumeClaimTemplate is nil,
	// so we always get a valid PVC back.
	return []corev1.PersistentVolumeClaim{*nt.VolumeClaimTemplate.ToPersistentVolumeClaim()}
}

func (vct *VolumeClaimTemplate) ToPersistentVolumeClaim() *corev1.PersistentVolumeClaim {
	if vct == nil {
		defaultSpec := escommon.DefaultDataVolumeClaim.Spec.DeepCopy()
		defaultVolumeClaimTemplate := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: escommon.ElasticsearchDataVolumeName,
			},
			Spec: *defaultSpec,
		}
		return defaultVolumeClaimTemplate
	}

	result := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: vct.Metadata.Annotations,
			Labels:      vct.Metadata.Labels,
			Name:        escommon.ElasticsearchDataVolumeName,
		},
		Spec: vct.Spec,
	}
	if result.Spec.AccessModes == nil {
		result.Spec.AccessModes = escommon.DefaultDataVolumeClaim.Spec.AccessModes
	}
	return result
}

func (nt *NamedTierSpec) GetPodTemplate() corev1.PodTemplateSpec {
	if nt == nil || nt.TierSpec == nil {
		return corev1.PodTemplateSpec{}
	}
	return nt.PodTemplate
}

func (t *TierSpec) AsNamedTierSpec(name ElasticsearchTierName) *NamedTierSpec {
	return &NamedTierSpec{
		Name:     string(name),
		TierSpec: t,
	}
}

func (n *NamedTierSpec) DeepCopy() *NamedTierSpec {
	if n == nil {
		return nil
	}
	return &NamedTierSpec{
		Name:     n.Name,
		TierSpec: n.TierSpec.DeepCopy(),
	}
}
