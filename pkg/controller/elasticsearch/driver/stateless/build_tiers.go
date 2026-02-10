// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"errors"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

type TierResources struct {
	meta       metadata.Metadata
	deployment *appsv1.Deployment
	config     settings.CanonicalConfig
}

func (d *Driver) buildTierResources(
	ctx context.Context,
	meta metadata.Metadata,
	ver version.Version,
	keystoreResources *keystore.Resources,
) (map[esv1.ElasticsearchTierName]*TierResources, error) {
	deployments := make(map[esv1.ElasticsearchTierName]*TierResources)
	var errs []error
	for _, tier := range esv1.AllElasticsearchTierNames {
		meta := meta.Merge(metadata.Metadata{Labels: map[string]string{
			label.TierLabelName: string(tier),
		}})

		tierSpec, err := d.ES.GetTierSpec(tier)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Build ES configuration.
		userCfg := commonv1.Config{}
		if tierSpec.Config != nil {
			userCfg = *tierSpec.Config
		}

		// Get Policy config
		policyConfig, err := nodespec.GetPolicyConfig(ctx, d.Client, d.ES)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cfg, err := settings.NewMergedESConfig(
			d.ES.Name, true, ver,
			d.OperatorParameters.IPFamily,
			d.ES.Spec.HTTP, userCfg,
			policyConfig.ElasticsearchConfig,
			false, /* Spec.RemoteClusterServer.Enabled */
			d.OperatorParameters.SetDefaultSecurityContext,
		)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Add stateless specific config
		cfg, err = settings.WithStatelessConfig(tier, d.ES.Spec.StatelessSpec.StatelessConfig.ObjectStore, cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		deploymentName := esv1.PodsControllerResourceName(d.ES.Name, string(tier))
		if err := settings.ReconcileConfig(ctx, d.Client, d.ES, deploymentName, cfg, meta); err != nil {
			errs = append(errs, err)
			continue
		}

		// deploymentSelector is used to match the deploymentSelector pods
		deploymentSelector := label.NewDeploymentLabels(k8s.ExtractNamespacedName(&d.ES), deploymentName)
		mergedMeta := meta.Merge(metadata.Metadata{Labels: deploymentSelector})

		// Pod template
		podTemplateSpec, err := nodespec.BuildPodTemplateSpec(
			ctx,
			d.Client,
			d.ES,
			tierSpec.AsNamedTierSpec(tier),
			cfg,
			keystoreResources,
			d.OperatorParameters.SetDefaultSecurityContext,
			policyConfig,
			meta,
		)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Check if the user has provided the mandatory elasticsearch-data volume in the pod template.
		// If not, insert an ephemeral volume using DefaultStatelessPersistentVolume().
		hasDataVolume := false
		for _, vol := range podTemplateSpec.Spec.Volumes {
			if vol.Name == volume.ElasticsearchDataVolumeName {
				hasDataVolume = true
				break
			}
		}
		// Add default data volume if not present in the user-provided pod template.
		if !hasDataVolume {
			podTemplateSpec.Spec.Volumes = append(
				podTemplateSpec.Spec.Volumes,
				esv1.DefaultStatelessPersistentVolume(),
			)
		}

		rollingUpdate := tierSpec.RollingUpdate.DeepCopy()
		if rollingUpdate == nil {
			rollingUpdate = &appsv1.RollingUpdateDeployment{
				MaxUnavailable: ptr.To(intstr.FromInt32(0)),
				MaxSurge:       ptr.To(intstr.FromString("25%")),
			}
		}

		// Expected Deployment.
		expected := appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        deploymentName,
				Namespace:   d.ES.Namespace,
				Labels:      mergedMeta.Labels,
				Annotations: mergedMeta.Annotations,
			},
			Spec: appsv1.DeploymentSpec{
				Strategy: appsv1.DeploymentStrategy{
					Type:          appsv1.RollingUpdateDeploymentStrategyType,
					RollingUpdate: rollingUpdate,
				},
				Selector: &metav1.LabelSelector{
					MatchLabels: deploymentSelector,
				},
				Replicas: ptr.To(tierSpec.Count),
				Template: podTemplateSpec,
			},
		}
		expected = WithTemplateHash(expected)
		deployments[tier] = &TierResources{
			meta:       meta,
			deployment: &expected,
			config:     cfg,
		}
	}
	return deployments, errors.Join(errs...)
}

// WithTemplateHash returns a new Deployment with a hash of its template to ease comparisons.
func WithTemplateHash(cs appsv1.Deployment) appsv1.Deployment {
	csCopy := *cs.DeepCopy()
	csCopy.Labels = hash.SetTemplateHashLabel(csCopy.Labels, csCopy)
	return csCopy
}
