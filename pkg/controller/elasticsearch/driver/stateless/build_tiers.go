// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"errors"

	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/bootstrap"
	netutil "github.com/elastic/cloud-on-k8s/v3/pkg/utils/net"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
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
) (map[v1alpha1.ElasticsearchTierName]*TierResources, error) {
	deployments := make(map[v1alpha1.ElasticsearchTierName]*TierResources)
	var errs []error
	for _, tier := range v1alpha1.AllElasticsearchTierNames {
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
		policyConfig, err := nodespec.GetPolicyConfig(ctx, d.Client, &d.ES)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cfg, err := settings.NewMergedESConfig(
			d.ES.Name, true, ver,
			d.OperatorParameters.IPFamily,
			d.ES.Spec.HTTP, userCfg,
			policyConfig.ElasticsearchConfig,
			d.ES.GetRemoteClusterServer().Enabled,
			d.OperatorParameters.SetDefaultSecurityContext,
		)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Add stateless specific config
		cfg, err = settings.WithStatelessConfig(tier, d.ES.Spec.ObjectStore, cfg)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		deploymentName := common.PodControllerName(&d.ES, string(tier))
		// deploymentSelector is used to match the deploymentSelector pods
		deploymentSelector := label.NewDeploymentLabels(k8s.ExtractNamespacedName(&d.ES), deploymentName)
		mergedMeta := meta.Merge(metadata.Metadata{Labels: deploymentSelector})

		namedTierSpec := tierSpec.AsNamedTierSpec(tier)
		// Pod template
		podTemplateSpec, err := nodespec.BuildPodTemplateSpec(
			ctx,
			d.Client,
			&d.ES,
			namedTierSpec,
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

		if tier == v1alpha1.IndexTierName && !bootstrap.AnnotatedForBootstrap(&d.ES) {
			// !!!! Dirty hack for stateless clusters until we can use something else than the current stateful only image !!!!
			// SetupInitialMasterNodes is only called during upscales in stateful
			cfg.SetStrings(esv1.ClusterInitialMasterNodes, netutil.IPLiteralFor("${"+settings.EnvPodIP+"}", d.OperatorParameters.IPFamily))
		}

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
