// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	common "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// nodeSetResources holds the built resources for a single stateless NodeSet.
type nodeSetResources struct {
	nodeSetName string
	tier        esv1.StatelessTier
	deployment  appsv1.Deployment
	config      settings.CanonicalConfig
}

// buildNodeSetResources builds the Deployment and config for a single stateless NodeSet.
func buildNodeSetResources(
	ctx context.Context,
	client k8s.Client,
	es esv1.Elasticsearch,
	nodeSet esv1.NodeSet,
	ver version.Version,
	ipFamily corev1.IPFamily,
	setDefaultSecurityContext bool,
	policyConfig nodespec.PolicyConfig,
	meta metadata.Metadata,
	actualPodsRestartTriggerAnnotationValue string,
) (nodeSetResources, error) {
	tier, err := nodeSet.ResolvedTier()
	if err != nil {
		return nodeSetResources{}, err
	}

	if es.Spec.ObjectStore == nil {
		return nodeSetResources{}, fmt.Errorf("objectStore is required for stateless mode")
	}

	// Build stateless-specific ES config (node roles, object store, discovery, etc.)
	statelessCfg, err := settings.NewStatelessConfig(tier, *es.Spec.ObjectStore)
	if err != nil {
		return nodeSetResources{}, err
	}

	// Overlay the NodeSet-level user config on top of the stateless baseline.
	// applyUserConfigOverrides implements "user wins" semantics including for
	// list-valued tier defaults (node.roles) — see its godoc for the full
	// rationale.
	var userConfig commonv1.Config
	if nodeSet.Config != nil {
		userConfig = *nodeSet.Config
	}
	userCfg, err := common.NewCanonicalConfigFrom(userConfig.Data)
	if err != nil {
		return nodeSetResources{}, err
	}
	if _, err := applyUserConfigOverrides(statelessCfg, userCfg); err != nil {
		return nodeSetResources{}, err
	}
	mergedUserConfig := commonv1.Config{Data: make(map[string]interface{})}
	if err := statelessCfg.CanonicalConfig.Unpack(&mergedUserConfig.Data); err != nil {
		return nodeSetResources{}, err
	}

	nodeSets := esv1.NodeSetList(es.Spec.NodeSets)
	cfg, err := settings.NewMergedESConfig(
		es.Name,
		ver,
		ipFamily,
		es.Spec.HTTP,
		mergedUserConfig,
		policyConfig.ElasticsearchConfig,
		false, false, // no remote cluster server/client in stateless
		nodeSets.HasZoneAwareness(), false, // zone awareness from NodeSets, no client auth
	)
	if err != nil {
		return nodeSetResources{}, err
	}

	deploymentName := esv1.Deployment(es.Name, nodeSet.Name)

	// Build the pod template via the shared nodespec.BuildPodTemplateSpec. The
	// function branches on es.IsStateless() for mode-specific behavior
	// (ephemeral data volume, skipped pre-stop hook, tier-based labels, etc.).
	//
	// keystoreResources is nil because stateless delivers secure settings via
	// cluster_secrets in file-based settings (see Driver.reconcileSecureSettings),
	// not via the keystore init container used by stateful clusters.
	// clientAuthenticationRequired is false because client certificate
	// authentication is not yet wired for stateless.
	podTemplate, err := nodespec.BuildPodTemplateSpec(
		ctx,
		client,
		es,
		nodeSet,
		cfg,
		nil,
		setDefaultSecurityContext,
		policyConfig,
		meta,
		actualPodsRestartTriggerAnnotationValue,
		false,
	)
	if err != nil {
		return nodeSetResources{}, err
	}

	// Build Deployment
	esNsn := k8s.ExtractNamespacedName(&es)
	deploymentLabels := label.NewDeploymentLabels(esNsn, deploymentName, tier)

	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: es.Namespace,
			Name:      deploymentName,
			Labels:    deploymentLabels,
		},
		Spec: appsv1.DeploymentSpec{
			RevisionHistoryLimit: revisionHistoryLimit(es),
			Replicas:             ptr.To(nodeSet.Count),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					label.ClusterNameLabelName:    es.Name,
					label.DeploymentNameLabelName: deploymentName,
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: ptr.To(intstr.FromInt32(0)),
					MaxSurge:       ptr.To(intstr.FromString("25%")),
				},
			},
			Template: podTemplate,
		},
	}

	return nodeSetResources{
		nodeSetName: nodeSet.Name,
		tier:        tier,
		deployment:  deployment,
		config:      cfg,
	}, nil
}

// revisionHistoryLimit returns the RevisionHistoryLimit for stateless Deployments.
// Defaults to 0 (no old ReplicaSets kept) since stateless pods are ephemeral.
func revisionHistoryLimit(es esv1.Elasticsearch) *int32 {
	if es.Spec.RevisionHistoryLimit != nil {
		return es.Spec.RevisionHistoryLimit
	}
	return ptr.To[int32](0)
}
