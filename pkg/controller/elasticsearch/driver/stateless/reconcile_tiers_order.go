// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/bootstrap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/slices"
)

func (sd *statelessDriver) observedDeployments(ctx context.Context) ([]*appsv1.Deployment, error) {
	observedDeployments := make([]*appsv1.Deployment, 0, len(esv1.AllElasticsearchTierNames))
	for _, tier := range esv1.AllElasticsearchTierNames {
		deploymentName := esv1.PodsControllerResourceName(sd.ES.Name, string(tier))
		deployment := &appsv1.Deployment{}
		err := sd.Client.Get(
			ctx,
			client.ObjectKey{Namespace: sd.ES.Namespace, Name: deploymentName},
			deployment,
		)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		observedDeployments = append(observedDeployments, deployment)
	}
	return observedDeployments, nil
}

// shouldGroupDeploymentReconciliation returns a boolean indicating when to reconcile deployments in the right order to enable smooth upgrades (reader before writer).
// We group whenever there is a version upgrade happening or pending with the following exceptions:
// * the cluster is not yet bootstrapped (we need index nodes right away to form a cluster)
// * all deployments are unavailable and there is therefore no point in optimising the rollout
// * all deployments with master role are unavailable which means the cluster as a whole is unavailable
func (sd *statelessDriver) shouldGroupDeploymentReconciliation(
	ctx context.Context,
	observedDeployments []*appsv1.Deployment,
	expectedDeployments []*appsv1.Deployment,
) bool {
	isClusterBootstrapped := bootstrap.AnnotatedForBootstrap(sd.ES)
	log := crlog.FromContext(ctx)
	switch {
	case !isClusterBootstrapped:
		log.Info("Reconciling all deployments at once because cluster is not bootstrapped yet")
		return false
	case allDeploymentsUnavailable(observedDeployments):
		log.Info("Reconciling all deployments at once because all nodes are unavailable")
		return false
	case allMastersUnavailable(observedDeployments):
		log.Info("Reconciling all deployments at once because no master node is available")
		return false
	case isVersionUpgradeInProgress(observedDeployments) || isVersionUpgradePending(expectedDeployments, observedDeployments):
		log.Info("Reconciling deployments in groups because a version upgrade is in progress or pending")
		return true
	}
	return false
}

func deploymentsWithMasterRole(all []*appsv1.Deployment) []*appsv1.Deployment {
	return slices.Filter(all, func(d *appsv1.Deployment) bool {
		return esv1.TiersWithMasterRole.Has(esv1.NodeRole(d.Labels[label.TierLabelName]))
	})
}

func allDeploymentsUnavailable(all []*appsv1.Deployment) bool {
	for _, d := range all {
		// all unavailable is equivalent to !exists(oneAvailableDeployment)
		if d.Spec.Replicas != nil && *(d.Spec.Replicas) > 0 && d.Status.AvailableReplicas > 0 {
			return false
		}
	}
	return true
}

func allMastersUnavailable(currentDeployments []*appsv1.Deployment) bool {
	withMaster := deploymentsWithMasterRole(currentDeployments)
	if len(withMaster) == 0 {
		return true
	}
	return allDeploymentsUnavailable(withMaster)
}

func isVersionUpgradeInProgress(observedDeployments []*appsv1.Deployment) bool {
	// if we have more than one image in use an upgrade is in progress
	return imagesInUse(observedDeployments).Len() > 1
}

func elasticsearchContainerImage(d *appsv1.Deployment) string {
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == esv1.ElasticsearchContainerName {
			return c.Image
		}
	}
	return ""
}

func isVersionUpgradePending(expected []*appsv1.Deployment, observedDeployments []*appsv1.Deployment) bool {
	if len(observedDeployments) == 0 {
		return false
	}
	// if there is a difference in the images between expected and currently existing deployments it means
	// that once we apply the expected deployments a version upgrade will start
	return imagesInUse(expected).Difference(imagesInUse(observedDeployments)).Len() > 0
}

func imagesInUse(deps []*appsv1.Deployment) sets.Set[string] {
	images := sets.New[string]()
	for _, d := range deps {
		images.Insert(elasticsearchContainerImage(d))
	}
	return images
}
