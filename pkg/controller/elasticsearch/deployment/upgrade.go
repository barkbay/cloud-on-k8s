// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package deployment

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/pod"
)

// ImagesInUse returns the set of Elasticsearch container images in use across
// the given Deployments. Deployments without an elasticsearch container
// contribute the empty string.
func ImagesInUse(deployments []appsv1.Deployment) sets.Set[string] {
	images := sets.New[string]()
	for i := range deployments {
		var image string
		if c := pod.ContainerByName(deployments[i].Spec.Template.Spec, esv1.ElasticsearchContainerName); c != nil {
			image = c.Image
		}
		images.Insert(image)
	}
	return images
}

// IsVersionUpgradeInProgress reports whether the observed Deployments are
// currently serving more than one Elasticsearch image, i.e. a version upgrade
// is mid-flight.
func IsVersionUpgradeInProgress(observed []appsv1.Deployment) bool {
	return ImagesInUse(observed).Len() > 1
}

// IsVersionUpgradePending reports whether applying the expected Deployments
// would start a new version upgrade: true when the expected image set contains
// at least one image not yet present on any observed Deployment. Returns false
// when there are no observed Deployments (initial bootstrap).
func IsVersionUpgradePending(expected, observed []appsv1.Deployment) bool {
	if len(observed) == 0 {
		return false
	}
	return ImagesInUse(expected).Difference(ImagesInUse(observed)).Len() > 0
}

// AllDeploymentsUnavailable reports whether none of the given Deployments has
// any available replica. An empty input slice returns true.
func AllDeploymentsUnavailable(deployments []appsv1.Deployment) bool {
	for i := range deployments {
		d := &deployments[i]
		if d.Spec.Replicas != nil && *d.Spec.Replicas > 0 && d.Status.AvailableReplicas > 0 {
			return false
		}
	}
	return true
}

// HasMasterRoleFunc reports whether a Deployment carries the Elasticsearch
// master role. Implementations typically inspect a tier label whose name
// (and accepted values) depend on the calling controller.
type HasMasterRoleFunc func(d appsv1.Deployment) bool

// ShouldGroupDeploymentReconciliation decides whether to reconcile Deployments
// in tier-ordered groups (reader tier first, writer tiers second) rather than
// all at once.
//
// It returns true when a version upgrade is in progress or pending, except in
// any of these short-circuit cases:
//   - the cluster is not yet bootstrapped — index nodes must come up right
//     away to form the cluster;
//   - all observed Deployments are unavailable — nothing to optimise;
//   - every Deployment carrying the master role is unavailable — the cluster
//     as a whole is unavailable.
//
// The hasMasterRole predicate lets the caller plug in its own tier-label
// scheme (e.g. ECK-stateless uses esv1.StatelessTier values under
// label.TierLabelName).
func ShouldGroupDeploymentReconciliation(
	ctx context.Context,
	observed, expected []appsv1.Deployment,
	isClusterBootstrapped bool,
	hasMasterRole HasMasterRoleFunc,
) bool {
	log := crlog.FromContext(ctx)
	switch {
	case !isClusterBootstrapped:
		log.V(1).Info("Reconciling all deployments at once because cluster is not bootstrapped yet")
		return false
	case AllDeploymentsUnavailable(observed):
		log.V(1).Info("Reconciling all deployments at once because all nodes are unavailable")
		return false
	case allMastersUnavailable(observed, hasMasterRole):
		log.V(1).Info("Reconciling all deployments at once because no master node is available")
		return false
	case IsVersionUpgradeInProgress(observed) || IsVersionUpgradePending(expected, observed):
		log.Info("Reconciling deployments in groups because a version upgrade is in progress or pending")
		return true
	}
	return false
}

// allMastersUnavailable reports whether every Deployment selected by
// hasMasterRole is unavailable. If no Deployment carries the master role, the
// cluster is treated as having no masters available.
func allMastersUnavailable(observed []appsv1.Deployment, hasMasterRole HasMasterRoleFunc) bool {
	withMaster := make([]appsv1.Deployment, 0, len(observed))
	for i := range observed {
		if hasMasterRole(observed[i]) {
			withMaster = append(withMaster, observed[i])
		}
	}
	if len(withMaster) == 0 {
		return true
	}
	return AllDeploymentsUnavailable(withMaster)
}

// GroupByPriorityTier splits resources into ordered reconciliation groups:
// resources whose tier matches priorityTier come first, every other
// resource comes second. The returned slice always has exactly two inner
// slices; either may be empty. Resources preserve their input order within
// each group.
//
// Callers use this to express dependency-ordered rollouts — "roll this
// tier first, delay the rest":
//   - version upgrade: priorityTier = "search" (readers roll before writers;
//     search nodes read from the shared object store while index nodes
//     write to it, so readers are safe to bump first).
//   - adding a dedicated master tier: priorityTier = "master" (quorum
//     provider rolls before the index tier sheds its master role).
//   - removing a dedicated master tier: priorityTier = "index" (the new
//     master-bearing tier rolls before the outgoing master is GC'd).
//
// tierOf extracts the tier name from a resource. It lets each caller keep
// its own resource type and field layout without having to commit to a
// shared type — ECK-stateless's `nodeSetResources` has a StatelessTier
// field, and callers satisfy this contract via a one-line accessor.
func GroupByPriorityTier[T any](resources []T, tierOf func(T) string, priorityTier string) [][]T {
	priority := make([]T, 0, len(resources))
	others := make([]T, 0, len(resources))
	for i := range resources {
		if tierOf(resources[i]) == priorityTier {
			priority = append(priority, resources[i])
		} else {
			others = append(others, resources[i])
		}
	}
	return [][]T{priority, others}
}
