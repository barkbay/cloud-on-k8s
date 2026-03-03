// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// podsForTierResources returns all pods matching the deployment selectors for the given tier resources.
func podsForTierResources(c k8s.Client, namespace string, resources []*TierResources) ([]corev1.Pod, error) {
	var allPods []corev1.Pod
	for _, resource := range resources {
		pods, err := k8s.PodsMatchingLabels(c, namespace, resource.deployment.Spec.Selector.MatchLabels)
		if err != nil {
			return nil, err
		}
		allPods = append(allPods, pods...)
	}
	return allPods, nil
}

// isDeploymentGroupFullyReconciled returns true if all deployments in the group are rolled out
// and there are no terminating pods.
func isDeploymentGroupFullyReconciled(ctx context.Context, deployments []*appsv1.Deployment, pods []corev1.Pod) bool {
	return allDeploymentsRolledOut(ctx, deployments) && noTerminatingPods(ctx, pods)
}

func allDeploymentsRolledOut(ctx context.Context, deployments []*appsv1.Deployment) bool {
	log := crlog.FromContext(ctx)
	for _, d := range deployments {
		if !isDeploymentRolledOut(d) {
			var desired string
			if d.Spec.Replicas != nil {
				desired = fmt.Sprintf(" desired=%d", *d.Spec.Replicas)
			}
			log.Info(
				fmt.Sprintf("Deployment %s not rolled out: updated=%d available=%d total=%d%s",
					d.Name, d.Status.UpdatedReplicas, d.Status.AvailableReplicas, d.Status.Replicas, desired),
				"deployment", d.Name,
				"tier", d.Labels[label.TierLabelName],
				"generation", d.Generation,
				"observedGeneration", d.Status.ObservedGeneration,
			)
			return false
		}
	}
	return true
}

// isDeploymentRolledOut returns true if the deployment has completed its rollout.
func isDeploymentRolledOut(d *appsv1.Deployment) bool {
	if d == nil {
		return false
	}
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.UpdatedReplicas == desired &&
		d.Status.Replicas == desired &&
		d.Status.AvailableReplicas == desired &&
		d.Status.UnavailableReplicas == 0
}

func noTerminatingPods(ctx context.Context, pods []corev1.Pod) bool {
	log := crlog.FromContext(ctx)
	for _, pod := range pods {
		if !pod.DeletionTimestamp.IsZero() {
			log.Info(
				fmt.Sprintf("Pod %s is terminating", pod.Name),
				"pod", pod.Name,
				"tier", pod.Labels[label.TierLabelName],
				"deletionTimestamp", pod.DeletionTimestamp.Time,
			)
			return false
		}
	}
	return true
}
