// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package fixtures

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

const (
	// OldESImage and NewESImage pin the Elasticsearch container images used
	// by upgrade-scenario tests. They exist mostly so the test code and the
	// assertions agree, and so intentional bumps are one-liners.
	OldESImage = "docker.elastic.co/elasticsearch/elasticsearch:9.2.0"
	NewESImage = "docker.elastic.co/elasticsearch/elasticsearch:9.3.0"
)

// SeededDeployment returns a minimal Deployment that looks like what a
// previous reconciliation of the stateless driver would have created for the
// given tier, with the provided container image and status.
//
// It is intentionally hand-rolled rather than going through the driver's own
// build path so the "observed" vs "expected" gap in upgrade scenarios is
// purely about the container image and status: this isolates the tier-
// ordering behaviour the caller is usually trying to cover.
func SeededDeployment(es esv1.Elasticsearch, nodeSetName string, tier esv1.StatelessTier, image string, status appsv1.DeploymentStatus) *appsv1.Deployment {
	name := esv1.Deployment(es.Name, nodeSetName)
	labels := label.NewDeploymentLabels(
		types.NamespacedName{Namespace: es.Namespace, Name: es.Name},
		name,
		tier,
	)
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: es.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  esv1.ElasticsearchContainerName,
						Image: image,
					}},
				},
			},
		},
		Status: status,
	}
}

// RolledOutDeploymentStatus returns a DeploymentStatus that reports the
// rollout as finished: every replica is available, updated, and ready;
// ObservedGeneration is high enough that IsRolledOut returns true for any
// reasonable Generation.
func RolledOutDeploymentStatus(replicas int32) appsv1.DeploymentStatus {
	return appsv1.DeploymentStatus{
		Replicas:           replicas,
		UpdatedReplicas:    replicas,
		AvailableReplicas:  replicas,
		ReadyReplicas:      replicas,
		ObservedGeneration: 10,
	}
}

// InProgressDeploymentRolloutStatus returns a DeploymentStatus that reports
// replicas as available (so AllDeploymentsUnavailable returns false and
// grouping can kick in) but with zero updated replicas so IsRolledOut is
// false.
func InProgressDeploymentRolloutStatus(replicas int32) appsv1.DeploymentStatus {
	return appsv1.DeploymentStatus{
		Replicas:           replicas,
		UpdatedReplicas:    0,
		AvailableReplicas:  replicas,
		ReadyReplicas:      replicas,
		ObservedGeneration: 10,
	}
}
