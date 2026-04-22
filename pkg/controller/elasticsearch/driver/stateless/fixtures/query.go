// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package fixtures

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
)

// RequireDeploymentExists fails the test unless a Deployment with the given
// namespaced name exists on the fake client.
func RequireDeploymentExists(t *testing.T, c client.Client, namespace, name string) {
	t.Helper()
	var got appsv1.Deployment
	err := c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, &got)
	require.NoError(t, err, "expected Deployment %s/%s to exist", namespace, name)
}

// RequireDeploymentAbsent fails the test unless the given Deployment is gone
// (the GC path for stale Deployments). Any error other than NotFound bubbles
// up so the test stays honest.
func RequireDeploymentAbsent(t *testing.T, c client.Client, namespace, name string) {
	t.Helper()
	var got appsv1.Deployment
	err := c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, &got)
	require.Error(t, err, "expected Deployment %s/%s to be gone", namespace, name)
	require.True(t, apierrors.IsNotFound(err), "expected NotFound, got %v", err)
}

// DeploymentImage returns the elasticsearch container image of the named
// Deployment in the given namespace. Used by upgrade-tier assertions to show
// at a glance which tier was updated by the reconcile pass.
func DeploymentImage(t *testing.T, c client.Client, namespace, name string) string {
	t.Helper()
	var got appsv1.Deployment
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, &got))
	for _, ct := range got.Spec.Template.Spec.Containers {
		if ct.Name == esv1.ElasticsearchContainerName {
			return ct.Image
		}
	}
	t.Fatalf("elasticsearch container not found in Deployment %s/%s", namespace, name)
	return ""
}

// ConfigSecretRef returns the name of the immutable config Secret mounted at
// the elastic-internal-elasticsearch-config volume of the named Deployment.
// This is what the config-rotation scenario compares across two passes.
func ConfigSecretRef(t *testing.T, c client.Client, namespace, name string) string {
	t.Helper()
	var got appsv1.Deployment
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, &got))
	const volName = "elastic-internal-elasticsearch-config"
	for _, v := range got.Spec.Template.Spec.Volumes {
		if v.Name == volName && v.Secret != nil {
			return v.Secret.SecretName
		}
	}
	t.Fatalf("Deployment %s/%s has no %s secret volume", namespace, name, volName)
	return ""
}

// FindContainer returns a pointer to the container with the given name in
// the pod spec, or nil when absent. A tiny convenience used by tests that
// need to peek at a specific container (typically the Elasticsearch main
// container).
func FindContainer(pod corev1.PodSpec, name string) *corev1.Container {
	for i := range pod.Containers {
		if pod.Containers[i].Name == name {
			return &pod.Containers[i]
		}
	}
	return nil
}

// HasEnv returns true when the container defines an env var with the given
// name (regardless of its value).
func HasEnv(c *corev1.Container, name string) bool {
	if c == nil {
		return false
	}
	for _, e := range c.Env {
		if e.Name == name {
			return true
		}
	}
	return false
}

// AggregateReconcileErr flattens a *reconciler.Results error aggregate. Used
// in helper messages — callers still gate on HasError/HasRequeue themselves.
func AggregateReconcileErr(res *reconciler.Results) error {
	_, err := res.Aggregate()
	return err
}
