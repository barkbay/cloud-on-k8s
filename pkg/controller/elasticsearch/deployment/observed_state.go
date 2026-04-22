// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package deployment

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// IsRolledOut reports whether the given Deployment has completed its latest
// rollout: all replicas are updated, available, and the controller has
// observed the latest generation.
func IsRolledOut(d *appsv1.Deployment) bool {
	if d == nil || d.Spec.Replicas == nil {
		return false
	}
	return d.Status.UpdatedReplicas == *d.Spec.Replicas &&
		d.Status.Replicas == *d.Spec.Replicas &&
		d.Status.AvailableReplicas == *d.Spec.Replicas &&
		d.Status.ObservedGeneration >= d.Generation
}

// GC deletes Deployments in ns matching the given selectors whose name is not
// present in expectedNames. It is the caller's responsibility to pass a
// selector narrow enough that only Deployments managed by this controller are
// returned.
func GC(ctx context.Context, c k8s.Client, ns string, expectedNames sets.Set[string], opts ...client.ListOption) error {
	var list appsv1.DeploymentList
	listOpts := append([]client.ListOption{client.InNamespace(ns)}, opts...)
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return err
	}
	for i := range list.Items {
		d := &list.Items[i]
		if expectedNames.Has(d.Name) {
			continue
		}
		// Tolerate 404s: the Deployment may have been removed by another
		// actor (manual delete, racing reconcile) between List and Delete —
		// the desired end state is already achieved.
		if err := c.Delete(ctx, d); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
