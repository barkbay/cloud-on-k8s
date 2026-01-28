// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

func (d *Driver) expectationsSatisfied(ctx context.Context) (bool, string, error) {
	// check if actual Deployments match our expectations before applying any change
	ok, reason, err := d.Expectations.Satisfied()
	if err != nil {
		return false, reason, err
	}
	if !ok {
		ulog.FromContext(ctx).Info("Cache expectations are not satisfied yet, re-queueing", "namespace", d.ES.Namespace, "es_name", d.ES.Name, "reason", reason)
		return false, reason, nil
	}

	// check if all Deployments most recent generation is observed before applying any change.
	deployments := appsv1.DeploymentList{}
	if err := d.Client.List(ctx, &deployments, client.InNamespace(d.ES.Namespace), label.NewLabelSelectorForElasticsearchStatelessClusterName(d.ES.Name)); err != nil {
		return false, "", err
	}
	// sort Deployments by name to have a stable returned result
	sort.Slice(deployments.Items, func(i, j int) bool {
		return deployments.Items[i].Name < deployments.Items[j].Name
	})
	for _, deployment := range deployments.Items {
		if deployment.Generation != deployment.Status.ObservedGeneration {
			ulog.FromContext(ctx).Info("Waiting for Deployment to be observed before applying further changes", "deployment.name", k8s.ExtractNamespacedName(&deployment))
			return false, fmt.Sprintf("Waiting for Deployment %s/%s generation %d to be observed", deployment.Namespace, deployment.Name, deployment.Generation), nil
		}
	}
	return true, "", nil
}
