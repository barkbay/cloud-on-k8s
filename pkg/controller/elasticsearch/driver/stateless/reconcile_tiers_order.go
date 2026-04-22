// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

// tiersWithMasterRole lists the stateless tiers that carry the master role
// (see settings.DefaultTierRoles). When every Deployment for these tiers is
// unavailable, the cluster as a whole is considered unavailable.
var tiersWithMasterRole = map[esv1.StatelessTier]struct{}{
	esv1.IndexTier:  {},
	esv1.MasterTier: {},
}

// observedDeployments returns the existing Deployments belonging to this
// Elasticsearch cluster, matched by the cluster-name and deployment-name
// labels used for stateless Deployments.
func (d *Driver) observedDeployments(ctx context.Context) ([]appsv1.Deployment, error) {
	var list appsv1.DeploymentList
	if err := d.Client.List(ctx, &list,
		client.InNamespace(d.ES.Namespace),
		label.NewLabelSelectorForElasticsearchClusterName(d.ES.Name),
		client.HasLabels{label.DeploymentNameLabelName},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// statelessHasMasterRole is the ECK-stateless HasMasterRoleFunc used by
// deployment.ShouldGroupDeploymentReconciliation. It reports whether the
// given Deployment belongs to a stateless tier that carries the master role.
func statelessHasMasterRole(d appsv1.Deployment) bool {
	tier := esv1.StatelessTier(d.Labels[label.TierLabelName])
	_, ok := tiersWithMasterRole[tier]
	return ok
}

// nodeSetTier is the tierOf callback passed to deployment.GroupByTier. It
// returns the string representation of the NodeSet's stateless tier.
func nodeSetTier(r nodeSetResources) string {
	return string(r.tier)
}

// tierNames returns the distinct tier identifiers of the given resources, in
// sorted order, for logging. Multiple NodeSets in the same tier contribute a
// single entry.
func tierNames(resources []nodeSetResources) []string {
	names := sets.New[string]()
	for i := range resources {
		names.Insert(string(resources[i].tier))
	}
	return sets.List(names)
}
