// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/settings"
	essettings "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

// rolesSetting captures roles in the Elasticsearch configuration
type rolesSetting struct {
	Roles []string `config:"node.roles"`
}

// NamedTiers is used to hold the tiers in a manifest.
// We use the name of the associated resource policy as the key.
type NamedTiers map[string][]esv1.NodeSet

// getNamedTiers retrieves the name of all the tiers in the Elasticsearch manifest.
func getNamedTiers(client k8s.Client, es esv1.Elasticsearch, resourcePolicies commonv1.ResourcePolicies) (NamedTiers, error) {
	namedTiersSet := make(NamedTiers)
	for _, nodeSet := range es.Spec.NodeSets {
		resourcePolicy, err := getNamedTier(client, es, resourcePolicies, nodeSet)
		if err != nil {
			return nil, err
		}
		if resourcePolicy == nil {
			// This nodeSet is not managed by an autoscaling policy
			continue
		}
		namedTiersSet[*resourcePolicy.Name] = append(namedTiersSet[*resourcePolicy.Name], *nodeSet.DeepCopy())
	}
	return namedTiersSet, nil
}

// getNamedTier retrieves the resource policy associated to a NodeSet or nil if none.
func getNamedTier(
	client k8s.Client,
	es esv1.Elasticsearch,
	resourcePolicies commonv1.ResourcePolicies,
	nodeSet esv1.NodeSet,
) (*commonv1.ResourcePolicy, error) {
	// Get the config Secret
	sset := esv1.StatefulSet(es.Name, nodeSet.Name)
	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: es.Namespace,
		Name:      esv1.ConfigSecret(sset),
	}
	if err := client.Get(key, &secret); err != nil {
		return nil, err
	}
	rawCfg, exists := secret.Data[essettings.ConfigFileName]
	if !exists {
		return nil, nil
	}
	cfg, err := settings.ParseConfig(rawCfg)
	if err != nil {
		return nil, err
	}

	var r rolesSetting
	if err := cfg.Unpack(&r); err != nil {
		return nil, err
	}
	return resourcePolicies.FindByRoles(r.Roles), nil
}
