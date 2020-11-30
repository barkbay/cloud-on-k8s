// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"encoding/json"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
)

// NamedTiers is used to hold the tiers in a manifest.
// We use the name of the associated resource policy as the key.
type NamedTiers map[string][]esv1.NodeSet

func (n NamedTiers) String() string {
	namedTiers := make(map[string][]string, len(n))
	for namedTier, nodeSets := range n {
		for _, nodeSet := range nodeSets {
			namedTiers[namedTier] = append(namedTiers[namedTier], nodeSet.Name)
		}
	}
	namedTiersAsString, err := json.Marshal(namedTiers)
	if err != nil {
		return err.Error()
	}
	return string(namedTiersAsString)
}

// GetNamedTiers retrieves the name of all the tiers in the Elasticsearch manifest.
func GetNamedTiers(es esv1.Elasticsearch, resourcePolicies commonv1.ResourcePolicies) (NamedTiers, error) {
	namedTiersSet := make(NamedTiers)
	for _, nodeSet := range es.Spec.NodeSets {
		resourcePolicy, err := getNamedTier(es, resourcePolicies, nodeSet)
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
	es esv1.Elasticsearch,
	resourcePolicies commonv1.ResourcePolicies,
	nodeSet esv1.NodeSet,
) (*commonv1.ResourcePolicy, error) {
	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return nil, err
	}
	cfg := esv1.ElasticsearchSettings{}
	if err := esv1.UnpackConfig(nodeSet.Config, *v, &cfg); err != nil {
		return nil, err
	}
	log.V(1).Info("roles", "roles", cfg.Node.Roles)
	return resourcePolicies.FindByRoles(cfg.Node.Roles), nil
}
