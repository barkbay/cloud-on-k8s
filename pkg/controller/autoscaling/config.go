// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"sort"
	"strings"

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

type NamedTiers map[string][]esv1.NodeSet

// getNamedTiers retrieves the name of all the named tiers in the Elasticsearch manifest.
func getNamedTiers(client k8s.Client, es esv1.Elasticsearch) (NamedTiers, error) {
	namedTiersSet := make(NamedTiers)
	for _, nodeSet := range es.Spec.NodeSets {
		namedTier, err := getNamedTier(client, es, nodeSet)
		if err != nil {
			return nil, err
		}
		namedTiersSet[namedTier] = append(namedTiersSet[namedTier], *nodeSet.DeepCopy())
	}
	return namedTiersSet, nil
}

// getNamedTier computes the name of the named tier from a NodeSet.
func getNamedTier(client k8s.Client, es esv1.Elasticsearch, nodeSet esv1.NodeSet) (string, error) {
	// Get the config Secret
	sset := esv1.StatefulSet(es.Name, nodeSet.Name)
	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: es.Namespace,
		Name:      esv1.ConfigSecret(sset),
	}
	if err := client.Get(key, &secret); err != nil {
		return "", err
	}
	rawCfg, exists := secret.Data[essettings.ConfigFileName]
	if !exists {
		return "", nil
	}
	cfg, err := settings.ParseConfig(rawCfg)
	if err != nil {
		return "", err
	}

	var r rolesSetting
	if err := cfg.Unpack(&r); err != nil {
		return "", err
	}
	sort.Strings(r.Roles)
	return namedTierName(r.Roles), nil
}

func namedTierName(roles []string) string {
	sort.Strings(roles)
	return strings.Join(roles, "_")
}
