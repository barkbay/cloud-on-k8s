// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"fmt"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	commonsettings "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
)

var (
	DefaultNodeRoles = map[v1alpha1.ElasticsearchTierName][]escommon.NodeRole{
		v1alpha1.IndexTierName:  {escommon.MasterRole, escommon.IndexRole, escommon.IngestRole, escommon.RemoteClusterClientRole},
		v1alpha1.SearchTierName: { /* escommon.SearchRole, -- Re-Enabled when stateless is available */ escommon.RemoteClusterClientRole, escommon.TransformRole /* !!! Only for tests */, escommon.DataRole /* !!! Only for tests */},
		v1alpha1.MLTierName:     {escommon.MLRole, escommon.RemoteClusterClientRole},
	}
)

const (
	HealthPeriodicLoggerEnabled         = "health.periodic_logger.enabled"
	HealthPeriodicLoggerPollInterval    = "health.periodic_logger.poll_interval"
	HealthMasterIdentityChangeThreshold = "health.master_history.identity_changes_threshold"
)

func WithStatelessConfig(tier v1alpha1.ElasticsearchTierName, objectStoreConfig v1alpha1.ObjectStoreConfig, baseCfg CanonicalConfig) (CanonicalConfig, error) {
	statelessCfg, err := statelessConfig(tier, objectStoreConfig)
	if err != nil {
		return CanonicalConfig{}, err
	}
	if err := baseCfg.CanonicalConfig.MergeWith(statelessCfg.CanonicalConfig); err != nil {
		return CanonicalConfig{}, err
	}
	return baseCfg, nil
}

// statelessConfig returns the configuration bit related to stateless
func statelessConfig(tier v1alpha1.ElasticsearchTierName, objectSoreConfig v1alpha1.ObjectStoreConfig) (*CanonicalConfig, error) {
	statelessConfigAsMap := map[string]interface{}{}
	/* !!! Stateless feature is disabled temporarily since no stateless image is available !!!
		"enabled": true,
		/*"object_store.type":   objectSoreConfig.Type,
		"object_store.bucket": objectSoreConfig.Bucket,
		"object_store.client": objectSoreConfig.Client,
	}
	if objectSoreConfig.BasePath != "" {
		statelessConfigAsMap["object_store.base_path"] = objectSoreConfig.BasePath
	}
	*/

	nodeRoles, ok := DefaultNodeRoles[tier]
	if !ok {
		return nil, fmt.Errorf("cannot find default node role for tier [%s]", tier)
	}

	cfg := map[string]interface{}{
		"stateless":                 statelessConfigAsMap,
		esv1.DiscoverySeedProviders: "file",
		// to avoid misleading error messages about the inability to connect to localhost for discovery despite us using
		// file based discovery
		esv1.DiscoverySeedHosts: []string{},

		// Enable the HealthPeriodicLogger to log the output of /_health_report every 60 seconds
		HealthPeriodicLoggerEnabled:      "true",
		HealthPeriodicLoggerPollInterval: "60s",

		// Make the master_is_stable Health API indicator more tolerant of master changes in Serverless
		HealthMasterIdentityChangeThreshold: "30",

		"node.roles": nodeRoles,
	}

	return &CanonicalConfig{commonsettings.MustCanonicalConfig(cfg)}, nil
}
