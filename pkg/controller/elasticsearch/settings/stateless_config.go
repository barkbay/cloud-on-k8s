// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commonsettings "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
)

var (
	DefaultNodeRoles = map[esv1.ElasticsearchTierName][]esv1.NodeRole{
		esv1.IndexTierName:  {esv1.MasterRole, esv1.IndexRole, esv1.IngestRole, esv1.RemoteClusterClientRole},
		esv1.SearchTierName: {esv1.SearchRole, esv1.RemoteClusterClientRole, esv1.TransformRole},
		esv1.MLTierName:     {esv1.MLRole, esv1.RemoteClusterClientRole},
	}
)

const (
	HealthPeriodicLoggerEnabled         = "health.periodic_logger.enabled"
	HealthPeriodicLoggerPollInterval    = "health.periodic_logger.poll_interval"
	HealthMasterIdentityChangeThreshold = "health.master_history.identity_changes_threshold"
)

// xpackConfig returns the configuration bit related to XPack settings
func statelessConfig(tier esv1.ElasticsearchTierName, objectSoreConfig esv1.ObjectStoreConfig) (*CanonicalConfig, error) {
	// enable x-pack security, including TLS
	objectSoreConfigAsMap := map[string]interface{}{
		"object_store.type":   objectSoreConfig.Type,
		"object_store.bucket": objectSoreConfig.Bucket,
		"object_store.client": objectSoreConfig.Client,
	}
	if objectSoreConfig.BasePath != "" {
		objectSoreConfigAsMap["object_store.base_path"] = objectSoreConfig.BasePath
	}

	nodeRoles, ok := DefaultNodeRoles[tier]
	if !ok {
		return nil, fmt.Errorf("cannot find default node role for tier [%s]", tier)
	}

	cfg := map[string]interface{}{
		"stateless":                 objectSoreConfigAsMap,
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

const (
	// SecureSettingsDirName is the directory name of the file used to store secure Elasticsearch settings
	SecureSettingsDirName = "secrets"
	// SecureSettingsFileName is the name of the file used to store secure Elasticsearch settings
	SecureSettingsFileName = "secrets.json"
	// SecureSettingsHashAnnotationName is an annotation used to store a hash of the secure Elasticsearch settings
	SecureSettingsHashAnnotationName = "elasticsearch.k8s.elastic.co/secret-settings-hash"
	// SecureSettingVolumeName is the name of the volume used to specifically mount the secure settings file from the config secret
	SecureSettingVolumeName = "elastic-internal-elasticsearch-secure-settings"
)
