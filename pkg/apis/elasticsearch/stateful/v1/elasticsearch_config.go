// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1

import (
	"github.com/elastic/go-ucfg"
	"k8s.io/utils/ptr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
)

// ClusterSettings is the cluster node in elasticsearch.yml.
type ClusterSettings struct {
	InitialMasterNodes []string `config:"initial_master_nodes"`
}

// ElasticsearchSettings is a typed subset of elasticsearch.yml for purposes of the operator.
type ElasticsearchSettings struct {
	Node    *escommon.Node  `config:"node"`
	Cluster ClusterSettings `config:"cluster"`
}

// DefaultCfg is an instance of ElasticsearchSettings with defaults set as they are in Elasticsearch.
// cfg is the user provided config we want defaults for, ver is the version of Elasticsearch.
func DefaultCfg(ver version.Version) ElasticsearchSettings {
	settings := ElasticsearchSettings{
		// Values below only make sense if there is no "node.roles" in the configuration provided by the user
		Node: &escommon.Node{
			Master: ptr.To[bool](true),
			Data:   ptr.To[bool](true),
			Ingest: ptr.To[bool](true),
			ML:     ptr.To[bool](true),
		},
	}

	configureTransformRole(&settings, ver)

	return settings
}

// UnpackConfig unpacks Config into a typed subset.
func UnpackConfig(c *commonv1.Config, ver version.Version, out *ElasticsearchSettings) error {
	if c == nil {
		return nil
	}

	config, err := ucfg.NewFrom(c.Data, commonv1.CfgOptions...)
	if err != nil {
		return err
	}

	if err := config.Unpack(out, commonv1.CfgOptions...); err != nil {
		return err
	}

	configureTransformRole(out, ver)

	return nil
}

// configureTransformRole explicitly sets the transform role to false if the version is below 7.7.0
func configureTransformRole(cfg *ElasticsearchSettings, ver version.Version) {
	// nothing to do if the version is above 7.7.0 as the transform role is automatically applied to data nodes by the HasTransformRole method.
	if ver.GTE(version.From(7, 7, 0)) {
		return
	}

	if cfg.Node == nil {
		cfg.Node = &escommon.Node{}
	}

	cfg.Node.Transform = ptr.To[bool](false)
}
