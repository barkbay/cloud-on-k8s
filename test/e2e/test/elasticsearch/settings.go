// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package elasticsearch

import (
	"github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	common "github.com/elastic/cloud-on-k8s/pkg/controller/common/settings"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/settings"
)

func MustNumDataNodes(es v1alpha1.Elasticsearch) int {
	var numNodes int
	for _, n := range es.Spec.Nodes {
		if isDataNode(n) {
			numNodes += int(n.NodeCount)
		}
	}
	return numNodes
}

func isDataNode(node v1alpha1.NodeSpec) bool {
	if node.Config == nil {
		return true // if not specified all node type fields are true
	}
	config, err := common.NewCanonicalConfigFrom(node.Config.Data)
	if err != nil {
		panic(err)
	}
	nodeCfg, err := settings.CanonicalConfig{
		CanonicalConfig: config,
	}.Unpack()
	if err != nil {
		panic(err)
	}
	return nodeCfg.Node.Data
}
