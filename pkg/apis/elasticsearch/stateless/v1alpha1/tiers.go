// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/util/sets"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
)

type ElasticsearchTierName string

const (
	MLTierName     ElasticsearchTierName = "ml"
	SearchTierName ElasticsearchTierName = "search"
	IndexTierName  ElasticsearchTierName = "index"
)

// TiersWithMasterRole
// Index nodes have master role only in the absence of a dedicated master tier (this is the default in serverless).
var TiersWithMasterRole = sets.New(escommon.MasterRole, escommon.IndexRole)

var AllElasticsearchTierNames = []ElasticsearchTierName{
	IndexTierName,
	SearchTierName,
	MLTierName,
}
