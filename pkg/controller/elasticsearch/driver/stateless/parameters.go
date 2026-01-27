// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/shared"
)

// Parameters contains parameters for the stateless driver implementation.
type Parameters struct {
	shared.Parameters

	// ES is the ElasticsearchStateless resource to reconcile
	ES essv1alpha1.ElasticsearchStateless
}
