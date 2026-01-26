// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stackmon

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/stackmon/monitoring"
)

// MonitoredElasticsearch is an interface that represents an Elasticsearch cluster
// (either stateful or stateless) that supports stack monitoring.
// Both Elasticsearch and ElasticsearchStateless types implement this interface.
type MonitoredElasticsearch interface {
	// Embed HasMonitoring for monitoring-specific methods
	monitoring.HasMonitoring

	// client.Object embeds metav1.Object and runtime.Object, providing access to
	// Kubernetes object metadata (GetName, GetNamespace, etc.)
	client.Object

	// GetVersion returns the Elasticsearch version.
	GetVersion() string

	// GetHTTP returns the HTTP layer configuration.
	GetHTTP() commonv1.HTTPConfig

	// IsStateless returns true if this is a stateless Elasticsearch cluster.
	IsStateless() bool
}
