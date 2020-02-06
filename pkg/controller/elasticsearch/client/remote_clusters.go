// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package client

import (
	"context"
)

// RemoteClusterLister captures Elasticsearch API calls around remote clusters.
type RemoteClusterLister interface {
	GetRemoteClusters() ([]string, error)
}

func (c *clientV6) GetRemoteClusters() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReqTimeout)
	defer cancel()
	var settings Settings
	if err := c.get(ctx, "/_cluster/settings", &settings); err != nil {
		return nil, err
	}
	var clusters []string
	return clusters, nil
}
