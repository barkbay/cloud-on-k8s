// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package client

import (
	"context"
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
)

// MachineLearningSettings is used to build a request to update ML related settings for autoscaling.
type MachineLearningSettings struct {
	PersistentSettings *MLSettingsGroup `json:"persistent,omitempty"`
}

// SettingsGroup is a group of persistent settings.
type MLSettingsGroup struct {
	MaxLazyMLNodes int32 `json:"xpack.ml.max_lazy_ml_nodes"`
}

type AutoScalingClient interface {
	DeleteAutoscalingAutoscalingPolicies(ctx context.Context) error
	UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error
	GetAutoscalingCapacity(ctx context.Context) (Policies, error)
	UpdateMaxLazyMLNodes(ctx context.Context, maxLazyMLNodes int32) error
}

func (c *clientV7) UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error {
	path := fmt.Sprintf("/_autoscaling/policy/%s", policyName)
	return c.put(ctx, path, autoscalingPolicy, nil)
}

func (c *clientV7) DeleteAutoscalingAutoscalingPolicies(ctx context.Context) error {
	return c.delete(ctx, "/_autoscaling/policy/*", nil, nil)
}

func (c *clientV7) UpdateMaxLazyMLNodes(ctx context.Context, maxLazyMLNodes int32) error {
	return c.put(
		ctx,
		"/_cluster/settings",
		&MachineLearningSettings{&MLSettingsGroup{MaxLazyMLNodes: maxLazyMLNodes}}, nil)
}

// Policies represents autoscaling policies and decisions.
// It maps a policy name to the requirements.
type Policies struct {
	Policies map[string]PolicyInfo `json:"policies"`
}

type PolicyInfo struct {
	RequiredCapacity CapacityInfo `json:"required_capacity"`
	CurrentCapacity  CapacityInfo `json:"current_capacity"`
	CurrentNodes     []NodeInfo   `json:"current_nodes"`
}

type CapacityInfo struct {
	Node  Capacity `yaml:"node" json:"node,omitempty"`
	Total Capacity `yaml:"total" json:"total,omitempty"`
}

type NodeInfo struct {
	Name string `json:"name"`
}

func (rc CapacityInfo) IsEmpty() bool {
	return rc.Node.Memory == nil && rc.Node.Storage == nil &&
		rc.Total.Memory == nil && rc.Total.Storage == nil
}

type Capacity struct {
	Storage *int64 `yaml:"storage" json:"storage,omitempty"`
	Memory  *int64 `yaml:"memory" json:"memory,omitempty"`
}

func (c *clientV7) GetAutoscalingCapacity(ctx context.Context) (Policies, error) {
	var response Policies
	err := c.get(ctx, "/_autoscaling/capacity/", &response)
	return response, err
}
