// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package client

import (
	"context"
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
)

type AutoScalingClient interface {
	DeleteAutoscalingAutoscalingPolicies(ctx context.Context) error
	UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error
	GetAutoscalingCapacity(ctx context.Context) (Policies, error)
}

type DeciderConfiguration map[string]string

func (c *clientV7) UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error {
	path := fmt.Sprintf("/_autoscaling/policy/%s", policyName)
	return c.put(ctx, path, autoscalingPolicy, nil)
}

func (c *clientV7) DeleteAutoscalingAutoscalingPolicies(ctx context.Context) error {
	return c.delete(ctx, "/_autoscaling/policy/*", nil, nil)
}

// Policies represents autoscaling policies and decisions.
// It maps a policy name to the requirements.
type Policies struct {
	Policies map[string]PolicyInfo `json:"policies"`
}

type PolicyInfo struct {
	RequiredCapacity RequiredCapacity `json:"required_capacity"`
}

type RequiredCapacity struct {
	Node  Capacity `yaml:"node" json:"node,omitempty"`
	Total Capacity `yaml:"total" json:"total,omitempty"`
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
