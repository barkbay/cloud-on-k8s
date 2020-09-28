// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package client

import (
	"context"
	"fmt"
)

type AutoScalingClient interface {
	UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy AutoscalingPolicy) error
	GetAutoscalingDecisions(ctx context.Context) (Decisions, error)
}

// AutoscalingPolicy represents an autoscaling policy.
type AutoscalingPolicy struct {
	Policy Policy `json:"policy"`
}

type Policy struct {
	Deciders map[string]DeciderConfiguration `json:"deciders"`
}

func (c *clientV7) UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy AutoscalingPolicy) error {
	path := fmt.Sprintf("/_autoscaling/policy/%s", policyName)
	return c.put(ctx, path, autoscalingPolicy, nil)
}

type DeciderConfiguration map[string]string

// Decisions represents autoscaling decisions
type Decisions struct {
	Decisions []Decision `json:"decisions"`
}

type Decision struct {
	Tier             string           `json:"tier"`
	RequiredCapacity RequiredCapacity `json:"required_capacity"`
}

type RequiredCapacity struct {
	Node Capacity `yaml:"node" json:"node,omitempty"`
	Tier Capacity `yaml:"tier" json:"tier,omitempty"`
}

type Capacity struct {
	Storage string `yaml:"storage" json:"storage,omitempty"`
	Memory  string `yaml:"memory" json:"memory,omitempty"`
}

func (c *clientV7) GetAutoscalingDecisions(ctx context.Context) (Decisions, error) {
	var response Decisions
	err := c.get(ctx, "/_autoscaling/decision/", &response)
	return response, err
}
