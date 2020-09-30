// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/docker/go-units"

	"k8s.io/apimachinery/pkg/api/resource"
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
	Storage *resource.Quantity `yaml:"storage" json:"storage,omitempty"`
	Memory  *resource.Quantity `yaml:"memory" json:"memory,omitempty"`
}

type ElasticsearchCapacity struct {
	Storage string `json:"storage,omitempty"`
	Memory  string `json:"memory,omitempty"`
}

func (c *Capacity) UnmarshalJSON(data []byte) error {
	var ec ElasticsearchCapacity
	if err := json.Unmarshal(data, &ec); err != nil {
		return err
	}

	memory, err := units.FromHumanSize(ec.Memory)
	if err != nil {
		return fmt.Errorf("unable to parse memory quantity %s", ec.Memory)
	}
	c.Memory = resource.NewQuantity(memory, resource.DecimalSI)

	storage, err := units.FromHumanSize(ec.Storage)
	if err != nil {
		return fmt.Errorf("unable to parse storage quantity %s", ec.Storage)
	}
	c.Storage = resource.NewQuantity(storage, resource.DecimalSI)

	return nil
}

func (c *clientV7) GetAutoscalingDecisions(ctx context.Context) (Decisions, error) {
	var response Decisions
	err := c.get(ctx, "/_autoscaling/decision/", &response)
	return response, err
}
