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
	MaxLazyMLNodes              int32  `json:"xpack.ml.max_lazy_ml_nodes"`
	MaxMemory                   string `json:"xpack.ml.max_ml_node_size"`
	UseAutoMachineMemoryPercent bool   `json:"xpack.ml.use_auto_machine_memory_percent"`
}

type AutoScalingClient interface {
	DeleteAutoscalingAutoscalingPolicies(ctx context.Context) error
	UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error
	GetAutoscalingCapacity(ctx context.Context) (Policies, error)
	UpdateMLNodesSettings(ctx context.Context, maxLazyMLNodes int32, maxMemory string) error
}

func (c *clientV7) UpsertAutoscalingPolicy(ctx context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error {
	path := fmt.Sprintf("/_autoscaling/policy/%s", policyName)
	return c.put(ctx, path, autoscalingPolicy, nil)
}

func (c *clientV7) DeleteAutoscalingAutoscalingPolicies(ctx context.Context) error {
	return c.delete(ctx, "/_autoscaling/policy/*", nil, nil)
}

func (c *clientV7) UpdateMLNodesSettings(ctx context.Context, maxLazyMLNodes int32, maxMemory string) error {
	return c.put(
		ctx,
		"/_cluster/settings",
		&MachineLearningSettings{
			&MLSettingsGroup{
				MaxLazyMLNodes:              maxLazyMLNodes,
				MaxMemory:                   maxMemory,
				UseAutoMachineMemoryPercent: true,
			}}, nil)
}

// Policies represents autoscaling policies and decisions.
// It maps a policy name to the requirements.
type Policies struct {
	Policies map[string]PolicyInfo `json:"policies"`
}

type PolicyInfo struct {
	RequiredCapacity PolicyCapacityInfo `json:"required_capacity"`
	CurrentCapacity  PolicyCapacityInfo `json:"current_capacity"`
	CurrentNodes     []NodeInfo         `json:"current_nodes"`
}

// PolicyCapacityInfo models the capacity information, both current and required,
// as received by the autoscaling Elasticsearch API.
type PolicyCapacityInfo struct {
	Node  Capacity `yaml:"node" json:"node,omitempty"`
	Total Capacity `yaml:"total" json:"total,omitempty"`
}

type NodeInfo struct {
	Name string `json:"name"`
}

// IsEmpty returns true if all the resource values are empty (no values in the API response).
// 0 is considered as a value since deciders are allowed to return 0 to fully scale down a tier.
func (rc PolicyCapacityInfo) IsEmpty() bool {
	return rc.Node.IsEmpty() && rc.Total.IsEmpty()
}

// CapacityValue models a capacity value as received by Elasticsearch.
type CapacityValue int64

// Value return the int64 value returned by Elasticsearch. It returns 0 if no value has been set by Elasticsearch.
func (e *CapacityValue) Value() int64 {
	if e == nil {
		return 0
	}
	return int64(*e)
}

// IsEmpty returns true if the value is nil.
func (e *CapacityValue) IsEmpty() bool {
	return e == nil
}

// IsZero returns true if the value is greater than 0.
func (e *CapacityValue) IsZero() bool {
	return e.Value() == 0
}

type Capacity struct {
	Storage *CapacityValue `yaml:"storage" json:"storage,omitempty"`
	Memory  *CapacityValue `yaml:"memory" json:"memory,omitempty"`
}

// IsEmpty returns true if all the resource values are empty (no values, 0 being considered as a value).
// Expressed in a different way, it returns true if no resource as been returned in the autoscaling API response.
func (c *Capacity) IsEmpty() bool {
	if c == nil {
		return true
	}
	return c.Memory.IsEmpty() && c.Storage.IsEmpty()
}

// IsEmpty returns true if all the resource values are evaluated to 0.
// It also returns true if no value has been set, to check if the value exists in the API response see IsEmpty().
func (c *Capacity) IsZero() bool {
	if c == nil {
		return true
	}
	return c.Memory.IsZero() && c.Storage.IsZero()
}

func (c *clientV7) GetAutoscalingCapacity(ctx context.Context) (Policies, error) {
	var response Policies
	err := c.get(ctx, "/_autoscaling/capacity/", &response)
	return response, err
}
