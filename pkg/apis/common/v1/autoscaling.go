// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// ScalePolicy represents how resources can be scaled by the autoscaler controller.
type ScalePolicy struct {
	Roles []string `json:"roles"`
	// MinAllowed represents the lower limit for the resources managed by the autoscaler.
	MinAllowed ResourcePolicy `json:"minAllowed,omitempty"`
	// MaxAllowed represents the upper limit for the resources managed by the autoscaler.
	MaxAllowed ResourcePolicy `json:"maxAllowed,omitempty"`
}

type ResourcePolicy struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	Count *int32 `json:"count,omitempty"`
	// Cpu represents a CPU limits
	Cpu *resource.Quantity `json:"cpu"`
	// memory represents a CPU limits
	Memory *resource.Quantity `json:"memory"`
}
