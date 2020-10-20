// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourcePolicy represents how resources can be scaled by the autoscaler controller.
type ResourcePolicy struct {
	// +kubebuilder:validation:Required
	Roles []string `json:"roles"`
	// MinAllowed represents the lower limit for the resources managed by the autoscaler.
	// +kubebuilder:validation:Required
	MinAllowed AllowedResources `json:"minAllowed,omitempty"`
	// MaxAllowed represents the upper limit for the resources managed by the autoscaler.
	// +kubebuilder:validation:Required
	MaxAllowed AllowedResources `json:"maxAllowed,omitempty"`
}

type AllowedResources struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	// +kubebuilder:validation:Required
	Count *int32 `json:"count,omitempty"`
	// Cpu represents max CPU request
	// +kubebuilder:validation:Required
	Cpu *resource.Quantity `json:"cpu"`
	// memory represents max memory request
	// +kubebuilder:validation:Required
	Memory *resource.Quantity `json:"memory"`
	// storage represents storage request
	// +kubebuilder:validation:Required
	Storage *resource.Quantity `json:"storage"`
}
