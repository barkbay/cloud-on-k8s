// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"encoding/json"

	"github.com/elastic/cloud-on-k8s/pkg/utils/set"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const ElasticsearchAutoscalingAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-policies"

type ResourcePolicies []ResourcePolicy

func ResourcePoliciesFrom(s string) (ResourcePolicies, error) {
	resourcePolicies := make(ResourcePolicies, 0)
	err := json.Unmarshal([]byte(s), &resourcePolicies)
	return resourcePolicies, err
}

type DeciderSettings struct {
	Deciders map[string]string `json:"deciders,omitempty"`
}

// ResourcePolicy represents how resources can be scaled by the autoscaler controller.
type ResourcePolicy struct {
	NamedAutoscalingPolicy
	AllowedResources
}

type NamedAutoscalingPolicy struct {
	AutoscalingPolicy

	// A resource policy must identified by a unique name, provided by the user.
	Name *string `json:"name,omitempty"`
}

type AutoscalingPolicy struct {
	// A resource policy must be target a unique set of roles
	Roles []string `json:"roles,omitempty"`
	// Deciders allows the user to override default settings for autoscaling deciders.
	Deciders map[string]DeciderSettings `json:"deciders,omitempty"`
}

type AllowedResources struct {
	// MinAllowed represents the lower limit for the resources managed by the autoscaler.
	MinAllowed ResourcesSpecification `json:"minAllowed,omitempty"`
	// MaxAllowed represents the upper limit for the resources managed by the autoscaler.
	MaxAllowed ResourcesSpecification `json:"maxAllowed,omitempty"`
}

type ResourcesSpecification struct {
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

// FindByRoles returns the policy associated to a set of roles or nil if not found.
func (rps ResourcePolicies) FindByRoles(roles []string) *ResourcePolicy {
	for _, rp := range rps {
		if len(rp.Roles) != len(roles) {
			continue
		}
		rolesInPolicy := set.Make(rp.Roles...)
		for _, role := range roles {
			if !rolesInPolicy.Has(role) {
				continue
			}
		}
		return &rp
	}
	return nil
}

// FindByRoles returns the policy associated to a set of roles or nil if not found.
func (rps ResourcePolicies) ByNames() map[string]ResourcePolicy {
	rpByName := make(map[string]ResourcePolicy)
	for _, scalePolicy := range rps {
		scalePolicy := scalePolicy
		rpByName[*scalePolicy.Name] = scalePolicy
	}
	return rpByName
}

func resourcePolicyIndex(index int, child string, moreChildren ...string) *field.Path {
	return field.NewPath("metadata").
		Child("annotations", `"`+ElasticsearchAutoscalingAnnotationName+`"`).
		Index(index).
		Child(child, moreChildren...)
}

// TODO: detect duplicated policies (same roles, same names)
func (rps ResourcePolicies) Validate() field.ErrorList {
	var errs field.ErrorList
	for i, resourcePolicy := range rps {
		// name field is mandatory.
		if resourcePolicy.Roles == nil {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "name"), "name is mandatory"))
		}
		// roles field is mandatory.
		if resourcePolicy.Roles == nil {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "roles"), "roles is mandatory"))
		}

		// All fields are mandatory
		if resourcePolicy.MinAllowed.Count == nil {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "minAllowed", "count"), "count field is mandatory"))
		} else if !(*resourcePolicy.MinAllowed.Count >= 0) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "minAllowed", "count"), *resourcePolicy.MinAllowed.Count, "count must be a positive integer"))
		}
		if resourcePolicy.MaxAllowed.Count == nil {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "maxAllowed", "count"), "count field is mandatory"))
		} else if !(*resourcePolicy.MaxAllowed.Count >= 0) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "maxAllowed", "count"), *resourcePolicy.MaxAllowed.Count, "count must be a positive integer"))
		}

		// Check that max count is greater or equal than min count.
		if resourcePolicy.MaxAllowed.Count != nil && resourcePolicy.MinAllowed.Count != nil &&
			!(*resourcePolicy.MaxAllowed.Count >= *resourcePolicy.MinAllowed.Count) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "maxAllowed", "count"), *resourcePolicy.MaxAllowed.Count, "maxAllowed must be greater or equal than minAllowed"))
		}

		// Check CPU
		errs = validateQuantities(errs, resourcePolicy.MinAllowed.Cpu, resourcePolicy.MaxAllowed.Cpu, i, "cpu")

		// Check Memory
		errs = validateQuantities(errs, resourcePolicy.MinAllowed.Memory, resourcePolicy.MaxAllowed.Memory, i, "memory")

		// Check storage
		errs = validateQuantities(errs, resourcePolicy.MinAllowed.Storage, resourcePolicy.MaxAllowed.Storage, i, "storage")
	}
	return errs
}

func validateQuantities(errs field.ErrorList, min, max *resource.Quantity, index int, resource string) field.ErrorList {
	var quantityErrs field.ErrorList
	if min == nil {
		quantityErrs = append(quantityErrs, field.Required(resourcePolicyIndex(index, "minAllowed", "cpu"), "cpu field is mandatory"))
	}
	if max == nil {
		quantityErrs = append(quantityErrs, field.Required(resourcePolicyIndex(index, "maxAllowed", "cpu"), "cpu field is mandatory"))
	}
	if len(quantityErrs) > 0 {
		return append(errs, quantityErrs...)
	}
	if !(max.Cmp(*min) >= 0) {
		errs = append(quantityErrs, field.Invalid(resourcePolicyIndex(index, "maxAllowed", resource), (*max).String(), "maxAllowed must be greater or equal than minAllowed"))
	}
	return append(errs, quantityErrs...)
}
