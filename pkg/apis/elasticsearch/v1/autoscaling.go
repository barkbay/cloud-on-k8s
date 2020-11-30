// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/pkg/utils/set"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const ElasticsearchAutoscalingAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-policies"

// +kubebuilder:object:generate=false
type ResourcePolicies []ResourcePolicy

func (es Elasticsearch) GetResourcePolicies() (ResourcePolicies, error) {
	resourcePolicies := make(ResourcePolicies, 0)
	if len(es.AutoscalingSpec()) == 0 {
		return resourcePolicies, nil
	}
	err := json.Unmarshal([]byte(es.AutoscalingSpec()), &resourcePolicies)
	return resourcePolicies, err
}

type DeciderSettings struct {
	Deciders map[string]string `json:"deciders,omitempty"`
}

// ResourcePolicy represents how resources can be scaled by the autoscaler controller.
// +kubebuilder:object:generate=false
type ResourcePolicy struct {
	NamedAutoscalingPolicy
	AllowedResources
}

type NamedAutoscalingPolicy struct {
	AutoscalingPolicy

	// A resource policy must identified by a unique name, provided by the user.
	Name string `json:"name,omitempty"`
}

// +kubebuilder:object:generate=false
type AutoscalingPolicy struct {
	// A resource policy must be target a unique set of roles
	Roles []string `json:"roles,omitempty"`
	// Deciders allows the user to override default settings for autoscaling deciders.
	Deciders map[string]DeciderSettings `json:"deciders,omitempty"`
}

// +kubebuilder:object:generate=false
type AllowedResources struct {
	// MinAllowed represents the lower limit for the resources managed by the autoscaler.
	MinAllowed ResourcesSpecification `json:"minAllowed,omitempty"`
	// MaxAllowed represents the upper limit for the resources managed by the autoscaler.
	MaxAllowed ResourcesSpecification `json:"maxAllowed,omitempty"`
}

// +kubebuilder:object:generate=false
type ResourcesSpecification struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	Count int32 `json:"count,omitempty"`
	// Cpu represents max CPU request
	Cpu *resource.Quantity `json:"cpu"`
	// memory represents max memory request
	Memory *resource.Quantity `json:"memory"`
	// storage represents storage request
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
		rpByName[scalePolicy.Name] = scalePolicy
	}
	return rpByName
}

// NamedTiers is used to hold the tiers in a manifest.
// We use the name of the associated resource policy as the key.
type NamedTiers map[string][]NodeSet

func (n NamedTiers) String() string {
	namedTiers := make(map[string][]string, len(n))
	for namedTier, nodeSets := range n {
		for _, nodeSet := range nodeSets {
			namedTiers[namedTier] = append(namedTiers[namedTier], nodeSet.Name)
		}
	}
	namedTiersAsString, err := json.Marshal(namedTiers)
	if err != nil {
		return err.Error()
	}
	return string(namedTiersAsString)
}

// GetNamedTiers retrieves the name of all the tiers in the Elasticsearch manifest.
func (rps ResourcePolicies) GetNamedTiers(es Elasticsearch) (NamedTiers, error) {
	namedTiersSet := make(NamedTiers)
	for _, nodeSet := range es.Spec.NodeSets {
		resourcePolicy, err := rps.getNamedTier(es, nodeSet)
		if err != nil {
			return nil, err
		}
		if resourcePolicy == nil {
			// This nodeSet is not managed by an autoscaling policy
			continue
		}
		namedTiersSet[resourcePolicy.Name] = append(namedTiersSet[resourcePolicy.Name], *nodeSet.DeepCopy())
	}
	return namedTiersSet, nil
}

// getNamedTier retrieves the resource policy associated to a NodeSet or nil if none.
func (rps ResourcePolicies) getNamedTier(es Elasticsearch, nodeSet NodeSet) (*ResourcePolicy, error) {
	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return nil, err
	}
	cfg := ElasticsearchSettings{}
	if err := UnpackConfig(nodeSet.Config, *v, &cfg); err != nil {
		return nil, err
	}
	return rps.FindByRoles(cfg.Node.Roles), nil
}

func resourcePolicyIndex(index int, child string, moreChildren ...string) *field.Path {
	return field.NewPath("metadata").
		Child("annotations", `"`+ElasticsearchAutoscalingAnnotationName+`"`).
		Index(index).
		Child(child, moreChildren...)
}

func (rps ResourcePolicies) Validate() field.ErrorList {
	policyNames := set.Make()
	rolesSet := make([][]string, 0, len(rps))
	var errs field.ErrorList
	for i, resourcePolicy := range rps {
		// Validate the name field.
		if len(resourcePolicy.Name) == 0 {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "name"), "name is mandatory"))
		} else {
			if policyNames.Has(resourcePolicy.Name) {
				errs = append(errs, field.Invalid(resourcePolicyIndex(i, "name"), resourcePolicy.Name, "policy is duplicated"))
			}
			policyNames.Add(resourcePolicy.Name)
		}

		// Validate the roles.
		if resourcePolicy.Roles == nil {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "roles"), "roles is mandatory"))
		} else {
			sort.Strings(resourcePolicy.Roles)
			if ContainsStringSlide(rolesSet, resourcePolicy.Roles) {
				errs = append(errs, field.Invalid(resourcePolicyIndex(i, "name"), strings.Join(resourcePolicy.Roles, ","), "roles set is duplicated"))
			} else {
				rolesSet = append(rolesSet, resourcePolicy.Roles)
			}
		}

		if !(resourcePolicy.MinAllowed.Count >= 0) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "minAllowed", "count"), resourcePolicy.MinAllowed.Count, "count must be a positive integer"))
		}

		if !(resourcePolicy.MaxAllowed.Count > 0) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "maxAllowed", "count"), resourcePolicy.MaxAllowed.Count, "count must be an integer greater than 0"))
		}

		// Check that max count is greater or equal than min count.
		if !(resourcePolicy.MaxAllowed.Count >= resourcePolicy.MinAllowed.Count) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "maxAllowed", "count"), resourcePolicy.MaxAllowed.Count, "maxAllowed must be greater or equal than minAllowed"))
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

func ContainsStringSlide(slices [][]string, slice []string) bool {
	for _, s := range slices {
		if Equal(s, slice) {
			return true
		}
	}
	return false
}

// Equal returns true if both string slices contains the same ordered elements.
func Equal(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
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
