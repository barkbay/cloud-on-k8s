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

const ElasticsearchAutoscalingSpecAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-spec"

// +kubebuilder:object:generate=false
type AutoscalingSpecs []AutoscalingSpec

// GetAutoscalingSpecifications unmarshal autoscaling specifications from an Elasticsearch resource.
func (es Elasticsearch) GetAutoscalingSpecifications() (AutoscalingSpecs, error) {
	autoscalingSpecs := make(AutoscalingSpecs, 0)
	if len(es.AutoscalingSpec()) == 0 {
		return autoscalingSpecs, nil
	}
	err := json.Unmarshal([]byte(es.AutoscalingSpec()), &autoscalingSpecs)
	return autoscalingSpecs, err
}

// AutoscalingSpec represents how resources can be scaled by the autoscaler controller.
// +kubebuilder:object:generate=false
type AutoscalingSpec struct {
	NamedAutoscalingPolicy
	AllowedResources
}

// +kubebuilder:object:generate=false
type NamedAutoscalingPolicy struct {
	// A resource policy must be identified by a unique name, provided by the user.
	Name string `json:"name,omitempty"`

	AutoscalingPolicy
}

// DeciderSettings allows the user to tweak autoscaling deciders.
// +kubebuilder:object:generate=false
type DeciderSettings map[string]string

// +kubebuilder:object:generate=false
type AutoscalingPolicy struct {
	// A resource policy must target a unique set of roles
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

func (ar AllowedResources) IsMemoryDefined() bool {
	return ar.MinAllowed.IsMemoryDefined() && ar.MaxAllowed.IsMemoryDefined()
}

func (ar AllowedResources) IsCpuDefined() bool {
	return ar.MinAllowed.IsCpuDefined() && ar.MaxAllowed.IsCpuDefined()
}

func (ar AllowedResources) IsStorageDefined() bool {
	return ar.MinAllowed.IsStorageDefined() && ar.MaxAllowed.IsStorageDefined()
}

// ResourcesSpecification represents a set of resource specifications which can be used to describe
// either as a lower or an upper limit.
// +kubebuilder:object:generate=false
type ResourcesSpecification struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	Count int32 `json:"count"`
	// Cpu represents the CPU value
	Cpu *resource.Quantity `json:"cpu"`
	// memory represents the memory value
	Memory *resource.Quantity `json:"memory"`
	// storage represents the storage capacity value
	Storage *resource.Quantity `json:"storage"`
}

func (rs ResourcesSpecification) IsMemoryDefined() bool {
	return rs.Memory != nil
}

func (rs ResourcesSpecification) IsCpuDefined() bool {
	return rs.Cpu != nil
}

func (rs ResourcesSpecification) IsStorageDefined() bool {
	return rs.Storage != nil
}

func (rs ResourcesSpecification) Merge(other ResourcesSpecification) {
	if rs.Count < other.Count {
		rs.Count = other.Count
	}
	if rs.Cpu == nil || (other.Cpu != nil && rs.Cpu.Cmp(*other.Cpu) < 0) {
		rs.Cpu = other.Cpu
	}
	if rs.Memory == nil || (other.Memory != nil && rs.Memory.Cmp(*other.Memory) < 0) {
		rs.Memory = other.Memory
	}
	if rs.Storage == nil || (other.Storage != nil && rs.Storage.Cmp(*other.Storage) < 0) {
		rs.Storage = other.Storage
	}
}

// FindByRoles returns the autoscaling specification associated with a set of roles or nil if not found.
func (as AutoscalingSpecs) FindByRoles(roles []string) *AutoscalingSpec {
	for _, rp := range as {
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

// ByNames returns autoscaling specifications indexed by name.
func (as AutoscalingSpecs) ByNames() map[string]AutoscalingSpec {
	rpByName := make(map[string]AutoscalingSpec)
	for _, scalePolicy := range as {
		scalePolicy := scalePolicy
		rpByName[scalePolicy.Name] = scalePolicy
	}
	return rpByName
}

// NamedTiers is used to hold the tiers in a manifest, indexed by the resource policy name.
// +kubebuilder:object:generate=false
type NamedTiers map[string]NodeSetList

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
func (as AutoscalingSpecs) GetNamedTiers(es Elasticsearch) (NamedTiers, error) {
	namedTiersSet := make(NamedTiers)
	for _, nodeSet := range es.Spec.NodeSets {
		resourcePolicy, err := as.getAutoscalingSpecFor(es, nodeSet)
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

// getAutoscalingSpecFor retrieves the autoscaling spec associated to a NodeSet or nil if none.
func (as AutoscalingSpecs) getAutoscalingSpecFor(es Elasticsearch, nodeSet NodeSet) (*AutoscalingSpec, error) {
	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return nil, err
	}
	cfg := ElasticsearchSettings{}
	if err := UnpackConfig(nodeSet.Config, *v, &cfg); err != nil {
		return nil, err
	}
	return as.FindByRoles(cfg.Node.Roles), nil
}

func resourcePolicyIndex(index int, child string, moreChildren ...string) *field.Path {
	return field.NewPath("metadata").
		Child("annotations", `"`+ElasticsearchAutoscalingSpecAnnotationName+`"`).
		Index(index).
		Child(child, moreChildren...)
}

// Validate validates a set of autoscaling specifications.
func (as AutoscalingSpecs) Validate() field.ErrorList {
	policyNames := set.Make()
	rolesSet := make([][]string, 0, len(as))
	var errs field.ErrorList

	// TODO: We need the namedTiers to check if the min count allowed is at least >= than the number of nodeSets managed in a policy
	// Unfortunately it requires to parse the node configuration to grab the roles which may raise an error.

	for i, resourcePolicy := range as {
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

// ContainsStringSlide returns true if a slice is included in a slice of slice.
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

// validateQuantities checks that 2 resources boundaries are valid.
func validateQuantities(errs field.ErrorList, min, max *resource.Quantity, index int, resource string) field.ErrorList {
	var quantityErrs field.ErrorList
	if min == nil {
		quantityErrs = append(quantityErrs, field.Required(resourcePolicyIndex(index, "minAllowed", resource), "resource field is mandatory"))
	}
	if max == nil {
		quantityErrs = append(quantityErrs, field.Required(resourcePolicyIndex(index, "maxAllowed", resource), "resource field is mandatory"))
	}
	if len(quantityErrs) > 0 {
		return append(errs, quantityErrs...)
	}
	if !(max.Cmp(*min) >= 0) {
		errs = append(quantityErrs, field.Invalid(resourcePolicyIndex(index, "maxAllowed", resource), (*max).String(), "maxAllowed must be greater or equal than minAllowed"))
	}
	return append(errs, quantityErrs...)
}
