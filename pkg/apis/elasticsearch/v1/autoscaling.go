// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/pkg/utils/set"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const ElasticsearchAutoscalingSpecAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-spec"

var (
	minMemory  = resource.MustParse("2G")
	minCpu     = resource.MustParse("0")
	minStorage = resource.MustParse("0")
)

// +kubebuilder:object:generate=false
type AutoscalingPolicySpecs []AutoscalingPolicySpec

// GetAutoscalingSpecifications unmarshal autoscaling specifications from an Elasticsearch resource.
func (es Elasticsearch) GetAutoscalingSpecifications() (AutoscalingSpec, error) {
	autoscalingSpec := AutoscalingSpec{}
	if len(es.AutoscalingSpec()) == 0 {
		return autoscalingSpec, nil
	}
	err := json.Unmarshal([]byte(es.AutoscalingSpec()), &autoscalingSpec)
	return autoscalingSpec, err
}

// AutoscalingSpec represents how resources can be scaled by the autoscaler controller.
// +kubebuilder:object:generate=false
type AutoscalingSpec struct {
	AutoscalingPolicySpecs AutoscalingPolicySpecs `json:"policies"`
}

// +kubebuilder:object:generate=false
type AutoscalingPolicySpec struct {
	NamedAutoscalingPolicy

	AutoscalingResources `json:"resources"`
}

// +kubebuilder:object:generate=false
type AutoscalingResources struct {
	Cpu       *QuantityRange `json:"cpu,omitempty"`
	Memory    *QuantityRange `json:"memory,omitempty"`
	Storage   *QuantityRange `json:"storage,omitempty"`
	NodeCount CountRange     `json:"nodeCount"`
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
type QuantityRange struct {
	// Min represents the lower limit for the resources managed by the autoscaler.
	Min resource.Quantity `json:"min"`
	// Max represents the upper limit for the resources managed by the autoscaler.
	Max resource.Quantity `json:"max"`
	// RequestsToLimitsRatio allows to customize Kubernetes resource limit based on the requirement.
	RequestsToLimitsRatio *float64 `json:"requestsToLimitsRatio"`
}

// +kubebuilder:object:generate=false
type CountRange struct {
	// Min represents the minimum number of nodes in a tier.
	Min int32 `json:"min"`
	// Max represents the minimum number of nodes in a tier.
	Max int32 `json:"max"`
}

func (aps AutoscalingPolicySpec) IsMemoryDefined() bool {
	return aps.Memory != nil
}

func (aps AutoscalingPolicySpec) IsCpuDefined() bool {
	return aps.Cpu != nil
}

func (aps AutoscalingPolicySpec) IsStorageDefined() bool {
	return aps.Storage != nil
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

// ResourcesSpecificationInt64 is mostly use in logs to print comparable values.
// +kubebuilder:object:generate=false
type ResourcesSpecificationInt64 struct {
	// Count is a number of replicas which should be used as a limit (either lower or upper) in an autoscaling policy.
	Count int32 `json:"count"`
	// Cpu represents the CPU value
	Cpu *int64 `json:"cpu"`
	// memory represents the memory value
	Memory *int64 `json:"memory"`
	// storage represents the storage capacity value
	Storage *int64 `json:"storage"`
}

func (rs ResourcesSpecification) ToInt64() ResourcesSpecificationInt64 {
	rs64 := ResourcesSpecificationInt64{
		Count: rs.Count,
	}
	if rs.Cpu != nil {
		cpu := rs.Cpu.MilliValue()
		rs64.Cpu = &cpu
	}
	if rs.Memory != nil {
		memory := rs.Memory.Value()
		rs64.Memory = &memory
	}
	if rs.Storage != nil {
		storage := rs.Storage.Value()
		rs64.Storage = &storage
	}
	return rs64
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
func (as AutoscalingSpec) FindByRoles(roles []string) *AutoscalingPolicySpec {
	for _, rp := range as.AutoscalingPolicySpecs {
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
func (as AutoscalingSpec) ByNames() map[string]AutoscalingPolicySpec {
	rpByName := make(map[string]AutoscalingPolicySpec)
	for _, scalePolicy := range as.AutoscalingPolicySpecs {
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
func (as AutoscalingSpec) GetNamedTiers(es Elasticsearch) (NamedTiers, error) {
	namedTiersSet := make(NamedTiers)
	for _, nodeSet := range es.Spec.NodeSets {
		resourcePolicy, err := as.GetAutoscalingSpecFor(es, nodeSet)
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

// GetAutoscalingSpecFor retrieves the autoscaling spec associated to a NodeSet or nil if none.
func (as AutoscalingSpec) GetAutoscalingSpecFor(es Elasticsearch, nodeSet NodeSet) (*AutoscalingPolicySpec, error) {
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
func (as AutoscalingSpec) Validate() field.ErrorList {
	policyNames := set.Make()
	rolesSet := make([][]string, 0, len(as.AutoscalingPolicySpecs))
	var errs field.ErrorList

	// TODO: We need the namedTiers to check if the min count allowed is at least >= than the number of nodeSets managed in a policy
	// Unfortunately it requires to parse the node configuration to grab the roles, which may raise an error.

	for i, autoscalingSpec := range as.AutoscalingPolicySpecs {
		// Validate the name field.
		if len(autoscalingSpec.Name) == 0 {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "name"), "name is mandatory"))
		} else {
			if policyNames.Has(autoscalingSpec.Name) {
				errs = append(errs, field.Invalid(resourcePolicyIndex(i, "name"), autoscalingSpec.Name, "policy is duplicated"))
			}
			policyNames.Add(autoscalingSpec.Name)
		}

		// Validate the roles.
		if autoscalingSpec.Roles == nil {
			errs = append(errs, field.Required(resourcePolicyIndex(i, "roles"), "roles is mandatory"))
		} else {
			sort.Strings(autoscalingSpec.Roles)
			if ContainsStringSlide(rolesSet, autoscalingSpec.Roles) {
				errs = append(errs, field.Invalid(resourcePolicyIndex(i, "name"), strings.Join(autoscalingSpec.Roles, ","), "roles set is duplicated"))
			} else {
				rolesSet = append(rolesSet, autoscalingSpec.Roles)
			}
		}

		if !(autoscalingSpec.NodeCount.Min > 0) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "minAllowed", "count"), autoscalingSpec.NodeCount.Min, "count must be a greater than 1"))
		}

		if !(autoscalingSpec.NodeCount.Max > autoscalingSpec.NodeCount.Min) {
			errs = append(errs, field.Invalid(resourcePolicyIndex(i, "maxAllowed", "count"), autoscalingSpec.NodeCount.Max, "max node count must be an integer greater than min node count"))
		}

		// Check CPU
		errs = validateQuantities(errs, autoscalingSpec.Cpu, i, "cpu", minCpu)

		// Check Memory
		errs = validateQuantities(errs, autoscalingSpec.Memory, i, "memory", minMemory)

		// Check storage
		errs = validateQuantities(errs, autoscalingSpec.Storage, i, "storage", minStorage)
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
func validateQuantities(errs field.ErrorList, quantityRange *QuantityRange, index int, resource string, minQuantity resource.Quantity) field.ErrorList {
	var quantityErrs field.ErrorList
	if quantityRange == nil {
		return errs
	}

	if !minQuantity.IsZero() && !(quantityRange.Min.Cmp(minQuantity) >= 0) {
		quantityErrs = append(quantityErrs, field.Required(resourcePolicyIndex(index, "minAllowed", resource), fmt.Sprintf("min quantity must be greater than %s", minQuantity.String())))
	}

	if minQuantity.IsZero() && !(quantityRange.Min.Value() > 0) {
		quantityErrs = append(quantityErrs, field.Required(resourcePolicyIndex(index, "minAllowed", resource), "min quantity must be greater than 0"))
	}

	if quantityRange.Min.Cmp(quantityRange.Max) > 0 {
		quantityErrs = append(quantityErrs, field.Invalid(resourcePolicyIndex(index, "maxAllowed", resource), quantityRange.Max.String(), "max quantity must be greater or equal than min quantity"))
	}
	return append(errs, quantityErrs...)
}
