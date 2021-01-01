// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/pkg/utils/set"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const ElasticsearchAutoscalingSpecAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-spec"

var (
	// minMemory is the minimum amount of memory which can be set in the memory limits specification.
	minMemory = resource.MustParse("2G")

	minCpu     = resource.MustParse("0")
	minStorage = resource.MustParse("0")
)

// AutoscalingSpec is the root object of the autoscaling specification in the Elasticsearch resource definition.
// +kubebuilder:object:generate=false
type AutoscalingSpec struct {
	AutoscalingPolicySpecs AutoscalingPolicySpecs `json:"policies"`
}

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

// AutoscalingPolicySpec holds a named autoscaling policy and the associated resources limits (cpu, memory, storage).
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

// NamedAutoscalingPolicy models an autoscaling policy as expected by the Elasticsearch policy API identified by
// a unique name provided by the user.
// +kubebuilder:object:generate=false
type NamedAutoscalingPolicy struct {
	// A resource policy must be identified by a unique name, provided by the user.
	Name string `json:"name,omitempty"`

	AutoscalingPolicy
}

// DeciderSettings allows the user to tweak autoscaling deciders.
// +kubebuilder:object:generate=false
type DeciderSettings map[string]string

// AutoscalingPolicy models the Elasticsearch autoscaling API.
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

// findByRoles returns the autoscaling specification associated with a set of roles or nil if not found.
func (as AutoscalingSpec) findByRoles(roles []string) *AutoscalingPolicySpec {
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

// NamedTiers holds the tiers in a manifest, indexed by the autoscaling policy name.
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
	return as.findByRoles(cfg.Node.Roles), nil
}

// autoscalingSpecPath helps to compute the path used in validation error fields.
func autoscalingSpecPath(index int, child string, moreChildren ...string) *field.Path {
	return field.NewPath("metadata").
		Child("annotations", `"`+ElasticsearchAutoscalingSpecAnnotationName+`"`).
		Index(index).
		Child(child, moreChildren...)
}

// Validate validates the autoscaling specification submitted by the user.
func (as AutoscalingSpec) Validate() field.ErrorList {
	policyNames := set.Make()
	rolesSet := make([][]string, 0, len(as.AutoscalingPolicySpecs))
	var errs field.ErrorList

	// TODO: We need the namedTiers to check if the min count allowed is at least >= than the number of nodeSets managed in a policy
	// Unfortunately it requires to parse the node configuration to grab the roles, which may raise an error.

	for i, autoscalingSpec := range as.AutoscalingPolicySpecs {
		// Validate the name field.
		if len(autoscalingSpec.Name) == 0 {
			errs = append(errs, field.Required(autoscalingSpecPath(i, "name"), "name is mandatory"))
		} else {
			if policyNames.Has(autoscalingSpec.Name) {
				errs = append(errs, field.Invalid(autoscalingSpecPath(i, "name"), autoscalingSpec.Name, "policy is duplicated"))
			}
			policyNames.Add(autoscalingSpec.Name)
		}

		// Validate the roles.
		if autoscalingSpec.Roles == nil {
			errs = append(errs, field.Required(autoscalingSpecPath(i, "roles"), "roles is mandatory"))
		} else {
			sort.Strings(autoscalingSpec.Roles)
			if containsStringSlice(rolesSet, autoscalingSpec.Roles) {
				errs = append(errs, field.Invalid(autoscalingSpecPath(i, "name"), strings.Join(autoscalingSpec.Roles, ","), "roles set is duplicated"))
			} else {
				rolesSet = append(rolesSet, autoscalingSpec.Roles)
			}
		}

		if !(autoscalingSpec.NodeCount.Min > 0) {
			errs = append(errs, field.Invalid(autoscalingSpecPath(i, "minAllowed", "count"), autoscalingSpec.NodeCount.Min, "count must be a greater than 1"))
		}

		if !(autoscalingSpec.NodeCount.Max > autoscalingSpec.NodeCount.Min) {
			errs = append(errs, field.Invalid(autoscalingSpecPath(i, "maxAllowed", "count"), autoscalingSpec.NodeCount.Max, "max node count must be an integer greater than min node count"))
		}

		// Validate CPU
		errs = validateQuantities(errs, autoscalingSpec.Cpu, i, "cpu", minCpu)

		// Validate Memory
		errs = validateQuantities(errs, autoscalingSpec.Memory, i, "memory", minMemory)

		// Validate storage
		errs = validateQuantities(errs, autoscalingSpec.Storage, i, "storage", minStorage)
	}
	return errs
}

// containsStringSlice returns true if an ordered slice is included in a slice of slice.
func containsStringSlice(slices [][]string, slice []string) bool {
	for _, s := range slices {
		if reflect.DeepEqual(s, slice) {
			return true
		}
	}
	return false
}

// validateQuantities ensures that a quantity range is valid.
func validateQuantities(errs field.ErrorList, quantityRange *QuantityRange, index int, resource string, minQuantity resource.Quantity) field.ErrorList {
	var quantityErrs field.ErrorList
	if quantityRange == nil {
		return errs
	}

	if !minQuantity.IsZero() && !(quantityRange.Min.Cmp(minQuantity) >= 0) {
		quantityErrs = append(quantityErrs, field.Required(autoscalingSpecPath(index, "minAllowed", resource), fmt.Sprintf("min quantity must be greater than %s", minQuantity.String())))
	}

	if minQuantity.IsZero() && !(quantityRange.Min.Value() > 0) {
		quantityErrs = append(quantityErrs, field.Required(autoscalingSpecPath(index, "minAllowed", resource), "min quantity must be greater than 0"))
	}

	if quantityRange.Min.Cmp(quantityRange.Max) > 0 {
		quantityErrs = append(quantityErrs, field.Invalid(autoscalingSpecPath(index, "maxAllowed", resource), quantityRange.Max.String(), "max quantity must be greater or equal than min quantity"))
	}
	return append(errs, quantityErrs...)
}
