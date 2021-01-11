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

	"github.com/elastic/cloud-on-k8s/pkg/utils/stringsutil"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/pkg/utils/set"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const ElasticsearchAutoscalingSpecAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-spec"

var (
	ElasticsearchMinAutoscalingVersion = version.From(7, 11, 0)

	// minMemory is the minimum amount of memory which can be set in the memory limits specification.
	minMemory = resource.MustParse("2G")

	// No minimum values are expected for CPU and Storage.
	// If provided the validation function must ensure that the value is strictly greater than 0.
	minCPU     = resource.MustParse("0")
	minStorage = resource.MustParse("0")
)

// -- Elasticsearch Autoscaling API structures

// DeciderSettings allows the user to tweak autoscaling deciders.
// The map data structure complies with the <key,value> format expected by Elasticsearch.
// +kubebuilder:object:generate=false
type DeciderSettings map[string]string

// AutoscalingPolicy models the Elasticsearch autoscaling API.
// +kubebuilder:object:generate=false
type AutoscalingPolicy struct {
	// A resource policy must target a unique set of roles.
	Roles []string `json:"roles,omitempty"`
	// Deciders allows the user to override default settings for autoscaling deciders.
	Deciders map[string]DeciderSettings `json:"deciders,omitempty"`
}

// -- Elastic Cloud on K8S specific structures

// AutoscalingSpec is the root object of the autoscaling specification in the Elasticsearch resource definition.
// +kubebuilder:object:generate=false
type AutoscalingSpec struct {
	AutoscalingPolicySpecs AutoscalingPolicySpecs `json:"policies"`
	// Elasticsearch is stored in the autoscaling spec for convenience. It should be removed once the autoscaling spec is
	// fully part of the Elasticsearch specification.
	Elasticsearch Elasticsearch `json:"-"`
}

// +kubebuilder:object:generate=false
type AutoscalingPolicySpecs []AutoscalingPolicySpec

// NamedAutoscalingPolicy models an autoscaling policy as expected by the Elasticsearch policy API identified by
// a unique name provided by the user.
// +kubebuilder:object:generate=false
type NamedAutoscalingPolicy struct {
	// Name identifies the autoscaling policy in the autoscaling specification.
	Name string `json:"name,omitempty"`
	// AutoscalingPolicy is the autoscaling policy as expected by the Elasticsearch API.
	AutoscalingPolicy
}

// AutoscalingPolicySpec holds a named autoscaling policy and the associated resources limits (cpu, memory, storage).
// +kubebuilder:object:generate=false
type AutoscalingPolicySpec struct {
	NamedAutoscalingPolicy

	AutoscalingResources `json:"resources"`
}

// +kubebuilder:object:generate=false
// AutoscalingResources models the limits, submitted by the user, for the supported resources.
// Only the node count range is mandatory. For other resources, a limit range is required only
// if the Elasticsearch autoscaling capacity API returns a requirement for a given resource.
// For example, the memory limit range is only required if the autoscaling API answer returns a memory requirement.
// If there is no limit range then the resource specification in the nodeSet specification is left untouched.
type AutoscalingResources struct {
	CPU       *QuantityRange `json:"cpu,omitempty"`
	Memory    *QuantityRange `json:"memory,omitempty"`
	Storage   *QuantityRange `json:"storage,omitempty"`
	NodeCount CountRange     `json:"nodeCount"`
}

// QuantityRange models a resource limit range for resources which can be expressed with resource.Quantity.
// +kubebuilder:object:generate=false
type QuantityRange struct {
	// Min represents the lower limit for the resources managed by the autoscaler.
	Min resource.Quantity `json:"min"`
	// Max represents the upper limit for the resources managed by the autoscaler.
	Max resource.Quantity `json:"max"`
	// RequestsToLimitsRatio allows to customize Kubernetes resource Limit based on the Request.
	RequestsToLimitsRatio *float64 `json:"requestsToLimitsRatio"`
}

// CountRange is used to model the limit range for node deployed in
// +kubebuilder:object:generate=false
type CountRange struct {
	// Min represents the minimum number of nodes in a tier.
	Min int32 `json:"min"`
	// Max represents the minimum number of nodes in a tier.
	Max int32 `json:"max"`
}

// GetAutoscalingSpecification unmarshal autoscaling specifications from an Elasticsearch resource.
func (es Elasticsearch) GetAutoscalingSpecification() (AutoscalingSpec, error) {
	autoscalingSpec := AutoscalingSpec{}
	if len(es.AutoscalingSpec()) == 0 {
		return autoscalingSpec, nil
	}
	err := json.Unmarshal([]byte(es.AutoscalingSpec()), &autoscalingSpec)
	autoscalingSpec.Elasticsearch = es
	return autoscalingSpec, err
}

// IsMemoryDefined returns true if the user specified memory limits.
func (aps AutoscalingPolicySpec) IsMemoryDefined() bool {
	return aps.Memory != nil
}

// IsCPUDefined returns true if the user specified cpu limits.
func (aps AutoscalingPolicySpec) IsCPUDefined() bool {
	return aps.CPU != nil
}

// IsStorageDefined returns true if the user specified storage limits.
func (aps AutoscalingPolicySpec) IsStorageDefined() bool {
	return aps.Storage != nil
}

// findByRoles returns the autoscaling specification associated with a set of roles or nil if not found.
func (as AutoscalingSpec) findByRoles(roles []string) *AutoscalingPolicySpec {
	for _, rp := range as.AutoscalingPolicySpecs {
		if !rolesMatch(rp.Roles, roles) {
			continue
		}
		return &rp
	}
	return nil
}

// rolesMatch compares two set of roles and returns true if both sets contain the exact same roles.
func rolesMatch(roles1, roles2 []string) bool {
	if len(roles1) != len(roles2) {
		return false
	}
	rolesInPolicy := set.Make(roles1...)
	for _, role := range roles2 {
		if !rolesInPolicy.Has(role) {
			return false
		}
	}
	return true
}

// AutoscaledNodeSets holds the nodeSets managed by an autoscaling policy, indexed by the autoscaling policy name.
// +kubebuilder:object:generate=false
type AutoscaledNodeSets map[string]NodeSetList

// AutoscalingPolicies returns the list of autoscaling policies from the named tiers.
func (n AutoscaledNodeSets) AutoscalingPolicies() set.StringSet {
	autoscalingPolicies := set.Make()
	for autoscalingPolicy := range n {
		autoscalingPolicies.Add(autoscalingPolicy)
	}
	return autoscalingPolicies
}

// NodeSets returns the list of nodeSets managed by an autoscaling policy.
func (n AutoscaledNodeSets) NodeSets() set.StringSet {
	autoscalingPolicies := set.Make()
	for _, nodeSets := range n {
		for _, nodeSet := range nodeSets {
			autoscalingPolicies.Add(nodeSet.Name)
		}
	}
	return autoscalingPolicies
}

func (n AutoscaledNodeSets) String() string {
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

// +kubebuilder:object:generate=false
type NodeSetConfigError struct {
	error
	NodeSet
	Index int
}

// GetAutoscaledNodeSets retrieves the name of all the tiers in the Elasticsearch manifest.
func (as AutoscalingSpec) GetAutoscaledNodeSets() (AutoscaledNodeSets, *NodeSetConfigError) {
	namedTiersSet := make(AutoscaledNodeSets)
	for i, nodeSet := range as.Elasticsearch.Spec.NodeSets {
		resourcePolicy, err := as.GetAutoscalingSpecFor(nodeSet)
		if err != nil {
			return nil, &NodeSetConfigError{
				error:   err,
				NodeSet: nodeSet,
				Index:   i,
			}
		}
		if resourcePolicy == nil {
			// This nodeSet is not managed by an autoscaling policy
			continue
		}
		namedTiersSet[resourcePolicy.Name] = append(namedTiersSet[resourcePolicy.Name], *nodeSet.DeepCopy())
	}
	return namedTiersSet, nil
}

// GetMLNodesCount computes the total number of nodes which can be deployed in the cluster.
func (as AutoscalingSpec) GetMLNodesCount() (int32, error) {
	var totalMLNodesCount int32
	for _, nodeSet := range as.Elasticsearch.Spec.NodeSets {
		resourcePolicy, err := as.GetAutoscalingSpecFor(nodeSet)
		if err != nil {
			return 0, err
		}
		roles, err := getNodeSetRoles(as.Elasticsearch, nodeSet)
		if err != nil {
			return 0, err
		}
		rolesInPolicy := set.Make(roles...)
		if rolesInPolicy.Has(MLRole) {
			totalMLNodesCount += resourcePolicy.NodeCount.Max
		}
	}
	return totalMLNodesCount, nil
}

// GetAutoscalingSpecFor retrieves the autoscaling spec associated to a NodeSet or nil if none.
func (as AutoscalingSpec) GetAutoscalingSpecFor(nodeSet NodeSet) (*AutoscalingPolicySpec, error) {
	roles, err := getNodeSetRoles(as.Elasticsearch, nodeSet)
	if err != nil {
		return nil, err
	}
	return as.findByRoles(roles), nil
}

// getNodeSetRoles attempts to parse the roles specified in the configuration of a given nodeSet.
func getNodeSetRoles(es Elasticsearch, nodeSet NodeSet) ([]string, error) {
	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return nil, err
	}
	cfg := ElasticsearchSettings{}
	if err := UnpackConfig(nodeSet.Config, *v, &cfg); err != nil {
		return nil, err
	}
	if cfg.Node == nil {
		return nil, fmt.Errorf("node.roles must be set")
	}
	return cfg.Node.Roles, nil
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

		if stringsutil.StringInSlice(MLRole, autoscalingSpec.Roles) && len(autoscalingSpec.Roles) > 1 {
			errs = append(errs, field.Invalid(autoscalingSpecPath(i, "name"), strings.Join(autoscalingSpec.Roles, ","), "ML nodes must be in a dedicated autoscaling policy"))
		}

		if !(autoscalingSpec.NodeCount.Min >= 0) {
			errs = append(errs, field.Invalid(autoscalingSpecPath(i, "minAllowed", "count"), autoscalingSpec.NodeCount.Min, "count must be equal or greater than 0"))
		}

		if !(autoscalingSpec.NodeCount.Max > autoscalingSpec.NodeCount.Min) {
			errs = append(errs, field.Invalid(autoscalingSpecPath(i, "maxAllowed", "count"), autoscalingSpec.NodeCount.Max, "max node count must be an integer greater than min node count"))
		}

		// Validate CPU
		errs = validateQuantities(errs, autoscalingSpec.CPU, i, "cpu", minCPU)

		// Validate Memory
		errs = validateQuantities(errs, autoscalingSpec.Memory, i, "memory", minMemory)

		// Validate storage
		errs = validateQuantities(errs, autoscalingSpec.Storage, i, "storage", minStorage)
	}

	autoscaledNodeSets, err := as.GetAutoscaledNodeSets()
	if err != nil {
		errs = append(errs, field.Invalid(field.NewPath("spec").Child("nodeSets").Index(err.Index).Child("config"), err.NodeSet.Config, fmt.Sprintf("cannot parse nodeSet configuration: %s", err.Error())))
		// We stop the validation here as the named tiers are required to go further
		return errs
	}

	// We want to ensure that an autoscaling policy is at least managing one nodeSet
	policiesWithNodeSets := autoscaledNodeSets.AutoscalingPolicies()
	for i, policy := range as.AutoscalingPolicySpecs {
		if !policiesWithNodeSets.Has(policy.Name) {
			// No nodeSet matches this autoscaling policy
			errs = append(errs, field.Invalid(autoscalingSpecPath(i, "roles"), policy.Roles, "roles must be set in at least one nodeSet"))
		}
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

// containsString returns true if a string is included in a slice.
func containsString(roles []string, role string) bool {
	for _, s := range roles {
		if reflect.DeepEqual(s, role) {
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
