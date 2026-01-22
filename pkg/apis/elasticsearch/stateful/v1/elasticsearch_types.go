// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1

import (
	"strings"

	"github.com/blang/semver/v4"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/set"
)

const (
	// ElasticsearchContainerName is the name of the Elasticsearch container in pods.
	ElasticsearchContainerName = escommon.ElasticsearchContainerName
	// DisableUpgradePredicatesAnnotation is the annotation that can be applied to an
	// Elasticsearch cluster to disable certain predicates during rolling upgrades.  Multiple
	// predicates names can be separated by ",".
	//
	// Example:
	//
	//   To disable "if_yellow_only_restart_upgrading_nodes_with_unassigned_replicas" predicate
	//
	//   metadata:
	//     annotations:
	//       eck.k8s.elastic.co/disable-upgrade-predicates="if_yellow_only_restart_upgrading_nodes_with_unassigned_replicas"
	DisableUpgradePredicatesAnnotation = "eck.k8s.elastic.co/disable-upgrade-predicates"
	// DownwardNodeLabelsAnnotation holds an optional list of expected node labels to be set as annotations on the Elasticsearch Pods.
	DownwardNodeLabelsAnnotation = "eck.k8s.elastic.co/downward-node-labels"
	// SuspendAnnotation allows users to annotate the Elasticsearch resource with the names of Pods they want to suspend
	// for debugging purposes.
	SuspendAnnotation = "eck.k8s.elastic.co/suspend"
	// ElasticsearchAutoscalingSpecAnnotationName is the name of the annotation used to store the autoscaling specification.
	//
	// Deprecated: the autoscaling annotation has been deprecated in favor of the ElasticsearchAutoscaler custom resource.
	ElasticsearchAutoscalingSpecAnnotationName = "elasticsearch.alpha.elastic.co/autoscaling-spec"

	// TransportCertDisabledAnnotationName is the annotation that indicates that ECK-managed transport certs have been disabled for the Pod.
	TransportCertDisabledAnnotationName = "elasticsearch.k8s.elastic.co/self-signed-transport-cert-disabled"

	// Kind is inferred from the struct name using reflection in SchemeBuilder.Register()
	// we duplicate it as a constant here for practical purposes.
	Kind = "Elasticsearch"
)

// ServiceAccountMinVersion is the first version of Elasticsearch for which ECK supports service accounts.
// It is however up to each association controller to ensure that a specific service account is available
// in the current Elasticsearch version.
var ServiceAccountMinVersion = semver.MustParse("7.17.0")

func AreServiceAccountsSupported(version string) (bool, error) {
	esVersion, err := semver.Parse(version)
	if err != nil {
		return false, err
	}
	return esVersion.GTE(ServiceAccountMinVersion), nil
}

// +kubebuilder:object:root=true

// ElasticsearchList contains a list of Elasticsearch clusters
type ElasticsearchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Elasticsearch `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Elasticsearch{}, &ElasticsearchList{})
}

// ElasticsearchSpec holds the specification of an Elasticsearch cluster.
type ElasticsearchSpec struct {
	// Version of Elasticsearch.
	Version string `json:"version"`

	// Image is the Elasticsearch Docker image to deploy.
	Image string `json:"image,omitempty"`

	// RemoteClusterServer specifies if the remote cluster server should be enabled.
	// This must be enabled if this cluster is a remote cluster which is expected to be accessed using API key authentication.
	// +kubebuilder:validation:Optional
	RemoteClusterServer RemoteClusterServer `json:"remoteClusterServer,omitempty"`

	// HTTP holds HTTP layer settings for Elasticsearch.
	// +kubebuilder:validation:Optional
	HTTP commonv1.HTTPConfig `json:"http,omitempty"`

	// Transport holds transport layer settings for Elasticsearch.
	// +kubebuilder:validation:Optional
	Transport TransportConfig `json:"transport,omitempty"`

	// NodeSets allow specifying groups of Elasticsearch nodes sharing the same configuration and Pod templates.
	// +kubebuilder:validation:MinItems=1
	NodeSets []NodeSet `json:"nodeSets"`

	// UpdateStrategy specifies how updates to the cluster should be performed.
	// +kubebuilder:validation:Optional
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty"`

	// PodDisruptionBudget provides access to the default Pod disruption budget(s) for the Elasticsearch cluster.
	// The behavior depends on the license level.
	// With a Basic license or if podDisruptionBudget.spec is not empty:
	//   The default budget doesn't allow any Pod to be removed in case the cluster is not green or if there is only one node of type `data` or `master`.
	//   In all other cases the default podDisruptionBudget sets `minAvailable` equal to the total number of nodes minus 1.
	// With an Enterprise license and if podDisruptionBudget.spec is empty:
	//   The default budget is split into multiple budgets, each targeting a specific node role type allowing additional disruptions
	//   for certain roles according to the health status of the cluster.
	//     Example:
	//       All data roles (excluding frozen): allows disruptions only when the cluster is green.
	//       All other roles: allows disruptions only when the cluster is yellow or green.
	// To disable, set `podDisruptionBudget` to the empty value (`{}` in YAML).
	// +kubebuilder:validation:Optional
	PodDisruptionBudget *commonv1.PodDisruptionBudgetTemplate `json:"podDisruptionBudget,omitempty"`

	// Auth contains user authentication and authorization security settings for Elasticsearch.
	// +kubebuilder:validation:Optional
	Auth Auth `json:"auth,omitempty"`

	// SecureSettings is a list of references to Kubernetes secrets containing sensitive configuration options for Elasticsearch.
	// +kubebuilder:validation:Optional
	SecureSettings []commonv1.SecretSource `json:"secureSettings,omitempty"`

	// ServiceAccountName is used to check access from the current resource to a resource (for ex. a remote Elasticsearch cluster) in a different namespace.
	// Can only be used if ECK is enforcing RBAC on references.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// RemoteClusters enables you to establish uni-directional connections to a remote Elasticsearch cluster.
	// +optional
	RemoteClusters []RemoteCluster `json:"remoteClusters,omitempty"`

	// VolumeClaimDeletePolicy sets the policy for handling deletion of PersistentVolumeClaims for all NodeSets.
	// Possible values are DeleteOnScaledownOnly and DeleteOnScaledownAndClusterDeletion. Defaults to DeleteOnScaledownAndClusterDeletion.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=DeleteOnScaledownOnly;DeleteOnScaledownAndClusterDeletion
	VolumeClaimDeletePolicy VolumeClaimDeletePolicy `json:"volumeClaimDeletePolicy,omitempty"`

	// Monitoring enables you to collect and ship log and monitoring data of this Elasticsearch cluster.
	// See https://www.elastic.co/guide/en/elasticsearch/reference/current/monitor-elasticsearch-cluster.html.
	// Metricbeat and Filebeat are deployed in the same Pod as sidecars and each one sends data to one or two different
	// Elasticsearch monitoring clusters running in the same Kubernetes cluster.
	// +kubebuilder:validation:Optional
	Monitoring commonv1.Monitoring `json:"monitoring,omitempty"`

	// RevisionHistoryLimit is the number of revisions to retain to allow rollback in the underlying StatefulSets.
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`
}

// RemoteClusterServer is an alias to the common type for interface compatibility.
type RemoteClusterServer = escommon.RemoteClusterServer

// VolumeClaimDeletePolicy describes the delete policy for handling PersistentVolumeClaims that hold Elasticsearch data.
// Inspired by https://github.com/kubernetes/enhancements/pull/2440
type VolumeClaimDeletePolicy string

const (
	// DeleteOnScaledownAndClusterDeletionPolicy remove PersistentVolumeClaims when the corresponding Elasticsearch node is removed.
	DeleteOnScaledownAndClusterDeletionPolicy VolumeClaimDeletePolicy = "DeleteOnScaledownAndClusterDeletion"
	// DeleteOnScaledownOnlyPolicy removes PersistentVolumeClaims on scale down of Elasticsearch nodes but retains all
	// current PersistenVolumeClaims when the Elasticsearch cluster has been deleted.
	DeleteOnScaledownOnlyPolicy VolumeClaimDeletePolicy = "DeleteOnScaledownOnly"
)

// TransportConfig is an alias to the common type for interface compatibility.
type TransportConfig = escommon.TransportConfig

// TransportTLSOptions is an alias to the common type for interface compatibility.
type TransportTLSOptions = escommon.TransportTLSOptions

// SelfSignedTransportCertificates is an alias to the common type for interface compatibility.
type SelfSignedTransportCertificates = escommon.SelfSignedTransportCertificates

// RemoteCluster is an alias to the common type for interface compatibility.
type RemoteCluster = escommon.RemoteCluster

// NodeCount returns the total number of nodes of the Elasticsearch cluster
func (es ElasticsearchSpec) NodeCount() int32 {
	count := int32(0)
	for _, topoElem := range es.NodeSets {
		count += topoElem.Count
	}
	return count
}

func (es ElasticsearchSpec) VolumeClaimDeletePolicyOrDefault() VolumeClaimDeletePolicy {
	if es.VolumeClaimDeletePolicy == "" {
		return DeleteOnScaledownAndClusterDeletionPolicy
	}
	return es.VolumeClaimDeletePolicy
}

// Auth is an alias to the common type for interface compatibility.
type Auth = escommon.Auth

// RoleSource is an alias to the common type for interface compatibility.
type RoleSource = escommon.RoleSource

// FileRealmSource is an alias to the common type for interface compatibility.
type FileRealmSource = escommon.FileRealmSource

// NodeSet is the specification for a group of Elasticsearch nodes sharing the same configuration and a Pod template.
type NodeSet struct {
	// Name of this set of nodes. Becomes a part of the Elasticsearch node.name setting.
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9-]+
	// +kubebuilder:validation:MaxLength=23
	Name string `json:"name"`

	// Config holds the Elasticsearch configuration.
	// +kubebuilder:pruning:PreserveUnknownFields
	Config *commonv1.Config `json:"config,omitempty"`

	// Count of Elasticsearch nodes to deploy.
	// If the node set is managed by an autoscaling policy the initial value is automatically set by the autoscaling controller.
	// +kubebuilder:validation:Optional
	Count int32 `json:"count"`

	// PodTemplate provides customisation options (labels, annotations, affinity rules, resource requests, and so on) for the Pods belonging to this NodeSet.
	// +kubebuilder:validation:Optional
	// +kubebuilder:pruning:PreserveUnknownFields
	PodTemplate corev1.PodTemplateSpec `json:"podTemplate,omitempty"`

	// VolumeClaimTemplates is a list of persistent volume claims to be used by each Pod in this NodeSet.
	// Every claim in this list must have a matching volumeMount in one of the containers defined in the PodTemplate.
	// Items defined here take precedence over any default claims added by the operator with the same name.
	// +kubebuilder:validation:Optional
	VolumeClaimTemplates []corev1.PersistentVolumeClaim `json:"volumeClaimTemplates,omitempty"`
}

// +kubebuilder:object:generate=false
type NodeSetList []NodeSet

func (nsl NodeSetList) Names() []string {
	names := make([]string, len(nsl))
	for i := range nsl {
		names[i] = nsl[i].Name
	}
	return names
}

// GetESContainerTemplate returns the Elasticsearch container (if set) from the NodeSet's PodTemplate
func (n NodeSet) GetESContainerTemplate() *corev1.Container {
	for _, c := range n.PodTemplate.Spec.Containers {
		if c.Name == ElasticsearchContainerName {
			return &c
		}
	}
	return nil
}

// UpdateStrategy specifies how updates to the cluster should be performed.
type UpdateStrategy struct {
	// ChangeBudget defines the constraints to consider when applying changes to the Elasticsearch cluster.
	ChangeBudget ChangeBudget `json:"changeBudget,omitempty"`
}

// ChangeBudget defines the constraints to consider when applying changes to the Elasticsearch cluster.
type ChangeBudget struct {
	// MaxUnavailable is the maximum number of Pods that can be unavailable (not ready) during the update due to
	// circumstances under the control of the operator. Setting a negative value will disable this restriction.
	// Defaults to 1 if not specified.
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`

	// MaxSurge is the maximum number of new Pods that can be created exceeding the original number of Pods defined in
	// the specification. MaxSurge is only taken into consideration when scaling up. Setting a negative value will
	// disable the restriction. Defaults to unbounded if not specified.
	MaxSurge *int32 `json:"maxSurge,omitempty"`
}

// DefaultChangeBudget is used when no change budget is provided. It might not be the most effective, but should work in
// most cases.
var DefaultChangeBudget = ChangeBudget{
	MaxSurge:       nil,
	MaxUnavailable: ptr.To[int32](1),
}

func (cb ChangeBudget) GetMaxSurgeOrDefault() *int32 {
	// use default if not specified
	maxSurge := DefaultChangeBudget.MaxSurge
	if cb.MaxSurge != nil {
		maxSurge = cb.MaxSurge
	}

	// nil or negative in the spec denotes unlimited surge
	// in the code unlimited surge is denoted by nil
	if maxSurge == nil || *maxSurge < 0 {
		maxSurge = nil
	}

	return maxSurge
}

func (cb ChangeBudget) GetMaxUnavailableOrDefault() *int32 {
	// use default if not specified
	maxUnavailable := DefaultChangeBudget.MaxUnavailable
	if cb.MaxUnavailable != nil {
		maxUnavailable = cb.MaxUnavailable
	}

	// nil or negative in the spec denotes unlimited unavailability
	// in the code unlimited unavailability is denoted by nil
	if maxUnavailable == nil || *maxUnavailable < 0 {
		maxUnavailable = nil
	}

	return maxUnavailable
}

// +kubebuilder:object:root=true

// Elasticsearch represents an Elasticsearch resource in a Kubernetes cluster.
// +kubebuilder:resource:categories=elastic,shortName=es
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="health",type="string",JSONPath=".status.health"
// +kubebuilder:printcolumn:name="nodes",type="integer",JSONPath=".status.availableNodes",description="Available nodes"
// +kubebuilder:printcolumn:name="version",type="string",JSONPath=".status.version",description="Elasticsearch version"
// +kubebuilder:printcolumn:name="phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:storageversion
type Elasticsearch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec       ElasticsearchSpec                                    `json:"spec,omitempty"`
	Status     ElasticsearchStatus                                  `json:"status,omitempty"`
	AssocConfs map[commonv1.ObjectSelector]commonv1.AssociationConf `json:"-"`
}

// DownwardNodeLabels returns the set of expected node labels to be copied as annotations on the Elasticsearch Pods.
func (es Elasticsearch) DownwardNodeLabels() []string {
	expectedAnnotations, exist := es.Annotations[DownwardNodeLabelsAnnotation]
	expectedAnnotations = strings.TrimSpace(expectedAnnotations)
	if !exist || expectedAnnotations == "" {
		return nil
	}
	return strings.Split(expectedAnnotations, ",")
}

// HasDownwardNodeLabels returns true if some node labels are expected on the Elasticsearch Pods.
func (es Elasticsearch) HasDownwardNodeLabels() bool {
	return len(es.DownwardNodeLabels()) > 0
}

// IsMarkedForDeletion returns true if the Elasticsearch is going to be deleted
func (es Elasticsearch) IsMarkedForDeletion() bool {
	return !es.DeletionTimestamp.IsZero()
}

// IsConfiguredToAllowDowngrades returns true if the DisableDowngradeValidation annotation is set to the value of true.
func (es Elasticsearch) IsConfiguredToAllowDowngrades() bool {
	return commonv1.IsConfiguredToAllowDowngrades(&es)
}

func (es *Elasticsearch) ServiceAccountName() string {
	return es.Spec.ServiceAccountName
}

func (es *Elasticsearch) ElasticServiceAccount() (commonv1.ServiceAccountName, error) {
	return "", nil
}

// IsAutoscalingAnnotationSet returns true if there is an autoscaling configuration in the annotations.
//
// Deprecated: the autoscaling annotation has been deprecated in favor of the ElasticsearchAutoscaler custom resource.
func (es Elasticsearch) IsAutoscalingAnnotationSet() bool {
	_, ok := es.Annotations[ElasticsearchAutoscalingSpecAnnotationName]
	return ok
}

func (es Elasticsearch) SecureSettings() []commonv1.SecretSource {
	return es.Spec.SecureSettings
}

func (es Elasticsearch) SuspendedPodNames() set.StringSet {
	return setFromAnnotations(SuspendAnnotation, es.Annotations)
}

// GetObservedGeneration will return the observed generation from the Elasticsearch status.
func (es Elasticsearch) GetObservedGeneration() int64 {
	return es.Status.ObservedGeneration
}

// GetVersion returns the Elasticsearch version.
func (es Elasticsearch) GetVersion() string {
	return es.Spec.Version
}

// GetImage returns the Elasticsearch Docker image.
func (es Elasticsearch) GetImage() string {
	return es.Spec.Image
}

// GetHTTP returns the HTTP layer configuration.
func (es Elasticsearch) GetHTTP() commonv1.HTTPConfig {
	return es.Spec.HTTP
}

// GetTransport returns the transport layer configuration.
func (es Elasticsearch) GetTransport() escommon.TransportConfig {
	return es.Spec.Transport
}

// GetAuth returns the authentication and authorization settings.
func (es Elasticsearch) GetAuth() escommon.Auth {
	return es.Spec.Auth
}

// GetSecureSettings returns the list of secure settings secret sources.
func (es Elasticsearch) GetSecureSettings() []commonv1.SecretSource {
	return es.Spec.SecureSettings
}

// GetServiceAccountName returns the service account name.
func (es Elasticsearch) GetServiceAccountName() string {
	return es.Spec.ServiceAccountName
}

// GetRemoteClusterServer returns the remote cluster server configuration.
func (es Elasticsearch) GetRemoteClusterServer() escommon.RemoteClusterServer {
	return es.Spec.RemoteClusterServer
}

// GetRemoteClusters returns the list of remote cluster configurations.
func (es Elasticsearch) GetRemoteClusters() []escommon.RemoteCluster {
	return es.Spec.RemoteClusters
}

// IsStateless returns false for stateful Elasticsearch clusters.
func (es Elasticsearch) IsStateless() bool {
	return false
}

// Ensure Elasticsearch implements escommon.ElasticsearchCluster interface.
var _ escommon.ElasticsearchCluster = &Elasticsearch{}

func setFromAnnotations(annotationKey string, annotations map[string]string) set.StringSet {
	allValues, exists := annotations[annotationKey]
	if !exists {
		return nil
	}

	splitValues := strings.Split(allValues, ",")
	valueSet := set.Make()
	for _, p := range splitValues {
		valueSet.Add(strings.TrimSpace(p))
	}
	return valueSet
}

// -- associations

var _ commonv1.Associated = &Elasticsearch{}

func (es *Elasticsearch) GetAssociations() []commonv1.Association {
	associations := make([]commonv1.Association, 0)
	for _, ref := range es.Spec.Monitoring.Metrics.ElasticsearchRefs {
		if ref.IsDefined() {
			associations = append(associations, &EsMonitoringAssociation{
				Elasticsearch: es,
				ref:           ref.ObjectSelector.WithDefaultNamespace(es.Namespace),
			})
		}
	}
	for _, ref := range es.Spec.Monitoring.Logs.ElasticsearchRefs {
		if ref.IsDefined() {
			associations = append(associations, &EsMonitoringAssociation{
				Elasticsearch: es,
				ref:           ref.ObjectSelector.WithDefaultNamespace(es.Namespace),
			})
		}
	}
	return associations
}

// -- association with monitoring Elasticsearch clusters

// EsMonitoringAssociation helps to manage Elasticsearch+Metricbeat+Filebeat <-> Elasticsearch(es) associations
type EsMonitoringAssociation struct {
	// The monitored Elasticsearch cluster from where are collected logs and monitoring metrics
	*Elasticsearch
	// ref is the object selector of the Elasticsearch referenced in the Association used to send and store monitoring data
	ref commonv1.ObjectSelector
}

var _ commonv1.Association = &EsMonitoringAssociation{}

func (ema *EsMonitoringAssociation) Associated() commonv1.Associated {
	if ema == nil {
		return nil
	}
	if ema.Elasticsearch == nil {
		ema.Elasticsearch = &Elasticsearch{}
	}
	return ema.Elasticsearch
}

func (ema *EsMonitoringAssociation) AssociationConfAnnotationName() string {
	return commonv1.ElasticsearchConfigAnnotationName(ema.ref)
}

func (ema *EsMonitoringAssociation) AssociationType() commonv1.AssociationType {
	return commonv1.EsMonitoringAssociationType
}

func (ema *EsMonitoringAssociation) AssociationRef() commonv1.ObjectSelector {
	return ema.ref
}

func (ema *EsMonitoringAssociation) AssociationRefKind() string {
	return "" // Monitoring associations use ObjectSelector, Kind not yet supported
}

func (ema *EsMonitoringAssociation) AssociationConf() (*commonv1.AssociationConf, error) {
	return commonv1.GetAndSetAssociationConfByRef(ema, ema.ref, ema.AssocConfs)
}

func (ema *EsMonitoringAssociation) SetAssociationConf(assocConf *commonv1.AssociationConf) {
	if ema.AssocConfs == nil {
		ema.AssocConfs = make(map[commonv1.ObjectSelector]commonv1.AssociationConf)
	}
	if assocConf != nil {
		ema.AssocConfs[ema.ref] = *assocConf
	}
}

func (ema *EsMonitoringAssociation) SupportsAuthAPIKey() bool {
	return false
}

func (ema *EsMonitoringAssociation) AssociationID() string {
	return ema.ref.ToID()
}

// HasMonitoring methods

func (es *Elasticsearch) GetMonitoringMetricsRefs() []commonv1.ElasticsearchRef {
	return es.Spec.Monitoring.Metrics.ElasticsearchRefs
}

func (es *Elasticsearch) GetMonitoringLogsRefs() []commonv1.ElasticsearchRef {
	return es.Spec.Monitoring.Logs.ElasticsearchRefs
}

func (es *Elasticsearch) MonitoringAssociation(ref commonv1.ObjectSelector) commonv1.Association {
	return &EsMonitoringAssociation{
		Elasticsearch: es,
		ref:           ref.WithDefaultNamespace(es.Namespace),
	}
}

// DisabledPredicates returns the set of predicates that are currently disabled by the
// DisableUpgradePredicatesAnnotation annotation.
func (es Elasticsearch) DisabledPredicates() set.StringSet {
	return setFromAnnotations(DisableUpgradePredicatesAnnotation, es.Annotations)
}
