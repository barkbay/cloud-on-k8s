// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/optional"
)

const (
	// ElasticsearchContainerName is the name of the Elasticsearch container in pods.
	ElasticsearchContainerName = escommon.ElasticsearchContainerName
	// Kind is inferred from the struct name using reflection in SchemeBuilder.Register()
	// we duplicate it as a constant here for practical purposes.
	Kind = "ElasticsearchStateless"
	// DownwardNodeLabelsAnnotation holds an optional list of expected node labels to be set as annotations on the Elasticsearch Pods.
	DownwardNodeLabelsAnnotation = "eck.k8s.elastic.co/downward-node-labels"
)

// ElasticsearchStatelessSpec defines the desired state of an ElasticsearchStateless cluster.
type ElasticsearchStatelessSpec struct {
	// Version of Elasticsearch.
	Version string `json:"version"`

	// Image is the Elasticsearch Docker image to deploy.
	// +kubebuilder:validation:Optional
	Image string `json:"image,omitempty"`

	// RemoteClusterServer specifies if the remote cluster server should be enabled.
	// This must be enabled if this cluster is a remote cluster which is expected to be accessed using API key authentication.
	// +kubebuilder:validation:Optional
	RemoteClusterServer escommon.RemoteClusterServer `json:"remoteClusterServer,omitempty"`

	// ObjectStore contains the configuration for the object store used for stateless data.
	// +kubebuilder:validation:Required
	ObjectStore ObjectStoreConfig `json:"objectStore"`

	// Tiers defines the different tiers of the stateless Elasticsearch cluster.
	// +kubebuilder:validation:Required
	Tiers ElasticsearchStatelessTiers `json:"tiers"`

	// HTTP holds HTTP layer settings for Elasticsearch.
	// +kubebuilder:validation:Optional
	HTTP commonv1.HTTPConfig `json:"http,omitempty"`

	// Transport holds transport layer settings for Elasticsearch.
	// +kubebuilder:validation:Optional
	Transport escommon.TransportConfig `json:"transport,omitempty"`

	// Auth contains user authentication and authorization security settings for Elasticsearch.
	// +kubebuilder:validation:Optional
	Auth escommon.Auth `json:"auth,omitempty"`

	// SecureSettings is a list of references to Kubernetes secrets containing sensitive configuration options for Elasticsearch.
	// +kubebuilder:validation:Optional
	SecureSettings []commonv1.SecretSource `json:"secureSettings,omitempty"`

	// ServiceAccountName is used to check access from the current resource to a resource (for ex. a remote Elasticsearch cluster) in a different namespace.
	// Can only be used if ECK is enforcing RBAC on references.
	// +kubebuilder:validation:Optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// RemoteClusters enables you to establish uni-directional connections to a remote Elasticsearch cluster.
	// +optional
	RemoteClusters []escommon.RemoteCluster `json:"remoteClusters,omitempty"`

	// Monitoring enables you to collect and ship log and monitoring data of this Elasticsearch cluster.
	// See https://www.elastic.co/guide/en/elasticsearch/reference/current/monitor-elasticsearch-cluster.html.
	// Metricbeat and Filebeat are deployed in the same Pod as sidecars and each one sends data to one or two different
	// Elasticsearch monitoring clusters running in the same Kubernetes cluster.
	// +kubebuilder:validation:Optional
	Monitoring commonv1.Monitoring `json:"monitoring,omitempty"`
}

// ObjectStoreConfig contains the configuration for the object store used for stateless data.
type ObjectStoreConfig struct {
	// SecretName is the name of the secret containing the object store credentials.
	// +kubebuilder:validation:Required
	SecretName string `json:"secretName"`
}

// ElasticsearchStatelessTiers defines the tiers of a stateless Elasticsearch cluster.
type ElasticsearchStatelessTiers struct {
	// Index tier handles indexing operations.
	// +kubebuilder:validation:Optional
	Index *TierSpec `json:"index,omitempty"`

	// Search tier handles search operations.
	// +kubebuilder:validation:Optional
	Search *TierSpec `json:"search,omitempty"`

	// ML tier handles machine learning operations.
	// +kubebuilder:validation:Optional
	ML *TierSpec `json:"ml,omitempty"`
}

// TierSpec defines the specification for a tier in a stateless Elasticsearch cluster.
type TierSpec struct {
	// Replicas is the number of replicas for this tier.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// PodTemplate provides customisation options for the Pods belonging to this tier.
	// +kubebuilder:validation:Optional
	// +kubebuilder:pruning:PreserveUnknownFields
	PodTemplate corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
}

// ElasticsearchStatelessStatus represents the observed state of ElasticsearchStateless.
type ElasticsearchStatelessStatus struct {
	// AvailableNodes is the number of available instances.
	AvailableNodes int32 `json:"availableNodes,omitempty"`

	// Version of the stack resource currently running.
	Version string `json:"version,omitempty"`

	// Health is the health of the cluster as returned by the health API.
	Health escommon.ElasticsearchHealth `json:"health,omitempty"`

	// Phase is the phase Elasticsearch is in from the controller point of view.
	Phase escommon.ElasticsearchOrchestrationPhase `json:"phase,omitempty"`

	// ObservedGeneration is the most recent generation observed for this ElasticsearchStateless cluster.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// IndexTierStatus contains the status of the index tier.
	// +kubebuilder:validation:Optional
	IndexTierStatus *TierStatus `json:"indexTierStatus,omitempty"`

	// SearchTierStatus contains the status of the search tier.
	// +kubebuilder:validation:Optional
	SearchTierStatus *TierStatus `json:"searchTierStatus,omitempty"`

	// MLTierStatus contains the status of the ML tier.
	// +kubebuilder:validation:Optional
	MLTierStatus *TierStatus `json:"mlTierStatus,omitempty"`

	// MonitoringAssociationStatus is the status of any auto-linking to monitoring Elasticsearch clusters.
	MonitoringAssociationStatus commonv1.AssociationStatusMap `json:"monitoringAssociationStatus,omitempty"`
}

// TierStatus represents the status of a tier.
type TierStatus struct {
	// AvailableReplicas is the number of available replicas for this tier.
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// ExpectedReplicas is the expected number of replicas for this tier.
	ExpectedReplicas int32 `json:"expectedReplicas,omitempty"`
}

// +kubebuilder:object:root=true

// ElasticsearchStateless represents a stateless Elasticsearch resource in a Kubernetes cluster.
// +kubebuilder:resource:categories=elastic,shortName=ess
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="health",type="string",JSONPath=".status.health"
// +kubebuilder:printcolumn:name="nodes",type="integer",JSONPath=".status.availableNodes",description="Available nodes"
// +kubebuilder:printcolumn:name="version",type="string",JSONPath=".status.version",description="Elasticsearch version"
// +kubebuilder:printcolumn:name="phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:storageversion
type ElasticsearchStateless struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec      ElasticsearchStatelessSpec                           `json:"spec,omitempty"`
	Status    ElasticsearchStatelessStatus                         `json:"status,omitempty"`
	AssocConfs map[commonv1.ObjectSelector]commonv1.AssociationConf `json:"-"`
}

// IsMarkedForDeletion returns true if the ElasticsearchStateless is going to be deleted.
func (ess *ElasticsearchStateless) IsMarkedForDeletion() bool {
	return !ess.DeletionTimestamp.IsZero()
}

// ServiceAccountName returns the service account name.
func (ess *ElasticsearchStateless) ServiceAccountName() string {
	return ess.Spec.ServiceAccountName
}

// SecureSettings returns the secure settings.
func (ess *ElasticsearchStateless) SecureSettings() []commonv1.SecretSource {
	return ess.Spec.SecureSettings
}

// GetObservedGeneration will return the observed generation from the ElasticsearchStateless status.
func (ess *ElasticsearchStateless) GetObservedGeneration() int64 {
	return ess.Status.ObservedGeneration
}

// GetVersion returns the Elasticsearch version.
func (ess ElasticsearchStateless) GetVersion() string {
	return ess.Spec.Version
}

// GetImage returns the Elasticsearch Docker image.
func (ess ElasticsearchStateless) GetImage() string {
	return ess.Spec.Image
}

// GetHTTP returns the HTTP layer configuration.
func (ess ElasticsearchStateless) GetHTTP() commonv1.HTTPConfig {
	return ess.Spec.HTTP
}

// GetTransport returns the transport layer configuration.
func (ess ElasticsearchStateless) GetTransport() escommon.TransportConfig {
	return ess.Spec.Transport
}

// GetAuth returns the authentication and authorization settings.
func (ess ElasticsearchStateless) GetAuth() escommon.Auth {
	return ess.Spec.Auth
}

// GetSecureSettings returns the list of secure settings secret sources.
func (ess ElasticsearchStateless) GetSecureSettings() []commonv1.SecretSource {
	return ess.Spec.SecureSettings
}

// GetServiceAccountName returns the service account name.
func (ess ElasticsearchStateless) GetServiceAccountName() string {
	return ess.Spec.ServiceAccountName
}

// GetRemoteClusterServer returns the remote cluster server configuration.
func (ess ElasticsearchStateless) GetRemoteClusterServer() escommon.RemoteClusterServer {
	return ess.Spec.RemoteClusterServer
}

// GetRemoteClusters returns the list of remote cluster configurations.
func (ess ElasticsearchStateless) GetRemoteClusters() []escommon.RemoteCluster {
	return ess.Spec.RemoteClusters
}

// SupportsRemoteClusterAPIKeys returns true if this cluster supports connecting to a remote cluster using API keys.
// Stateless clusters always support API keys.
func (ess ElasticsearchStateless) SupportsRemoteClusterAPIKeys() (*optional.Bool, error) {
	return optional.NewBool(true), nil
}

// IsStateless returns true for stateless Elasticsearch clusters.
func (ess ElasticsearchStateless) IsStateless() bool {
	return true
}

// DownwardNodeLabels returns the set of expected node labels to be copied as annotations on the Elasticsearch Pods.
func (ess ElasticsearchStateless) DownwardNodeLabels() []string {
	expectedAnnotations, exist := ess.Annotations[DownwardNodeLabelsAnnotation]
	expectedAnnotations = strings.TrimSpace(expectedAnnotations)
	if !exist || expectedAnnotations == "" {
		return nil
	}
	return strings.Split(expectedAnnotations, ",")
}

// HasDownwardNodeLabels returns true if some node labels are expected on the Elasticsearch Pods.
func (ess ElasticsearchStateless) HasDownwardNodeLabels() bool {
	return len(ess.DownwardNodeLabels()) > 0
}

// IsConfiguredToAllowDowngrades returns true if the DisableDowngradeValidation annotation is set to the value of true.
func (ess ElasticsearchStateless) IsConfiguredToAllowDowngrades() bool {
	return commonv1.IsConfiguredToAllowDowngrades(&ess)
}

// -- HasMonitoring methods

// GetMonitoringMetricsRefs returns the list of Elasticsearch clusters to which monitoring metrics are sent.
func (ess *ElasticsearchStateless) GetMonitoringMetricsRefs() []commonv1.ElasticsearchRef {
	return ess.Spec.Monitoring.Metrics.ElasticsearchRefs
}

// GetMonitoringLogsRefs returns the list of Elasticsearch clusters to which monitoring logs are sent.
func (ess *ElasticsearchStateless) GetMonitoringLogsRefs() []commonv1.ElasticsearchRef {
	return ess.Spec.Monitoring.Logs.ElasticsearchRefs
}

// MonitoringAssociation returns the Association for the given ObjectSelector.
func (ess *ElasticsearchStateless) MonitoringAssociation(ref commonv1.ObjectSelector) commonv1.Association {
	return &EssMonitoringAssociation{
		ElasticsearchStateless: ess,
		ref:                    ref.WithDefaultNamespace(ess.Namespace),
	}
}

// GetAssociations returns the list of associations for this ElasticsearchStateless cluster.
func (ess *ElasticsearchStateless) GetAssociations() []commonv1.Association {
	associations := make([]commonv1.Association, 0)
	for _, ref := range ess.Spec.Monitoring.Metrics.ElasticsearchRefs {
		if ref.IsDefined() {
			associations = append(associations, &EssMonitoringAssociation{
				ElasticsearchStateless: ess,
				ref:                    ref.ObjectSelector.WithDefaultNamespace(ess.Namespace),
			})
		}
	}
	for _, ref := range ess.Spec.Monitoring.Logs.ElasticsearchRefs {
		if ref.IsDefined() {
			associations = append(associations, &EssMonitoringAssociation{
				ElasticsearchStateless: ess,
				ref:                    ref.ObjectSelector.WithDefaultNamespace(ess.Namespace),
			})
		}
	}
	return associations
}

// AssociationStatusMap returns the association status map for the given association type.
func (ess *ElasticsearchStateless) AssociationStatusMap(typ commonv1.AssociationType) commonv1.AssociationStatusMap {
	if typ != commonv1.EsMonitoringAssociationType {
		return commonv1.AssociationStatusMap{}
	}

	return ess.Status.MonitoringAssociationStatus
}

// SetAssociationStatusMap sets the association status map for the given association type.
func (ess *ElasticsearchStateless) SetAssociationStatusMap(typ commonv1.AssociationType, status commonv1.AssociationStatusMap) error {
	if typ != commonv1.EsMonitoringAssociationType {
		return fmt.Errorf("association type %s not known", typ)
	}

	ess.Status.MonitoringAssociationStatus = status
	return nil
}

// Ensure ElasticsearchStateless implements escommon.ElasticsearchCluster interface.
var _ escommon.ElasticsearchCluster = &ElasticsearchStateless{}

// Ensure ElasticsearchStateless implements commonv1.Associated interface.
var _ commonv1.Associated = &ElasticsearchStateless{}

// -- association with monitoring Elasticsearch clusters

// EssMonitoringAssociation helps to manage ElasticsearchStateless+Metricbeat+Filebeat <-> Elasticsearch(es) associations
type EssMonitoringAssociation struct {
	// The monitored ElasticsearchStateless cluster from where are collected logs and monitoring metrics
	*ElasticsearchStateless
	// ref is the object selector of the Elasticsearch referenced in the Association used to send and store monitoring data
	ref commonv1.ObjectSelector
}

var _ commonv1.Association = &EssMonitoringAssociation{}

func (ema *EssMonitoringAssociation) Associated() commonv1.Associated {
	if ema == nil {
		return nil
	}
	if ema.ElasticsearchStateless == nil {
		ema.ElasticsearchStateless = &ElasticsearchStateless{}
	}
	return ema.ElasticsearchStateless
}

func (ema *EssMonitoringAssociation) AssociationConfAnnotationName() string {
	return commonv1.ElasticsearchConfigAnnotationName(ema.ref)
}

func (ema *EssMonitoringAssociation) AssociationType() commonv1.AssociationType {
	return commonv1.EsMonitoringAssociationType
}

func (ema *EssMonitoringAssociation) AssociationRef() commonv1.ObjectSelector {
	return ema.ref
}

func (ema *EssMonitoringAssociation) AssociationRefKind() string {
	return "" // Monitoring associations use ObjectSelector, Kind not yet supported
}

func (ema *EssMonitoringAssociation) AssociationConf() (*commonv1.AssociationConf, error) {
	return commonv1.GetAndSetAssociationConfByRef(ema, ema.ref, ema.AssocConfs)
}

func (ema *EssMonitoringAssociation) SetAssociationConf(assocConf *commonv1.AssociationConf) {
	if ema.AssocConfs == nil {
		ema.AssocConfs = make(map[commonv1.ObjectSelector]commonv1.AssociationConf)
	}
	if assocConf != nil {
		ema.AssocConfs[ema.ref] = *assocConf
	}
}

func (ema *EssMonitoringAssociation) SupportsAuthAPIKey() bool {
	return false
}

func (ema *EssMonitoringAssociation) AssociationID() string {
	return ema.ref.ToID()
}

func (ema *EssMonitoringAssociation) ElasticServiceAccount() (commonv1.ServiceAccountName, error) {
	return "", nil
}

// -- Status methods for ElasticsearchCluster interface

// GetStatusHealth returns the current health status.
func (ess *ElasticsearchStateless) GetStatusHealth() escommon.ElasticsearchHealth {
	return ess.Status.Health
}

// SetStatusHealth sets the health status.
func (ess *ElasticsearchStateless) SetStatusHealth(health escommon.ElasticsearchHealth) {
	ess.Status.Health = health
}

// GetStatusPhase returns the current orchestration phase.
func (ess *ElasticsearchStateless) GetStatusPhase() escommon.ElasticsearchOrchestrationPhase {
	return ess.Status.Phase
}

// SetStatusPhase sets the orchestration phase.
func (ess *ElasticsearchStateless) SetStatusPhase(phase escommon.ElasticsearchOrchestrationPhase) {
	ess.Status.Phase = phase
}

// GetStatusAvailableNodes returns the number of available nodes.
func (ess *ElasticsearchStateless) GetStatusAvailableNodes() int32 {
	return ess.Status.AvailableNodes
}

// SetStatusAvailableNodes sets the number of available nodes.
func (ess *ElasticsearchStateless) SetStatusAvailableNodes(nodes int32) {
	ess.Status.AvailableNodes = nodes
}

// GetStatusVersion returns the running version.
func (ess *ElasticsearchStateless) GetStatusVersion() string {
	return ess.Status.Version
}

// SetStatusVersion sets the running version.
func (ess *ElasticsearchStateless) SetStatusVersion(version string) {
	ess.Status.Version = version
}

// GetStatusObservedGeneration returns the observed generation.
func (ess *ElasticsearchStateless) GetStatusObservedGeneration() int64 {
	return ess.Status.ObservedGeneration
}

// SetStatusObservedGeneration sets the observed generation.
func (ess *ElasticsearchStateless) SetStatusObservedGeneration(generation int64) {
	ess.Status.ObservedGeneration = generation
}

// StatusIsDegraded returns true if the current status is worse than the given previous status.
func (ess *ElasticsearchStateless) StatusIsDegraded(prevHealth escommon.ElasticsearchHealth) bool {
	return ess.Status.Health.Less(prevHealth)
}

// +kubebuilder:object:root=true

// ElasticsearchStatelessList contains a list of ElasticsearchStateless clusters.
type ElasticsearchStatelessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ElasticsearchStateless `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ElasticsearchStateless{}, &ElasticsearchStatelessList{})
}
