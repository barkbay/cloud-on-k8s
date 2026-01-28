// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package label

import (
	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/labels"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
)

const (
	// ClusterNameLabelName used to represent a cluster in k8s resources
	ClusterNameLabelName = "elasticsearch.k8s.elastic.co/cluster-name"
	// StatelessClusterNameLabelName used to represent a stateless cluster in k8s resources
	// We need a separate label for stateless clusters to be able to distinguish them from stateful ones in functions
	// like DeleteOrphanedSecrets, NewResourcesStateFromAPI, or NewElasticsearchURLProvider,
	// which only rely on that label to identify resources belonging to a cluster.
	StatelessClusterNameLabelName = "elasticsearch.k8s.elastic.co/stateless-cluster-name"
	// ClusterNamespaceLabelName used to represent a cluster in k8s resources
	ClusterNamespaceLabelName = "elasticsearch.k8s.elastic.co/cluster-namespace"
	// VersionLabelName used to store the Elasticsearch version of the resource
	VersionLabelName = "elasticsearch.k8s.elastic.co/version"
	// PodNameLabelName used to store the name of the pod on other objects
	PodNameLabelName = "elasticsearch.k8s.elastic.co/pod-name"
	// StatefulSetNameLabelName used to store the name of the statefulset.
	StatefulSetNameLabelName = "elasticsearch.k8s.elastic.co/statefulset-name"
	// DeploymentNameLabelName used to store the name of the deployment (for stateless clusters).
	DeploymentNameLabelName = "elasticsearch.k8s.elastic.co/deployment-name"
	// NodeTypesMasterLabelName is a label set to true on nodes with the master role
	NodeTypesMasterLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-master"
	// NodeTypesDataLabelName is a label set to true on nodes with the data role
	NodeTypesDataLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-data"
	// NodeTypesIngestLabelName is a label set to true on nodes with the ingest role
	NodeTypesIngestLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-ingest"
	// NodeTypesMLLabelName is a label set to true on nodes with the ml role
	NodeTypesMLLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-ml"
	// NodeTypesTransformLabelName is a label set to true on nodes with the transform role
	NodeTypesTransformLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-transform"
	// NodeTypesRemoteClusterClientLabelName is a label set to true on nodes with the remote_cluster_client role
	NodeTypesRemoteClusterClientLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-remote_cluster_client"
	// NodeTypesVotingOnlyLabelName is a label set to true on voting-only master-eligible nodes
	NodeTypesVotingOnlyLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-voting_only"
	// NodeTypesDataColdLabelName is a label set to true on nodes with the data_cold role.
	NodeTypesDataColdLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-data_cold"
	// NodeTypesDataContentLabelName is a label set to true on nodes with the data_content role.
	NodeTypesDataContentLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-data_content"
	// NodeTypesDataHotLabelName is a label set to true on nodes with the data_hot role.
	NodeTypesDataHotLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-data_hot"
	// NodeTypesDataWarmLabelName is a label set to true on nodes with the data_warm role.
	NodeTypesDataWarmLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-data_warm"
	// NodeTypesDataFrozenLabelName is a label set to true on nodes with the data_frozen role.
	NodeTypesDataFrozenLabelName labels.TrueFalseLabel = "elasticsearch.k8s.elastic.co/node-data_frozen"

	HTTPSchemeLabelName = "elasticsearch.k8s.elastic.co/http-scheme"

	// Type represents the Elasticsearch type
	Type = "elasticsearch"

	// TierLabelName holds the tier name in the context of Stateless Elasticsearch clusters.
	TierLabelName = "elasticsearch.k8s.elastic.co/tier"
)

// RoleMapping is a struct that maps a label name to a node role.
// This is used when creating PDBs for each node role.
type RoleMapping struct {
	LabelName string
	Role      common.NodeRole
}

// RoleMappings is the definitive list of labels to node roles.
var RoleMappings = []RoleMapping{
	{string(NodeTypesMasterLabelName), common.MasterRole},
	{string(NodeTypesDataLabelName), common.DataRole},
	{string(NodeTypesIngestLabelName), common.IngestRole},
	{string(NodeTypesMLLabelName), common.MLRole},
	{string(NodeTypesTransformLabelName), common.TransformRole},
	{string(NodeTypesRemoteClusterClientLabelName), common.RemoteClusterClientRole},
	{string(NodeTypesDataHotLabelName), common.DataHotRole},
	{string(NodeTypesDataWarmLabelName), common.DataWarmRole},
	{string(NodeTypesDataColdLabelName), common.DataColdRole},
	{string(NodeTypesDataContentLabelName), common.DataContentRole},
	{string(NodeTypesDataFrozenLabelName), common.DataFrozenRole},
}

// NonMasterRoles are all Elasticsearch node roles except master or voting-only.
var NonMasterRoles = []labels.TrueFalseLabel{
	NodeTypesDataLabelName,
	NodeTypesDataHotLabelName,
	NodeTypesDataColdLabelName,
	NodeTypesDataFrozenLabelName,
	NodeTypesDataContentLabelName,
	NodeTypesDataWarmLabelName,
	NodeTypesIngestLabelName,
	NodeTypesMLLabelName,
	NodeTypesRemoteClusterClientLabelName,
	NodeTypesTransformLabelName,
}

// IsMasterNode returns true if the pod has the master node label
func IsMasterNode(pod corev1.Pod) bool {
	return NodeTypesMasterLabelName.HasValue(true, pod.Labels)
}

// IsIndexTierNode returns true if the pod is in the index tier (i.e., is a master node)
func IsIndexTierNode(pod corev1.Pod) bool {
	if pod.Labels == nil {
		return false
	}
	if tier, ok := pod.Labels[TierLabelName]; ok {
		return tier == string(v1alpha1.IndexTierName)
	}
	return false
}

// IsMasterNodeSet returns true if the given StatefulSet specifies master nodes.
func IsMasterNodeSet(statefulSet appsv1.StatefulSet) bool {
	return NodeTypesMasterLabelName.HasValue(true, statefulSet.Spec.Template.Labels)
}

// IsDataNodeSet returns true if the given StatefulSet specifies data nodes.
func IsDataNodeSet(statefulSet appsv1.StatefulSet) bool {
	return NodeTypesDataLabelName.HasValue(true, statefulSet.Spec.Template.Labels) ||
		NodeTypesDataHotLabelName.HasValue(true, statefulSet.Spec.Template.Labels) ||
		NodeTypesDataColdLabelName.HasValue(true, statefulSet.Spec.Template.Labels) ||
		NodeTypesDataContentLabelName.HasValue(true, statefulSet.Spec.Template.Labels) ||
		NodeTypesDataWarmLabelName.HasValue(true, statefulSet.Spec.Template.Labels)
}

// IsIngestNodeSet returns true if the given StatefulSet specifies ingest nodes.
func IsIngestNodeSet(statefulSet appsv1.StatefulSet) bool {
	return NodeTypesIngestLabelName.HasValue(true, statefulSet.Spec.Template.Labels)
}

func FilterMasterNodePods(pods []corev1.Pod) []corev1.Pod {
	masters := []corev1.Pod{}
	for _, pod := range pods {
		if IsMasterNode(pod) {
			masters = append(masters, pod)
		}
	}
	return masters
}

// IsDataNode returns true if the pod has the data node label
func IsDataNode(pod corev1.Pod) bool {
	return NodeTypesDataLabelName.HasValue(true, pod.Labels)
}

// ExtractVersion extracts the Elasticsearch version from the given labels.
func ExtractVersion(labels map[string]string) (version.Version, error) {
	return version.FromLabels(labels, VersionLabelName)
}

// NewLabels constructs a new set of labels from an Elasticsearch definition.
func NewLabels(es types.NamespacedName, isStateless bool) map[string]string {
	clusterNameLabelName := ClusterNameLabelName
	if isStateless {
		clusterNameLabelName = StatelessClusterNameLabelName
	}
	return map[string]string{
		clusterNameLabelName:   es.Name,
		commonv1.TypeLabelName: Type,
	}
}

// NewDeploymentLabels returns labels to apply for an Elasticsearch Deployment in stateless mode.
func NewDeploymentLabels(es types.NamespacedName, deploymentName string) map[string]string {
	lbls := NewLabels(es, true)
	lbls[DeploymentNameLabelName] = deploymentName
	return lbls
}

// NewPodLabels returns labels to apply for a new Elasticsearch pod.
func NewPodLabels(
	es types.NamespacedName,
	isStateless bool,
	ssetName string,
	ver version.Version,
	nodeRoles *common.Node,
	scheme string,
) map[string]string {
	// cluster name based labels
	labels := NewLabels(es, isStateless)
	// version label
	labels[VersionLabelName] = ver.String()
	labels[HTTPSchemeLabelName] = scheme
	if isStateless {
		for k, v := range NewDeploymentLabels(es, ssetName) {
			labels[k] = v
		}
		return labels
	}

	// node types labels
	NodeTypesMasterLabelName.Set(nodeRoles.IsConfiguredWithRole(common.MasterRole), labels)
	NodeTypesDataLabelName.Set(nodeRoles.IsConfiguredWithRole(common.DataRole), labels)
	NodeTypesIngestLabelName.Set(nodeRoles.IsConfiguredWithRole(common.IngestRole), labels)
	NodeTypesMLLabelName.Set(nodeRoles.IsConfiguredWithRole(common.MLRole), labels)
	// transform and remote_cluster_client roles were only added in 7.7.0 so we should not annotate previous versions with them
	if ver.GTE(version.From(7, 7, 0)) {
		NodeTypesTransformLabelName.Set(nodeRoles.IsConfiguredWithRole(common.TransformRole), labels)
		NodeTypesRemoteClusterClientLabelName.Set(nodeRoles.IsConfiguredWithRole(common.RemoteClusterClientRole), labels)
	}
	// voting_only master eligible nodes were added only in 7.3.0 so we don't want to label prior versions with it
	if ver.GTE(version.From(7, 3, 0)) {
		NodeTypesVotingOnlyLabelName.Set(nodeRoles.IsConfiguredWithRole(common.VotingOnlyRole), labels)
	}
	// data tiers were added in 7.10.0
	if ver.GTE(version.From(7, 10, 0)) {
		NodeTypesDataContentLabelName.Set(nodeRoles.IsConfiguredWithRole(common.DataContentRole), labels)
		NodeTypesDataColdLabelName.Set(nodeRoles.IsConfiguredWithRole(common.DataColdRole), labels)
		NodeTypesDataHotLabelName.Set(nodeRoles.IsConfiguredWithRole(common.DataHotRole), labels)
		NodeTypesDataWarmLabelName.Set(nodeRoles.IsConfiguredWithRole(common.DataWarmRole), labels)
	}
	// frozen tier has been introduced in 7.12.0
	if ver.GTE(version.From(7, 12, 0)) {
		NodeTypesDataFrozenLabelName.Set(nodeRoles.IsConfiguredWithRole(common.DataFrozenRole), labels)
	}

	// apply stateful set label selector
	for k, v := range NewStatefulSetLabels(es, ssetName) {
		labels[k] = v
	}

	return labels
}

// NewConfigLabels returns labels to apply for an Elasticsearch Config secret.
func NewConfigLabels(es types.NamespacedName, ssetName string) map[string]string {
	return NewStatefulSetLabels(es, ssetName)
}

func NewStatefulSetLabels(es types.NamespacedName, ssetName string) map[string]string {
	lbls := NewLabels(es, false)
	lbls[StatefulSetNameLabelName] = ssetName
	return lbls
}

// NewLabelSelectorForElasticsearch returns a labels.Selector that matches the labels as constructed by NewLabels
func NewLabelSelectorForElasticsearch(es common.ElasticsearchCluster) client.MatchingLabels {
	if es.IsStateless() {
		return NewLabelSelectorForElasticsearchStatelessClusterName(es.GetName())
	}
	return NewLabelSelectorForElasticsearchClusterName(es.GetName())
}

// NewLabelSelectorForElasticsearchStatelessClusterName returns a labels.Selector that matches the labels as constructed by
// NewLabels for the provided cluster name.
func NewLabelSelectorForElasticsearchStatelessClusterName(clusterName string) client.MatchingLabels {
	return client.MatchingLabels(map[string]string{StatelessClusterNameLabelName: clusterName})
}

// NewLabelSelectorForElasticsearchClusterName returns a labels.Selector that matches the labels as constructed by
// NewLabels for the provided cluster name.
func NewLabelSelectorForElasticsearchClusterName(clusterName string) client.MatchingLabels {
	return client.MatchingLabels(map[string]string{ClusterNameLabelName: clusterName})
}

// NewLabelSelectorForStatefulSetName returns a labels.Selector that matches the labels set on resources managed for
// a given StatefulSet in a cluster.
func NewLabelSelectorForStatefulSetName(clusterName, ssetName string) client.MatchingLabels {
	return client.MatchingLabels(map[string]string{
		ClusterNameLabelName:     clusterName,
		StatefulSetNameLabelName: ssetName,
	})
}

// NewLabelSelectorForDeploymentName returns a labels.Selector that matches the labels set on resources managed for
// a given Deployment in a stateless cluster.
func NewLabelSelectorForDeploymentName(clusterName, deploymentName string) client.MatchingLabels {
	return client.MatchingLabels(map[string]string{
		StatelessClusterNameLabelName: clusterName,
		DeploymentNameLabelName:       deploymentName,
	})
}
