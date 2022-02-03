// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package reconcile

import (
	"reflect"
	"sort"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/shutdown"

	"k8s.io/utils/pointer"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

type StatusReporter struct {
	esv1.Conditions
	*UpscaleReporter
	*DownscaleReporter
	*UpgradeReporter
}

func (s *StatusReporter) MergeStatusReportingWith(otherStatus esv1.ElasticsearchStatus) esv1.ElasticsearchStatus {
	mergedStatus := otherStatus.DeepCopy()
	mergedStatus.RollingUpgradeOperation = s.UpgradeReporter.Merge(otherStatus.RollingUpgradeOperation)
	mergedStatus.UpscaleOperation = s.UpscaleReporter.Merge(otherStatus.UpscaleOperation)
	mergedStatus.DownscaleOperation = s.DownscaleReporter.Merge(otherStatus.DownscaleOperation)

	// Merge conditions
	for _, condition := range s.Conditions {
		mergedStatus.Conditions = mergedStatus.Conditions.MergeWith(condition)
	}

	return *mergedStatus
}

func (s *StatusReporter) ReportCondition(
	conditionType esv1.ConditionType,
	status corev1.ConditionStatus,
	message string) {
	s.Conditions = append(s.Conditions, esv1.Condition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Message:            message,
	})
}

// -- Upscale status

type UpscaleReporter struct {
	// Expected nodes to be upscaled
	nodes []string
}

func (u *UpscaleReporter) RecordNodesToBeUpscaled(nodes []string) {
	if nodes == nil {
		nodes = []string{}
	}
	sort.Strings(nodes)
	u.nodes = nodes
}

func (u *UpscaleReporter) Merge(other esv1.UpscaleOperation) esv1.UpscaleOperation {
	upscaleOperation := other.DeepCopy()
	if u == nil {
		return *upscaleOperation
	}
	nodes := make([]esv1.NewNode, 0, len(u.nodes))
	for _, node := range u.nodes {
		nodes = append(nodes, esv1.NewNode{
			Name: node,
		})
	}
	// Sort for stable comparison
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})
	if (u.nodes != nil && !reflect.DeepEqual(nodes, other.Nodes)) || upscaleOperation.LastUpdatedTime.IsZero() {
		upscaleOperation.Nodes = nodes
		upscaleOperation.LastUpdatedTime = metav1.Now()
	}
	return *upscaleOperation
}

// -- Upgrade status

type UpgradeReporter struct {
	// Expected nodes to be upgraded, key is node name
	nodes map[string]esv1.UpgradedNode

	// Predicate results
	predicatesResult map[string][]string
}

func (u *UpgradeReporter) RecordNodesToBeUpgraded(nodes []string) {
	if u.nodes == nil {
		u.nodes = make(map[string]esv1.UpgradedNode, len(nodes))
	}
	for _, node := range nodes {
		upgradedNode := u.nodes[node]
		upgradedNode.Name = node
		upgradedNode.DeleteStatus = "PENDING"
		u.nodes[node] = upgradedNode
	}
}

func (u *UpgradeReporter) RecordDeletedNode(node string) {
	if u.nodes == nil {
		u.nodes = make(map[string]esv1.UpgradedNode)
	}
	upgradedNode := u.nodes[node]
	upgradedNode.Name = node
	upgradedNode.DeleteStatus = "DELETED"
	u.nodes[node] = upgradedNode
}

// RecordPredicatesResult records predicates results for a set of nodes
func (u *UpgradeReporter) RecordPredicatesResult(predicatesResult map[string]string) {
	if u.nodes == nil {
		u.nodes = make(map[string]esv1.UpgradedNode, len(predicatesResult))
	}
	for node, predicate := range predicatesResult {
		upgradedNode := u.nodes[node]
		upgradedNode.Name = node
		upgradedNode.Predicate = pointer.String(predicate)
		u.nodes[node] = upgradedNode
	}
}

func (u *UpgradeReporter) Merge(other esv1.UpgradeOperation) esv1.UpgradeOperation {
	upgradeOperation := other.DeepCopy()
	if u == nil {
		return *upgradeOperation
	}
	nodes := make([]esv1.UpgradedNode, 0, len(u.nodes))
	for _, node := range u.nodes {
		nodes = append(nodes, esv1.UpgradedNode{
			Name:         node.Name,
			Predicate:    node.Predicate,
			DeleteStatus: node.DeleteStatus,
		})
	}
	// Sort for stable comparison
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})
	if (u.nodes != nil && !reflect.DeepEqual(nodes, other.Nodes)) || upgradeOperation.LastUpdatedTime.IsZero() {
		upgradeOperation.Nodes = nodes
		upgradeOperation.LastUpdatedTime = metav1.Now()
	}
	return *upgradeOperation
}

// -- Downscale status

type DownscaleReporter struct {
	// Expected nodes to be downscaled, key is node name
	nodes   map[string]esv1.DownscaledNode
	stalled *bool
}

func (d *DownscaleReporter) RecordNodesToBeRemoved(nodes []string) {
	if d.nodes == nil {
		d.nodes = make(map[string]esv1.DownscaledNode, len(nodes))
	}
	for _, node := range nodes {
		d.nodes[node] = esv1.DownscaledNode{
			Name: node,
			// We set an initial value to let the caller known that this node should be eventually deleted.
			// This should be overridden by the downscale algorithm.
			ShutdownStatus: "NOT_STARTED",
		}
	}
}

func (d *DownscaleReporter) Merge(other esv1.DownscaleOperation) esv1.DownscaleOperation {
	downscaleOperation := other.DeepCopy()
	if d == nil {
		return other
	}
	nodes := make([]esv1.DownscaledNode, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, esv1.DownscaledNode{
			Name:           node.Name,
			ShutdownStatus: node.ShutdownStatus,
			Explanation:    node.Explanation,
		})
	}
	// Sort for stable comparison
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})
	if (d.nodes != nil && !reflect.DeepEqual(nodes, other.Nodes)) || downscaleOperation.LastUpdatedTime.IsZero() {
		downscaleOperation.Nodes = nodes
		downscaleOperation.LastUpdatedTime = metav1.Now()
	}

	if !reflect.DeepEqual(d.stalled, other.Stalled) {
		downscaleOperation.Stalled = d.stalled
		downscaleOperation.LastUpdatedTime = metav1.Now()
	}

	return *downscaleOperation
}

func (d *DownscaleReporter) OnShutdownStatus(
	podName string,
	nodeShutdownStatus shutdown.NodeShutdownStatus,
) {
	if d == nil {
		return
	}
	if d.nodes == nil {
		d.nodes = make(map[string]esv1.DownscaledNode)
	}
	node := d.nodes[podName]
	node.ShutdownStatus = string(nodeShutdownStatus.Status)

	if len(nodeShutdownStatus.Explanation) > 0 {
		node.Explanation = pointer.StringPtr(nodeShutdownStatus.Explanation)
	}
	d.nodes[podName] = node
	if nodeShutdownStatus.Status == esclient.ShutdownStalled {
		d.stalled = pointer.Bool(true)
	}
}

func (d *DownscaleReporter) OnReconcileShutdowns(
	leavingNodes []string,
) {
	if d == nil {
		return
	}
	if d.nodes == nil {
		d.nodes = make(map[string]esv1.DownscaledNode)
	}
	// Update InProgress condition and DownscaleOperation
	for _, nodeName := range leavingNodes {
		node := d.nodes[nodeName]
		node.ShutdownStatus = string(esclient.ShutdownInProgress)
		d.nodes[nodeName] = node
	}
}
