// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package reconcile

import (
	"reflect"
	"sort"

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
	if (u.nodes != nil && !reflect.DeepEqual(u.nodes, other.Nodes)) || upscaleOperation.LastUpdatedTime.IsZero() {
		upscaleOperation.Nodes = u.nodes
		upscaleOperation.LastUpdatedTime = metav1.Now()
	}
	return *upscaleOperation
}

// -- Upgrade status

type UpgradeReporter struct {
	// Expected nodes to be upgraded
	nodes []string
}

func (u *UpgradeReporter) RecordNodesToBeUpgraded(nodes []string) {
	if nodes == nil {
		nodes = []string{}
	}
	sort.Strings(nodes)
	u.nodes = nodes
}

func (u *UpgradeReporter) Merge(other esv1.RollingUpgradeOperation) esv1.RollingUpgradeOperation {
	upgradeOperation := other.DeepCopy()
	if u == nil {
		return *upgradeOperation
	}
	if (u.nodes != nil && !reflect.DeepEqual(u.nodes, other.Nodes)) || upgradeOperation.LastUpdatedTime.IsZero() {
		upgradeOperation.Nodes = u.nodes
		upgradeOperation.LastUpdatedTime = metav1.Now()
	}
	return *upgradeOperation
}

// -- Downscale status

type DownscaleReporter struct {
	// Expected nodes to be downscaled
	nodes []string

	shardMigrationStatuses esv1.ShardMigrationStatuses
	stalled                *bool
}

func (d *DownscaleReporter) RecordNodesToBeRemoved(nodes []string) {
	if nodes == nil {
		nodes = []string{}
	}
	sort.Strings(nodes)
	d.nodes = nodes
}

func (d *DownscaleReporter) Merge(other esv1.DownscaleOperation) esv1.DownscaleOperation {
	downscaleOperation := other.DeepCopy()
	if d == nil {
		return other
	}

	if (d.nodes != nil && !reflect.DeepEqual(d.nodes, other.Nodes)) || downscaleOperation.LastUpdatedTime.IsZero() {
		downscaleOperation.Nodes = d.nodes
		downscaleOperation.LastUpdatedTime = metav1.Now()
	}

	if !d.shardMigrationStatuses.Equals(other.ShardMigrationStatuses) {
		downscaleOperation.ShardMigrationStatuses = d.shardMigrationStatuses
		downscaleOperation.LastUpdatedTime = metav1.Now()
	}

	if !reflect.DeepEqual(d.stalled, other.Stalled) {
		downscaleOperation.Stalled = d.stalled
		downscaleOperation.LastUpdatedTime = metav1.Now()
	}

	return *downscaleOperation
}

func (d *DownscaleReporter) RecordShutdownAPIResult(
	response esclient.ShutdownResponse,
	podToNodeID map[string]string,
) {
	if d == nil {
		return
	}
	if d.shardMigrationStatuses == nil {
		d.shardMigrationStatuses = make(map[string]esv1.ShardMigrationStatus)
	}
	// Update InProgress condition and DownscaleOperation
	for _, node := range response.Nodes {
		if !node.Is(esclient.Remove) || node.Status == esclient.ShutdownComplete {
			continue
		}
		podName := getPod(podToNodeID, node.NodeID)
		d.shardMigrationStatuses[podName] = esv1.ShardMigrationStatus{
			ShardsRemaining: pointer.Int(node.ShardMigration.ShardsRemaining),
			ShutdownStatus:  string(node.ShardMigration.Status),
			Explanation:     pointer.StringPtr(node.ShardMigration.Explanation),
		}
		if node.Status == esclient.ShutdownStalled {
			d.stalled = pointer.Bool(true)
		}
	}
}

// RecordMigratingData should be used for clusters which are not compatible with the Shutdown API.
func (d *DownscaleReporter) RecordMigratingData(
	leavingNodes []string,
) {
	if d == nil {
		return
	}
	if d.shardMigrationStatuses == nil {
		d.shardMigrationStatuses = make(map[string]esv1.ShardMigrationStatus)
	}
	// Update InProgress condition and DownscaleOperation
	for _, node := range leavingNodes {
		d.shardMigrationStatuses[node] = esv1.ShardMigrationStatus{
			ShutdownStatus: string(esclient.ShutdownStarted),
		}
	}
}

// getPod returns pod name from the node id, it returns the node id as a best effort if not found.
func getPod(podToNodeID map[string]string, nodeId string) string {
	for pod, id := range podToNodeID {
		if id == nodeId {
			return pod
		}
	}
	return nodeId
}

func (d *DownscaleReporter) RecordDeleteShutdown(
	nodeID string,
	podToNodeID map[string]string,
) {
	if d == nil || d.shardMigrationStatuses == nil {
		return
	}
	delete(d.shardMigrationStatuses, getPod(podToNodeID, nodeID))
}
