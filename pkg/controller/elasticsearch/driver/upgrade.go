// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

func (d *defaultDriver) handleRollingUpgrades(
	esClient esclient.Client,
	statefulSets sset.StatefulSetList,
) *reconciler.Results {
	results := &reconciler.Results{}

	// We need an up-to-date ES state, but avoid requesting information we may not need.
	esState := NewLazyESState(esClient)

	// Maybe upgrade some of the nodes.
	res := newRollingUpgrade(d, esClient, esState, statefulSets).run()
	results.WithResults(res)

	// Maybe re-enable shards allocation if upgraded nodes are back into the cluster.
	res = d.MaybeEnableShardsAllocation(esClient, esState, statefulSets)
	results.WithResults(res)

	return results
}

type rollingUpgradeCtx struct {
	client             k8s.Client
	ES                 v1alpha1.Elasticsearch
	statefulSets       sset.StatefulSetList
	esClient           esclient.Client
	esState            ESState
	deleteExpectations *DeleteExpectations
	podUpgradeDone     func(k8s.Client, ESState, corev1.Pod, string) (bool, error)
}

func newRollingUpgrade(
	d *defaultDriver,
	esClient esclient.Client,
	esState ESState,
	statefulSets sset.StatefulSetList,
) rollingUpgradeCtx {
	return rollingUpgradeCtx{
		client:             d.Client,
		ES:                 d.ES,
		statefulSets:       statefulSets,
		esClient:           esClient,
		esState:            esState,
		deleteExpectations: d.DeleteExpectations,
		podUpgradeDone:     podUpgradeDone,
	}
}

func (ctx rollingUpgradeCtx) run() *reconciler.Results {
	results := &reconciler.Results{}

	// TODO: deal with multiple restarts at once, taking the changeBudget into account.
	//  We'd need to stop checking cluster health and do something smarter, since cluster health green check
	//  should be done **in between** restarts to make sense, which is pretty hard to do since we don't
	//  trigger restarts but just allow the sset controller to do it at its own pace.
	//  Instead of green health, we could look at shards status, taking into account nodes
	//  we scheduled for a restart (maybe not restarted yet).

	healthyPods := make(map[types.NamespacedName]*corev1.Pod)
	var toBeDeletedPods []*corev1.Pod
	toUpdate := ctx.statefulSets.ToUpdate()
	for _, statefulSet := range toUpdate {
		// Inspect each pod, starting from the highest ordinal, and decrement the idx to allow
		// pod upgrades to go through, controlled by the StatefulSet controller.
		for idx := sset.GetReplicas(statefulSet) - 1; idx >= 0; idx-- {

			// Do we need to upgrade that pod?
			podName := sset.PodName(statefulSet.Name, idx)
			podRef := types.NamespacedName{Namespace: statefulSet.Namespace, Name: podName}
			// retrieve pod to inspect its revision label
			var pod corev1.Pod
			err := ctx.client.Get(podRef, &pod)
			if err != nil && !errors.IsNotFound(err) {
				return results.WithError(err)
			}
			if errors.IsNotFound(err) {
				// Pod does not exist, continue the loop as the absence will be accounted by the deletion controller
				continue
			}
			alreadyUpgraded, err := ctx.podUpgradeDone(ctx.client, ctx.esState, pod, statefulSet.Status.UpdateRevision)
			if err != nil {
				return results.WithError(err)
			}
			healthyPod, err := podIsHealthy(ctx.client, ctx.esState, pod)
			if err != nil {
				return results.WithError(err)
			}
			if healthyPod {
				healthyPods[podRef] = &pod
			}

			if !alreadyUpgraded {
				toBeDeletedPods = append(toBeDeletedPods, &pod)
			}

			// Is the cluster ready for the node upgrade?
			clusterReady, err := clusterReadyForNodeRestart(ctx.ES, ctx.esState)
			if err != nil {
				return results.WithError(err)
			}
			if !clusterReady {
				// retry later
				return results.WithResult(defaultRequeue)
			}

			log.Info("Preparing cluster for node restart", "namespace", ctx.ES.Namespace, "es_name", ctx.ES.Name)
			if err := prepareClusterForNodeRestart(ctx.esClient, ctx.esState); err != nil {
				return results.WithError(err)
			}
		}
	}
	deletionController := NewDeletionController(
		ctx.client,
		&ctx.ES,
		&ctx.esState,
		healthyPods,
		ctx.deleteExpectations,
	)
	_, err := deletionController.Delete(toBeDeletedPods)
	if err != nil {
		return results.WithError(err)
	}
	return results
}

func prepareClusterForNodeRestart(esClient esclient.Client, esState ESState) error {
	// Disable shard allocations to avoid shards moving around while the node is temporarily down
	shardsAllocationEnabled, err := esState.ShardAllocationsEnabled()
	if err != nil {
		return err
	}
	if shardsAllocationEnabled {
		if err := disableShardsAllocation(esClient); err != nil {
			return err
		}
	}

	// Request a sync flush to optimize indices recovery when the node restarts.
	if err := doSyncFlush(esClient); err != nil {
		return err
	}

	// TODO: halt ML jobs on that node
	return nil
}

// clusterReadyForNodeRestart returns true if the ES cluster allows a node to be restarted
// with minimized downtime and no unexpected data loss.
func clusterReadyForNodeRestart(es v1alpha1.Elasticsearch, esState ESState) (bool, error) {
	// Check the cluster health: only allow node restart if health is green.
	// This would cause downtime if some shards have 0 replicas, but we consider that's on the user.
	// TODO: we could technically still restart a node if the cluster is yellow,
	//  as long as there are other copies of the shards in-sync on other nodes
	// TODO: the fact we rely on a cached health here would prevent more than 1 restart
	//  in a single reconciliation
	green, err := esState.GreenHealth()
	if err != nil {
		return false, err
	}
	if !green {
		log.Info("Skipping node rolling upgrade since cluster is not green", "namespace", es.Namespace, "name", es.Name)
		return false, nil
	}
	return true, nil
}

// podUpgradeDone inspects the given pod and returns true if it was successfully upgraded.
func podUpgradeDone(c k8s.Client, esState ESState, pod corev1.Pod, expectedRevision string) (bool, error) {
	if expectedRevision == "" {
		// no upgrade scheduled for the sset
		return false, nil
	}
	if sset.PodRevision(pod) != expectedRevision {
		// pod revision does not match the sset upgrade revision
		return false, nil
	}
	return true, nil
}

func podIsHealthy(c k8s.Client, esState ESState, pod corev1.Pod) (bool, error) {
	if !pod.DeletionTimestamp.IsZero() {
		return false, nil
	}
	// is the pod ready?
	if !k8s.IsPodReady(pod) {
		return false, nil
	}
	// has the node joined the cluster yet?
	inCluster, err := esState.NodesInCluster([]string{pod.Name})
	if err != nil {
		return false, err
	}
	if !inCluster {
		log.V(1).Info("Node has not joined the cluster yet", "namespace", pod.Namespace, "name", pod.Name)
		return false, err
	}
	return true, nil
}

func disableShardsAllocation(esClient esclient.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), esclient.DefaultReqTimeout)
	defer cancel()
	return esClient.DisableReplicaShardsAllocation(ctx)
}

func doSyncFlush(esClient esclient.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), esclient.DefaultReqTimeout)
	defer cancel()
	return esClient.SyncedFlush(ctx)
}

func (d *defaultDriver) MaybeEnableShardsAllocation(
	esClient esclient.Client,
	esState ESState,
	statefulSets sset.StatefulSetList,
) *reconciler.Results {
	results := &reconciler.Results{}
	alreadyEnabled, err := esState.ShardAllocationsEnabled()
	if err != nil {
		return results.WithError(err)
	}
	if alreadyEnabled {
		return results
	}

	// Make sure all pods scheduled for upgrade have been upgraded.
	scheduledUpgradesDone, err := sset.ScheduledUpgradesDone(d.Client, statefulSets)
	if err != nil {
		return results.WithError(err)
	}
	if !scheduledUpgradesDone {
		log.V(1).Info(
			"Rolling upgrade not over yet, some pods don't have the updated revision, keeping shard allocations disabled",
			"namespace", d.ES.Namespace,
			"es_name", d.ES.Name,
		)
		return results.WithResult(defaultRequeue)
	}

	// Make sure all nodes scheduled for upgrade are back into the cluster.
	nodesInCluster, err := esState.NodesInCluster(statefulSets.PodNames())
	if err != nil {
		return results.WithError(err)
	}
	if !nodesInCluster {
		log.V(1).Info(
			"Some upgraded nodes are not back in the cluster yet, keeping shard allocations disabled",
			"namespace", d.ES.Namespace,
			"es_name", d.ES.Name,
		)
		return results.WithResult(defaultRequeue)
	}

	log.Info("Enabling shards allocation", "namespace", d.ES.Namespace, "es_name", d.ES.Name)
	ctx, cancel := context.WithTimeout(context.Background(), esclient.DefaultReqTimeout)
	defer cancel()
	if err := esClient.EnableShardAllocation(ctx); err != nil {
		return results.WithError(err)
	}

	return results
}
