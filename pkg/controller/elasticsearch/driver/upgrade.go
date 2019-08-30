// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"context"

	"github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

func (d *defaultDriver) handleRollingUpgrades(
	esClient esclient.Client,
	statefulSets sset.StatefulSetList,
	masterNodesNames []string,
) *reconciler.Results {
	results := &reconciler.Results{}

	// We need an up-to-date ES state, but avoid requesting information we may not need.
	esState := NewMemoizingESState(esClient)

	// Step 1: get all the pods
	podsStatus, err := getPodsStatus(d.Client, d.ES, statefulSets, esState)
	if err != nil {
		return results.WithError(err)
	}

	// Step 2: check if we have all the Pods
	if len(podsStatus.missing) > 0 {
		log.V(1).Info("cannot upgrade cluster because of missing Pods",
			"es_name", d.ES.Name,
			"es_namespace", d.ES.Namespace,
			"missing_pods", podsStatus.missing,
		)
	}

	// Step 3: check expectations
	expectationsCleared, err := d.DeleteExpectations.MayBeClearExpectations(
		k8s.ExtractNamespacedName(&d.ES),
		&podCheck{
			Client:        d.Client,
			Elasticsearch: &d.ES,
		},
	)
	if err != nil {
		return results.WithError(err)
	}
	if !expectationsCleared {
		log.Info("expectation not cleared", "es_name", d.ES.Name, "es_namespace", d.ES.Namespace)
		return results.WithResult(defaultRequeue)
	}
	log.Info("expectation cleared", "es_name", d.ES.Name, "es_namespace", d.ES.Namespace)

	// Maybe upgrade some of the nodes.
	deletedPods, err := newRollingUpgrade(d, esClient, esState, *podsStatus, statefulSets, masterNodesNames).run()
	if err != nil {
		return results.WithError(err)
	}
	if len(deletedPods) > 0 {
		// Some Pods have just been deleted, keep shards allocation disabled
		return results.WithResult(defaultRequeue)
	}
	if len(podsStatus.toUpdate) > len(deletedPods) {
		// Some Pods have not been update, ensure that we retry later
		results.WithResult(defaultRequeue)
	}

	// Maybe re-enable shards allocation if upgraded nodes are back into the cluster.
	return results.WithResults(d.MaybeEnableShardsAllocation(esClient, esState, statefulSets))
}

type rollingUpgradeCtx struct {
	client             k8s.Client
	ES                 v1alpha1.Elasticsearch
	statefulSets       sset.StatefulSetList
	esClient           esclient.Client
	esState            ESState
	podsStatus         PodsStatus
	deleteExpectations *DeleteExpectations
	masterNodesNames   []string
	podUpgradeDone     func(pod corev1.Pod, expectedRevision string) (bool, error)
}

func newRollingUpgrade(
	d *defaultDriver,
	esClient esclient.Client,
	esState ESState,
	podsStatus PodsStatus,
	statefulSets sset.StatefulSetList,
	masterNodesNames []string,
) rollingUpgradeCtx {
	return rollingUpgradeCtx{
		client:             d.Client,
		ES:                 d.ES,
		statefulSets:       statefulSets,
		esClient:           esClient,
		esState:            esState,
		podsStatus:         podsStatus,
		podUpgradeDone:     podUpgradeDone,
		deleteExpectations: d.DeleteExpectations,
		masterNodesNames:   masterNodesNames,
	}
}

func (ctx rollingUpgradeCtx) run() ([]corev1.Pod, error) {

	// TODO: deal with multiple restarts at once, taking the changeBudget into account.
	//  We'd need to stop checking cluster health and do something smarter, since cluster health green check
	//  should be done **in between** restarts to make sense, which is pretty hard to do since we don't
	//  trigger restarts but just allow the sset controller to do it at its own pace.
	//  Instead of green health, we could look at shards status, taking into account nodes
	//  we scheduled for a restart (maybe not restarted yet).

	deletionDriver := NewDeletionDriver(
		ctx.client,
		ctx.esClient,
		&ctx.ES,
		ctx.statefulSets,
		ctx.esState,
		ctx.masterNodesNames,
		ctx.podsStatus,
		ctx.deleteExpectations,
	)
	deletedPods, err := deletionDriver.Delete(ctx.podsStatus.toUpdate)
	if errors.IsConflict(err) || errors.IsNotFound(err) {
		// Cache is not up to date or Pod has been deleted by someone else
		// (could be the statefulset controller)
		// TODO: should we at least log this one in debug mode ?
		return deletedPods, nil
	}
	if err != nil {
		return deletedPods, err
	}
	return deletedPods, nil
}

// TODO: move this to sset package
// PodsByName is a map of Pods
type PodsByName map[string]corev1.Pod

type PodsStatus struct {
	currents PodsByName
	healthy  PodsByName
	toUpdate []corev1.Pod
	missing  []string
}

func getPodsStatus(
	client k8s.Client,
	ES v1alpha1.Elasticsearch,
	statefulSets sset.StatefulSetList,
	esState ESState,
) (*PodsStatus, error) {
	// 1. Get all current Pods for this cluster
	currents, err := sset.GetActualPodsForCluster(client, ES)
	if err != nil {
		return nil, err
	}
	// Create a map to lookup missing ones
	currentsByName := make(PodsByName, len(currents))
	for _, pod := range currents {
		currentsByName[pod.Name] = pod
	}

	// 2. Get missing Pods
	expectedPods := statefulSets.PodNames()
	missingPods := []string{}
	for _, expectedPod := range expectedPods {
		if _, found := currentsByName[expectedPod]; !found {
			missingPods = append(missingPods, expectedPod)
		}
	}

	// 2. Get the healthy ones
	healthyPods := make(PodsByName, len(currents))
	for _, pod := range currents {
		healthyPod, err := podIsHealthy(esState, pod)
		if err != nil {
			return nil, err
		}
		if healthyPod {
			healthyPods[pod.Name] = pod
		}
	}
	// 3. Get the ones that need an update
	toUpdate, err := podsToBeUpdate(client, statefulSets)
	if err != nil {
		return nil, err
	}
	return &PodsStatus{
		currents: currentsByName,
		toUpdate: toUpdate,
		missing:  missingPods,
		healthy:  healthyPods,
	}, nil
}

func podsToBeUpdate(
	client k8s.Client,
	statefulSets sset.StatefulSetList,
) ([]corev1.Pod, error) {
	var toBeDeletedPods []corev1.Pod
	toUpdate := statefulSets.ToUpdate()
	for _, statefulSet := range toUpdate {
		// Inspect each pod, starting from the highest ordinal, and decrement the idx to allow
		// pod upgrades to go through, controlled by the StatefulSet controller.
		for idx := sset.GetReplicas(statefulSet) - 1; idx >= 0; idx-- {

			// Do we need to upgrade that pod?
			podName := sset.PodName(statefulSet.Name, idx)
			podRef := types.NamespacedName{Namespace: statefulSet.Namespace, Name: podName}
			// retrieve pod to inspect its revision label
			var pod corev1.Pod
			err := client.Get(podRef, &pod)
			if err != nil && !errors.IsNotFound(err) {
				return toBeDeletedPods, err
			}
			if errors.IsNotFound(err) {
				// Pod does not exist, continue the loop as the absence will be accounted by the deletion driver
				continue
			}
			alreadyUpgraded, err := podUpgradeDone(pod, statefulSet.Status.UpdateRevision)
			if err != nil {
				return toBeDeletedPods, err
			}

			if !alreadyUpgraded {
				toBeDeletedPods = append(toBeDeletedPods, pod)
			}
		}
	}
	return toBeDeletedPods, nil
}

func podIsHealthy(esState ESState, pod corev1.Pod) (bool, error) {
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

	// Make sure all nodes are back into the cluster.
	// We can't specifically check which ones have been updated.
	nodesInCluster, err := esState.NodesInCluster(statefulSets.PodNames())
	if err != nil {
		return results.WithError(err)
	}

	// TODO: Do we need a timeout here ? If some node(s) never come back the cluster will stay yellow.
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

// podUpgradeDone inspects the given pod and returns true if it was successfully upgraded.
func podUpgradeDone(pod corev1.Pod, expectedRevision string) (bool, error) {
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
