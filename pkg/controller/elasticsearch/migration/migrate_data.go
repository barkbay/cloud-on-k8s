// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package migration

import (
	"context"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/observer"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
)

var log = logf.Log.WithName("migration")

func shardIsMigrating(toMigrate client.Shard, others []client.Shard) bool {
	log.Info("check if shard is migrating",
		"state", toMigrate.State,
		"node", toMigrate.Node,
		"shard", toMigrate.Shard,
	)
	if toMigrate.IsRelocating() || toMigrate.IsInitializing() {
		log.Info("Shard is migrating (relocating or initializing")
		return true // being migrated away or weirdly just initializing
	}
	if !toMigrate.IsStarted() {
		log.Info("Shard not migrating because not started")
		return false // early return as we are interested only in started shards for migration purposes
	}
	for _, otherCopy := range others {
		if otherCopy.IsStarted() {
			log.Info("Shard not migrating because other copy is started",
				"other_state", otherCopy.State,
				"other_shard", otherCopy.Shard,
				"other_node", otherCopy.Node)
			return false // found another shard copy
		}
	}
	log.Info("Shard is migrating (relocating or initializing")
	return true // we assume other copies are initializing or there are no other copies
}

// nodeIsMigratingData is the core of IsMigratingData just with any I/O
// removed to facilitate testing. See IsMigratingData for a high-level description.
func nodeIsMigratingData(nodeName string, shards []client.Shard, exclusions map[string]struct{}) bool {
	log.Info("nodeIsMigratingData", "pod", nodeName, "shards", shards, "exclusions", exclusions)
	// all other shards not living on the node that is about to go away mapped to their corresponding shard keys
	othersByShard := make(map[string][]client.Shard)
	// all shard copies currently living on the node leaving the cluster
	candidates := make([]client.Shard, 0)

	// divide all shards into the to groups: migration candidate or other shard copy
	for _, shard := range shards {
		_, ignore := exclusions[shard.Node]
		if shard.Node == nodeName {
			candidates = append(candidates, shard)
		} else if !ignore {
			key := shard.Key()
			others, found := othersByShard[key]
			if !found {
				othersByShard[key] = []client.Shard{shard}
			} else {
				othersByShard[key] = append(others, shard)
			}
		}
	}

	// check if there is at least one shard on this node that is migrating or needs to migrate
	for _, toMigrate := range candidates {
		if shardIsMigrating(toMigrate, othersByShard[toMigrate.Key()]) {
			return true
		}
	}
	return false

}

// IsMigratingData looks only at the presence of shards on a given node
// and checks if there is at least one other copy of the shard in the cluster
// that is started and not relocating.
func IsMigratingData(state observer.State, podName string, exclusions []string) bool {
	log.Info("IsMigratingData", "pod", podName, "exclusions", exclusions)
	clusterState := state.ClusterState
	if clusterState == nil || clusterState.IsEmpty() {
		return true // we don't know if the request timed out or the cluster has not formed yet
	}
	for _, shard := range clusterState.GetShards() {
		if shard.Primary && shard.IsUnassigned() {
			log.Info("Unassigned primary shard", "index", shard.Index, "shard", shard.Shard)
			return true // a primary shard is not assigned, data could still live on a node being removed
		}
	}
	excludedNodes := make(map[string]struct{}, len(exclusions))
	for _, name := range exclusions {
		excludedNodes[name] = struct{}{}
	}
	return nodeIsMigratingData(podName, clusterState.GetShards(), excludedNodes)
}

// AllocationSettings captures Elasticsearch API calls around allocation filtering.
type AllocationSettings interface {
	ExcludeFromShardAllocation(context context.Context, nodes string) error
}

// setAllocationExcludes sets allocation filters for the given nodes.
func setAllocationExcludes(asClient AllocationSettings, leavingNodes []string) error {
	exclusions := "none_excluded"
	if len(leavingNodes) > 0 {
		exclusions = strings.Join(leavingNodes, ",")
	}
	// update allocation exclusions
	ctx, cancel := context.WithTimeout(context.Background(), client.DefaultReqTimeout)
	defer cancel()
	return asClient.ExcludeFromShardAllocation(ctx, exclusions)
}

// MigrateData sets allocation filters for the given nodes.
func MigrateData(client AllocationSettings, leavingNodes []string) error {
	return setAllocationExcludes(client, leavingNodes)
}
