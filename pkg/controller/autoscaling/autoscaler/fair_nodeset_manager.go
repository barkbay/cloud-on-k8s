// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/resources"
	"github.com/go-logr/logr"
)

// FairNodesManager helps to distribute nodes among several NodeSets whose belong to a same tier.
type FairNodesManager struct {
	log                  logr.Logger
	nodeSetNodeCountList resources.NodeSetNodeCountList
}

// sort sorts NodeSets by the value of the Count field, giving priority to NodeSets with less nodes.
// If several NodeSets have the same number of nodes they are sorted alphabetically.
func (fnm *FairNodesManager) sort() {
	sort.SliceStable(fnm.nodeSetNodeCountList, func(i, j int) bool {
		if fnm.nodeSetNodeCountList[i].NodeCount == fnm.nodeSetNodeCountList[j].NodeCount {
			return strings.Compare(fnm.nodeSetNodeCountList[i].Name, fnm.nodeSetNodeCountList[j].Name) < 0
		}
		return fnm.nodeSetNodeCountList[i].NodeCount < fnm.nodeSetNodeCountList[j].NodeCount
	})
}

func NewFairNodesManager(log logr.Logger, nodeSetNodeCount []resources.NodeSetNodeCount) FairNodesManager {
	fnm := FairNodesManager{
		log:                  log,
		nodeSetNodeCountList: nodeSetNodeCount,
	}
	fnm.sort()
	return fnm
}

// AddNode selects the nodeSet with the highest priority and increases by one the value its Count field.
// Priority is defined as the nodeSet with the lowest Count value or the first nodeSet in the alphabetical order if
// several NodeSets have the same Count value.
func (fnm *FairNodesManager) AddNode() {
	// Peak the first element, this is the one with the less nodes
	fnm.nodeSetNodeCountList[0].NodeCount++
	// Ensure the set is sorted
	fnm.sort()
}