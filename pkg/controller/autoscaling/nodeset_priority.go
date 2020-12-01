// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"context"
	"sort"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// FairNodesManager helps to distribute nodes among NodeSets whose belong to a same tier.
type FairNodesManager struct {
	ctx               context.Context
	nodeSetsResources NodeSetsResources
}

func (fnm *FairNodesManager) sort() {
	sort.SliceStable(fnm.nodeSetsResources, func(i, j int) bool {
		if fnm.nodeSetsResources[i].Count == fnm.nodeSetsResources[j].Count {
			return strings.Compare(fnm.nodeSetsResources[i].Name, fnm.nodeSetsResources[j].Name) < 0
		}
		return fnm.nodeSetsResources[i].Count < fnm.nodeSetsResources[j].Count
	})
}

func NewFairNodesManager(ctx context.Context, nodeSetsResources NodeSetsResources) FairNodesManager {
	fnm := FairNodesManager{
		ctx:               ctx,
		nodeSetsResources: nodeSetsResources,
	}
	fnm.sort()
	return fnm
}

func (fnm *FairNodesManager) AddNode() {
	// Peak the first element, this is the one with the less nodes
	fnm.nodeSetsResources[0].Count++
	// Ensure the set is sorted
	fnm.sort()
}

func (fnm *FairNodesManager) RemoveNode() {
	nodeSet := fnm.nodeSetsResources[len(fnm.nodeSetsResources)-1]
	if nodeSet.Count == 1 {
		logf.FromContext(fnm.ctx).V(1).Info("Can't scale down a nodeSet to 0", "nodeSet", nodeSet.Name)
		return
	}
	// Peak the last element, this is the one with the more nodes
	nodeSet.Count--
	// Ensure the set is sorted
	fnm.sort()
}
