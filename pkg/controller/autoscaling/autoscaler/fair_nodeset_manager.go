// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"

	"github.com/go-logr/logr"
)

// FairNodesManager helps to distribute nodes among several NodeSets whose belong to a same tier.
type FairNodesManager struct {
	log               logr.Logger
	nodeSetsResources nodesets.NodeSetsResources
}

// sort sorts nodeSets by the value of the Count field, giving priority to nodeSets with less nodes.
// If several nodeSets have the same number of nodes they are sorted alphabetically.
func (fnm *FairNodesManager) sort() {
	sort.SliceStable(fnm.nodeSetsResources, func(i, j int) bool {
		if fnm.nodeSetsResources[i].Count == fnm.nodeSetsResources[j].Count {
			return strings.Compare(fnm.nodeSetsResources[i].Name, fnm.nodeSetsResources[j].Name) < 0
		}
		return fnm.nodeSetsResources[i].Count < fnm.nodeSetsResources[j].Count
	})
}

func NewFairNodesManager(log logr.Logger, nodeSetsResources nodesets.NodeSetsResources) FairNodesManager {
	fnm := FairNodesManager{
		log:               log,
		nodeSetsResources: nodeSetsResources,
	}
	fnm.sort()
	return fnm
}

// AddNode selects the nodeSet with the highest priority and increases by one the value its Count field.
// Priority is defined as the nodeSet with the lowest Count value or the first nodeSet in the alphabetical order if
// several nodeSets have the same Count value.
func (fnm *FairNodesManager) AddNode() {
	// Peak the first element, this is the one with the less nodes
	fnm.nodeSetsResources[0].Count++
	// Ensure the set is sorted
	fnm.sort()
}
