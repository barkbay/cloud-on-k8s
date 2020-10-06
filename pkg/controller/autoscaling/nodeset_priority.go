// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"sort"
	"strings"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
)

// A PriorityQueue implements heap.Interface and holds Items.
type FairNodesManager struct {
	nodeSets []esv1.NodeSet
}

func (fnm *FairNodesManager) sort() {
	sort.SliceStable(fnm.nodeSets, func(i, j int) bool {
		if fnm.nodeSets[i].Count == fnm.nodeSets[j].Count {
			return strings.Compare(fnm.nodeSets[i].Name, fnm.nodeSets[j].Name) < 0
		}
		return fnm.nodeSets[i].Count < fnm.nodeSets[j].Count
	})
}

func NewFairNodesManager(nodeSets []v1.NodeSet) FairNodesManager {
	fnm := FairNodesManager{nodeSets: nodeSets}
	fnm.sort()
	return fnm
}

func (fnm *FairNodesManager) AddNode() {
	// Peak the first element, this is the one with the highest priority
	fnm.nodeSets[0].Count++
	// Ensure the set is sorted
	fnm.sort()
}

func (fnm *FairNodesManager) RemoveNode() {
	// Peak the last element, this is the one with the highest priority
	fnm.nodeSets[len(fnm.nodeSets)-1].Count--
	// Ensure the set is sorted
	fnm.sort()
}
