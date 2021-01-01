// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"testing"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/stretchr/testify/assert"
)

func TestFairNodesManager_AddNode(t *testing.T) {
	type fields struct {
		nodeSetsResources nodesets.NodeSetsResources
	}
	tests := []struct {
		name       string
		fields     fields
		assertFunc func(t *testing.T, fnm FairNodesManager)
	}{
		{
			name: "One nodeSet",
			fields: fields{
				nodeSetsResources: []nodesets.NodeSetResources{{Name: "nodeset-1"}},
			},
			assertFunc: func(t *testing.T, fnm FairNodesManager) {
				assert.Equal(t, 1, len(fnm.nodeSetsResources))
				assert.Equal(t, int32(0), fnm.nodeSetsResources[0].Count)
				fnm.AddNode()
				assert.Equal(t, int32(1), fnm.nodeSetsResources[0].Count)
				fnm.AddNode()
				assert.Equal(t, int32(2), fnm.nodeSetsResources[0].Count)
			},
		},
		{
			name: "Several nodeSets",
			fields: fields{
				nodeSetsResources: []nodesets.NodeSetResources{{Name: "nodeset-1"}, {Name: "nodeset-2"}},
			},
			assertFunc: func(t *testing.T, fnm FairNodesManager) {
				assert.Equal(t, 2, len(fnm.nodeSetsResources))
				assert.Equal(t, int32(0), fnm.nodeSetsResources.ByNodeSet()["nodeset-1"].Count)
				assert.Equal(t, int32(0), fnm.nodeSetsResources.ByNodeSet()["nodeset-2"].Count)

				fnm.AddNode()
				assert.Equal(t, int32(1), fnm.nodeSetsResources.ByNodeSet()["nodeset-1"].Count)
				assert.Equal(t, int32(0), fnm.nodeSetsResources.ByNodeSet()["nodeset-2"].Count)

				fnm.AddNode()
				assert.Equal(t, int32(1), fnm.nodeSetsResources.ByNodeSet()["nodeset-1"].Count)
				assert.Equal(t, int32(1), fnm.nodeSetsResources.ByNodeSet()["nodeset-2"].Count)

				fnm.AddNode()
				assert.Equal(t, int32(2), fnm.nodeSetsResources.ByNodeSet()["nodeset-1"].Count)
				assert.Equal(t, int32(1), fnm.nodeSetsResources.ByNodeSet()["nodeset-2"].Count)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fnm := NewFairNodesManager(logTest, tt.fields.nodeSetsResources)
			tt.assertFunc(t, fnm)
		})
	}
}
