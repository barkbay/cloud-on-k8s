// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"reflect"
	"testing"

	"github.com/elastic/cloud-on-k8s/operators/pkg/controller/elasticsearch/nodespec"
	"github.com/elastic/cloud-on-k8s/operators/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/operators/pkg/utils/k8s"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/elastic/cloud-on-k8s/operators/pkg/apis/elasticsearch/v1alpha1"
	"github.com/elastic/cloud-on-k8s/operators/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/operators/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/operators/pkg/controller/elasticsearch/observer"
	"github.com/elastic/cloud-on-k8s/operators/pkg/controller/elasticsearch/reconcile"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Sample StatefulSets to use in tests
var (
	es6SsetMaster3Replicas     = nodespec.TestSset{Name: "ssetMaster3Replicas", Version: "6.8.1", Replicas: 3, Master: true, Data: false}.Build()
	es6podsSsetMaster3Replicas = []corev1.Pod{
		nodespec.TestPod{
			Namespace:   ssetMaster3Replicas.Namespace,
			Name:        sset.PodName(ssetMaster3Replicas.Name, 0),
			ClusterName: clusterName,
			Version:     "6.8.1",
			Master:      true,
		}.Build(),
		nodespec.TestPod{
			Namespace:   ssetMaster3Replicas.Namespace,
			Name:        sset.PodName(ssetMaster3Replicas.Name, 1),
			ClusterName: clusterName,
			Version:     "6.8.1",
			Master:      true,
		}.Build(),
		nodespec.TestPod{
			Namespace:   ssetMaster3Replicas.Namespace,
			Name:        sset.PodName(ssetMaster3Replicas.Name, 2),
			ClusterName: clusterName,
			Version:     "6.8.1",
			Master:      true,
		}.Build(),
	}

	es6runtimeObjs = []runtime.Object{&ssetMaster3Replicas, &ssetData4Replicas,
		&podsSsetMaster3Replicas[0], &podsSsetMaster3Replicas[1], &podsSsetMaster3Replicas[2],
		&podsSsetData4Replicas[0], &podsSsetData4Replicas[1], &podsSsetData4Replicas[2], &podsSsetData4Replicas[3],
	}
)

func Test_defaultDriver_reconcileNodeSpecs(t *testing.T) {

	k8sClient := k8s.WrapClient(fake.NewFakeClient(es6runtimeObjs...))
	esClient := &fakeESClient{}

	type fields struct {
		DefaultDriverParameters DefaultDriverParameters
	}
	type args struct {
		esReachable       bool
		esClient          *fakeESClient
		reconcileState    *reconcile.State
		observedState     observer.State
		resourcesState    reconcile.ResourcesState
		keystoreResources *keystore.Resources
	}
	tests := []struct {
		name                         string
		fields                       fields
		args                         args
		want                         *reconciler.Results
		wantMinimumMasterNodesCalled int
	}{
		{
			fields: fields{DefaultDriverParameters: DefaultDriverParameters{
				ES:     v1alpha1.Elasticsearch{},
				Client: k8sClient,
			}},
			args: args{
				esReachable:    true,
				esClient:       esClient,
				reconcileState: &reconcile.State{},
				observedState:  observer.State{},
				resourcesState: reconcile.ResourcesState{},
			},
			wantMinimumMasterNodesCalled: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &defaultDriver{
				DefaultDriverParameters: tt.fields.DefaultDriverParameters,
			}
			if got := d.reconcileNodeSpecs(tt.args.esReachable, tt.args.esClient, tt.args.reconcileState, tt.args.observedState, tt.args.resourcesState, tt.args.keystoreResources); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("defaultDriver.reconcileNodeSpecs() = %v, want %v", got, tt.want)
			}
			require.Equal(t, tt.wantMinimumMasterNodesCalled, tt.args.esClient.SetMinimumMasterNodesCalled)
		})
	}
}
