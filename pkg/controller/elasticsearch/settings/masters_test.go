// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"context"
	"testing"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// newPodWithIP creates a new Pod potentially labeled as master with a given podIP
func newPodWithIP(name, ip string, master bool) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: make(map[string]string),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{}},
		},
		Status: corev1.PodStatus{
			PodIP: ip,
		},
	}
	label.NodeTypesMasterLabelName.Set(master, p.Labels)
	return p
}

// newStatelessPod creates a stateless-style pod (tier-labeled, no master
// role label) with the given tier.
func newStatelessPod(name, ip string, tier esv1.StatelessTier) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{label.TierLabelName: string(tier)},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{}}},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

func TestIsMasterEligibleStatelessPod(t *testing.T) {
	indexOnly := esv1.Elasticsearch{
		Spec: esv1.ElasticsearchSpec{
			Mode: esv1.ElasticsearchModeStateless,
			ObjectStore: &esv1.ObjectStoreConfig{
				Type:   esv1.ObjectStoreTypeS3,
				Bucket: "b",
			},
			NodeSets: []esv1.NodeSet{
				{Name: "index-a", Count: 3, Tier: esv1.IndexTier},
				{Name: "search-a", Count: 2, Tier: esv1.SearchTier},
			},
		},
	}
	withMasters := indexOnly.DeepCopy()
	withMasters.Spec.NodeSets = append(
		[]esv1.NodeSet{{Name: "master-a", Count: 3, Tier: esv1.MasterTier}},
		withMasters.Spec.NodeSets...,
	)

	tests := []struct {
		name string
		es   esv1.Elasticsearch
		pod  corev1.Pod
		want bool
	}{
		{
			name: "index-only: index pod is master-eligible",
			es:   indexOnly,
			pod:  newStatelessPod("i", "10.0.0.1", esv1.IndexTier),
			want: true,
		},
		{
			name: "index-only: search pod is not",
			es:   indexOnly,
			pod:  newStatelessPod("s", "10.0.0.2", esv1.SearchTier),
			want: false,
		},
		{
			name: "with masters: master-tier pod is master-eligible",
			es:   *withMasters,
			pod:  newStatelessPod("m", "10.0.0.3", esv1.MasterTier),
			want: true,
		},
		{
			name: "with masters: index pod is NOT master-eligible",
			es:   *withMasters,
			pod:  newStatelessPod("i", "10.0.0.1", esv1.IndexTier),
			want: false,
		},
		{
			name: "pod without tier label is not master-eligible",
			es:   indexOnly,
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Labels: map[string]string{"foo": "bar"}},
			},
			want: false,
		},
		{
			name: "pod with nil labels is not master-eligible",
			es:   indexOnly,
			pod:  corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsMasterEligibleStatelessPod(tt.es, tt.pod))
		})
	}
}

// TestUpdateSeedHostsConfigMap_Stateless verifies the seed-hosts ConfigMap is
// built from the right set of pods in stateless mode: index-tier pods when no
// dedicated master tier exists, master-tier pods only when one does.
func TestUpdateSeedHostsConfigMap_Stateless(t *testing.T) {
	esStatelessIndexOnly := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{Name: "es1", Namespace: "ns1"},
		Spec: esv1.ElasticsearchSpec{
			Mode: esv1.ElasticsearchModeStateless,
			ObjectStore: &esv1.ObjectStoreConfig{
				Type: esv1.ObjectStoreTypeS3, Bucket: "b",
			},
			NodeSets: []esv1.NodeSet{
				{Name: "index-a", Count: 3, Tier: esv1.IndexTier},
				{Name: "search-a", Count: 2, Tier: esv1.SearchTier},
			},
		},
	}
	esStatelessWithMasters := *esStatelessIndexOnly.DeepCopy()
	esStatelessWithMasters.Spec.NodeSets = append(
		[]esv1.NodeSet{{Name: "master-a", Count: 3, Tier: esv1.MasterTier}},
		esStatelessWithMasters.Spec.NodeSets...,
	)

	tests := []struct {
		name            string
		es              esv1.Elasticsearch
		pods            []corev1.Pod
		expectedContent string
	}{
		{
			name: "index-only: seed hosts are the index-tier pods",
			es:   esStatelessIndexOnly,
			pods: []corev1.Pod{
				newStatelessPod("i1", "10.0.0.1", esv1.IndexTier),
				newStatelessPod("i2", "10.0.0.2", esv1.IndexTier),
				newStatelessPod("s1", "10.0.0.3", esv1.SearchTier),
			},
			expectedContent: "10.0.0.1:9300\n10.0.0.2:9300",
		},
		{
			name: "dedicated masters: only the master-tier pods are seed hosts",
			es:   esStatelessWithMasters,
			pods: []corev1.Pod{
				newStatelessPod("m1", "10.0.0.10", esv1.MasterTier),
				newStatelessPod("m2", "10.0.0.11", esv1.MasterTier),
				newStatelessPod("i1", "10.0.0.1", esv1.IndexTier),
				newStatelessPod("s1", "10.0.0.3", esv1.SearchTier),
			},
			expectedContent: "10.0.0.10:9300\n10.0.0.11:9300",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := k8s.NewFakeClient()
			require.NoError(t, UpdateSeedHostsConfigMap(context.Background(), c, tt.es, tt.pods, metadata.Metadata{}))

			cm := &corev1.ConfigMap{}
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{
				Namespace: tt.es.Namespace, Name: esv1.UnicastHostsConfigMap(tt.es.Name),
			}, cm))
			assert.Equal(t, tt.expectedContent, cm.Data[volume.UnicastHostsFile])
		})
	}
}

func TestUpdateSeedHostsConfigMap(t *testing.T) {
	es := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "es1",
			Namespace: "ns1",
		},
	}
	type args struct {
		c    k8s.Client
		es   esv1.Elasticsearch
		pods []corev1.Pod
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		expectedContent string
	}{
		{
			name: "Do not fail if no master has an IP",
			args: args{
				pods: []corev1.Pod{
					newPodWithIP("master1", "", true),
					newPodWithIP("master2", "", true),
					newPodWithIP("master3", "", true),
					newPodWithIP("node1", "", false),
					newPodWithIP("node2", "10.0.2.8", false),
				},
				c:  k8s.NewFakeClient(),
				es: es,
			},
			wantErr:         false,
			expectedContent: "",
		},
		{
			name: "Do not fail if there's no master at all",
			args: args{
				pods: []corev1.Pod{
					newPodWithIP("node1", "", false),
					newPodWithIP("node2", "10.0.2.8", false),
				},
				c:  k8s.NewFakeClient(),
				es: es,
			},
			wantErr:         false,
			expectedContent: "",
		},
		{
			name: "One of the master doesn't have an IP",
			args: args{
				pods: []corev1.Pod{ //
					newPodWithIP("master1", "10.0.9.2", true),
					newPodWithIP("master2", "", true),
					newPodWithIP("master3", "10.0.3.3", true),
					newPodWithIP("node1", "10.0.9.3", false),
					newPodWithIP("node2", "10.0.2.8", false),
				},
				c:  k8s.NewFakeClient(),
				es: es,
			},
			wantErr:         false,
			expectedContent: "10.0.3.3:9300\n10.0.9.2:9300",
		},
		{
			name: "All masters have IPs, some nodes don't",
			args: args{
				pods: []corev1.Pod{ //
					newPodWithIP("master1", "10.0.9.2", true),
					newPodWithIP("master2", "10.0.6.5", true),
					newPodWithIP("master3", "10.0.3.3", true),
					newPodWithIP("node1", "", false),
					newPodWithIP("node2", "10.0.2.8", false),
				},
				c:  k8s.NewFakeClient(),
				es: es,
			},
			wantErr:         false,
			expectedContent: "10.0.3.3:9300\n10.0.6.5:9300\n10.0.9.2:9300",
		},
		{
			name: "Ordering of pods should not matter",
			args: args{
				pods: []corev1.Pod{ //
					newPodWithIP("master2", "10.0.6.5", true),
					newPodWithIP("master3", "10.0.3.3", true),
					newPodWithIP("master1", "10.0.9.2", true),
				},
				c:  k8s.NewFakeClient(),
				es: es,
			},
			wantErr:         false,
			expectedContent: "10.0.3.3:9300\n10.0.6.5:9300\n10.0.9.2:9300",
		},
		{
			name: "Can handle IPv6 addresses",
			args: args{
				pods: []corev1.Pod{ //
					newPodWithIP("master2", "fd00:10:244:0:2::3", true),
					newPodWithIP("master3", "fd00:10:244:0:2::5", true),
					newPodWithIP("master1", "fd00:10:244:0:2::2", true),
				},
				c:  k8s.NewFakeClient(),
				es: es,
			},
			wantErr:         false,
			expectedContent: "[fd00:10:244:0:2::2]:9300\n[fd00:10:244:0:2::3]:9300\n[fd00:10:244:0:2::5]:9300",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := UpdateSeedHostsConfigMap(context.Background(), tt.args.c, tt.args.es, tt.args.pods, metadata.Metadata{})
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateSeedHostsConfigMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check the resulting confimap
			file := &corev1.ConfigMap{}
			if err := tt.args.c.Get(context.Background(),
				types.NamespacedName{
					Namespace: "ns1",
					Name:      esv1.UnicastHostsConfigMap(es.Name),
				}, file); err != nil {
				t.Errorf("Error while getting the seed hosts configmap: %v", err)
			}
			assert.Equal(t, len(file.Data), 1)
			assert.Equal(t, tt.expectedContent, file.Data[volume.UnicastHostsFile])
		})
	}
}
