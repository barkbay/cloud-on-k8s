// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package topology

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

func Test_getTopologyKeys(t *testing.T) {
	type args struct {
		k8s      k8s.Client
		nodeName string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name: "Happy path",
			args: args{
				k8s: k8s.NewFakeClient(
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
							Labels: map[string]string{
								"topology.kubernetes.io/region": "europe-west1",
								"topology.kubernetes.io/zone":   "europe-west1-c",
							},
						},
					},
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
							Labels: map[string]string{
								"topology.kubernetes.io/region": "europe-west1",
								"topology.kubernetes.io/zone":   "europe-west1-a",
							},
						},
					},
					&corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
							Labels: map[string]string{
								"topology.kubernetes.io/region": "europe-west1",
								"topology.kubernetes.io/zone":   "europe-west1-b",
							},
						},
					},
				),
				nodeName: "node1",
			},
			want: map[string]string{
				"topology.kubernetes.io/region":  "europe-west1",
				"topology.kubernetes.io/zone":    "europe-west1-c",
				"topology.kubernetes.io/regions": "europe-west1",
				"topology.kubernetes.io/zones":   "europe-west1-a,europe-west1-b,europe-west1-c",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getTopologyKeys(context.TODO(), tt.args.k8s, tt.args.nodeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTopologyKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTopologyKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}
