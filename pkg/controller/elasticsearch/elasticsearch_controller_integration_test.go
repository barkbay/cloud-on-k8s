// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.
package elasticsearch

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"testing"
	"time"

	commonv1 "github.com/elastic/cloud-on-k8s/v2/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v2/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v2/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/v2/pkg/utils/retry"
	"github.com/elastic/cloud-on-k8s/v2/pkg/utils/test"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestMain(m *testing.M) {
	test.RunWithK8s(m)
}

const (
	operatorNs             = "elastic-system"
	elasticsearchResources = 20
)

// go test -v github.com/elastic/cloud-on-k8s/v2/pkg/controller/elasticsearch -run '^TestReconcile$' -race -count=1
func TestReconcile(t *testing.T) {
	k8sClient, stop := test.StartManager(t,
		Add,
		operator.Parameters{
			OperatorNamespace:       operatorNs,
			MaxConcurrentReconciles: 20,
		},
	)
	defer stop()

	require.NoError(t, test.EnsureNamespace(k8sClient, operatorNs))

	require.NoError(t, createElasticsearchResources(k8sClient))

	retry.UntilSuccess(
		func() error {
			t.Helper()
			// Try to get the Pods
			ssets := &appsv1.StatefulSetList{}
			if err := k8sClient.List(context.TODO(), ssets); err != nil {
				return err
			}
			expected := elasticsearchResources
			if len(ssets.Items) != expected {
				msg := fmt.Sprintf("expected %d StatefulSets, got %d", expected, len(ssets.Items))
				return errors.Errorf(msg)
			}
			return nil
		},
		time.Second*60, time.Second*1)
}

func createElasticsearchResources(k8sClient client.Client) error {
	for i := 1; i <= elasticsearchResources; i++ {
		es := &esv1.Elasticsearch{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("es-%d", i),
				Namespace: "default",
			},
			Spec: esv1.ElasticsearchSpec{
				Version: "8.0.0",
				NodeSets: []esv1.NodeSet{
					{
						Name:   "default",
						Config: &commonv1.Config{Data: map[string]interface{}{}},
						Count:  10,
					},
				},
			},
		}
		if err := k8sClient.Create(context.TODO(), es); err != nil {
			return err
		}
		// Built-in controllers are not running, create the service endpoint manually.
		endpoints := &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("es-%d-es-internal-http", i),
				Namespace: "default",
			},
		}
		if err := k8sClient.Create(context.TODO(), endpoints); err != nil {
			return err
		}
	}

	return nil
}
