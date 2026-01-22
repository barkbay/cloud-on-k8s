// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"context"
	"reflect"
	"testing"

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	common "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

const (
	notStateless = false
)

// getESConfigSecretForStatefulSet returns the secret holding the ES configuration for the given StatefulSet.
// This is a test helper function.
func getESConfigSecretForStatefulSet(client k8s.Client, namespace string, ssetName string) (corev1.Secret, error) {
	var secret corev1.Secret
	if err := client.Get(context.Background(), types.NamespacedName{
		Namespace: namespace,
		Name:      ConfigSecretName(ssetName, notStateless),
	}, &secret); err != nil {
		return corev1.Secret{}, err
	}
	return secret, nil
}

// getESConfigContentForStatefulSet retrieves the configuration secret of the given StatefulSet,
// and returns the corresponding CanonicalConfig.
// This is a test helper function.
func getESConfigContentForStatefulSet(client k8s.Client, namespace string, ssetName string) (CanonicalConfig, error) {
	secret, err := getESConfigSecretForStatefulSet(client, namespace, ssetName)
	if err != nil {
		return CanonicalConfig{}, err
	}
	if len(secret.Data) == 0 {
		return CanonicalConfig{}, pkgerrors.Errorf("no configuration found in secret %s", ConfigSecretName(ssetName, notStateless))
	}
	content := secret.Data[ConfigFileName]
	if len(content) == 0 {
		return CanonicalConfig{}, pkgerrors.Errorf("no configuration found in secret %s", ConfigSecretName(ssetName, notStateless))
	}

	cfg, err := common.ParseConfig(content)
	if err != nil {
		return CanonicalConfig{}, err
	}
	return CanonicalConfig{cfg}, nil
}

func TestConfigSecretName(t *testing.T) {
	require.Equal(t, "ssetname-es-config", ConfigSecretName("ssetname", notStateless))
}

func Test_getESConfigContentForStatefulSet(t *testing.T) {
	namespace := "namespace"
	ssetName := "sset"
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigSecretName(ssetName, notStateless),
			Namespace: namespace,
		},
		Data: map[string][]byte{
			ConfigFileName: []byte("a: b\nc: d\n"),
		},
	}
	secretInvalid := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigSecretName(ssetName, notStateless),
			Namespace: namespace,
		},
		Data: map[string][]byte{
			ConfigFileName: []byte("yolo"),
		},
	}
	tests := []struct {
		name      string
		client    k8s.Client
		namespace string
		ssetName  string
		want      CanonicalConfig
		wantErr   bool
	}{
		{
			name:      "valid config exists",
			client:    k8s.NewFakeClient(&secret),
			namespace: namespace,
			ssetName:  ssetName,
			want:      CanonicalConfig{common.MustCanonicalConfig(map[string]string{"a": "b", "c": "d"})},
			wantErr:   false,
		},
		{
			name:      "config does not exist",
			client:    k8s.NewFakeClient(),
			namespace: namespace,
			ssetName:  ssetName,
			want:      CanonicalConfig{},
			wantErr:   true,
		},
		{
			name:      "stored config is invalid",
			client:    k8s.NewFakeClient(&secretInvalid),
			namespace: namespace,
			ssetName:  ssetName,
			want:      CanonicalConfig{},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getESConfigContentForStatefulSet(tt.client, tt.namespace, tt.ssetName)
			if (err != nil) != tt.wantErr {
				t.Errorf("getESConfigContentForStatefulSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getESConfigContentForStatefulSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileConfig(t *testing.T) {
	es := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "cluster",
		},
	}
	ssetName := "sset"
	config := CanonicalConfig{common.MustCanonicalConfig(map[string]string{"a": "b", "c": "d"})}
	rendered, err := config.Render()
	require.NoError(t, err)
	configSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: es.Namespace,
			Name:      ConfigSecretName(ssetName, notStateless),
			Labels: map[string]string{
				label.ClusterNameLabelName:     es.Name,
				label.StatefulSetNameLabelName: ssetName,
			},
		},
		Data: map[string][]byte{
			ConfigFileName: rendered,
		},
	}
	tests := []struct {
		name     string
		client   k8s.Client
		es       esv1.Elasticsearch
		ssetName string
		config   CanonicalConfig
		wantErr  bool
	}{
		{
			name:     "config does not exist",
			client:   k8s.NewFakeClient(),
			es:       es,
			ssetName: ssetName,
			config:   config,
			wantErr:  false,
		},
		{
			name:     "config already exists",
			client:   k8s.NewFakeClient(&configSecret),
			es:       es,
			ssetName: ssetName,
			config:   config,
			wantErr:  false,
		},
		{
			name:     "config should be updated",
			client:   k8s.NewFakeClient(&configSecret),
			es:       es,
			ssetName: ssetName,
			config:   CanonicalConfig{common.MustCanonicalConfig(map[string]string{"a": "b", "c": "different"})},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ReconcileConfig(context.Background(), tt.client, &tt.es, tt.ssetName, tt.config, metadata.Metadata{}); (err != nil) != tt.wantErr {
				t.Errorf("ReconcileConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			// config in the apiserver should be the expected one
			parsed, err := getESConfigContentForStatefulSet(tt.client, tt.es.Namespace, tt.ssetName)
			require.NoError(t, err)
			require.Equal(t, tt.config, parsed)
		})
	}
}
