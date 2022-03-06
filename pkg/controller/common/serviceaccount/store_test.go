// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"encoding/base64"
	"reflect"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	kbv1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

var (
	existingKibana = kbv1.Kibana{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "kibana-sample",
			Namespace:       "e2e-venus",
			UID:             types.UID("892ff7d8-9cf2-48f0-89bc-5a530e77a930"),
			ResourceVersion: "8819",
			Generation:      2,
		},
		Spec: kbv1.KibanaSpec{
			Count: 1,
			ElasticsearchRef: commonv1.ObjectSelector{
				Name:      "elasticsearch-sample",
				Namespace: "e2e-mercury",
			},
		},
	}

	noKibanaApplicationStore = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "e2e-venus",
			Name:      "e2e-mercury-elasticsearch-sample-service-accounts",
		},
		Data: map[string][]byte{
			"e2e-venus_fleet-server": []byte(`{
			"serviceAccountName": "elastic/fleet-server",
			"tokenName": "e2e-venus-fleet-server-fleet-server",
			"token": "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL2Nsb3VkLWludGVybmFsOnoxajlmNnUyYlZab3FzSkR5aEZ4NVFDTHZ6Z0sxT0h4NHdPS2d4T25KTEU9",
			"hash": "{PBKDF2_STRETCH}10000$0/jnJjtqWqbbWmdxnsLW1x8c6Gkv0UVzCwNNrBL4qv0=$cFHN2D7vUiZuIJr8a2AK2BwC5MXuN61VkkNuSk95svg="
		}`),
		},
	}

	existingApplicationStore = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "e2e-venus",
			Name:      "e2e-mercury-elasticsearch-sample-service-accounts",
		},
		Data: map[string][]byte{
			"e2e-venus_kibana-sample_" + string(existingKibana.UID): []byte(`{
    "serviceAccountName": "elastic/kibana",
    "tokenName": "e2e-venus-kibana-kibana-sample",
    "token": "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL2Nsb3VkLWludGVybmFsOnoxajlmNnUyYlZab3FzSkR5aEZ4NVFDTHZ6Z0sxT0h4NHdPS2d4T25KTEU9",
    "hash": "{PBKDF2_STRETCH}10000$0/jnJjtqWqbbWmdxnsLW1x8c6Gkv0UVzCwNNrBL4qv0=$cFHN2D7vUiZuIJr8a2AK2BwC5MXuN61VkkNuSk95svg="
  }`),
			"e2e-venus_fleet-server": []byte(`{
			"serviceAccountName": "elastic/fleet-server",
			"tokenName": "e2e-venus-fleet-server-fleet-server",
			"token": "AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL2Nsb3VkLWludGVybmFsOnoxajlmNnUyYlZab3FzSkR5aEZ4NVFDTHZ6Z0sxT0h4NHdPS2d4T25KTEU9",
			"hash": "{PBKDF2_STRETCH}10000$0/jnJjtqWqbbWmdxnsLW1x8c6Gkv0UVzCwNNrBL4qv0=$cFHN2D7vUiZuIJr8a2AK2BwC5MXuN61VkkNuSk95svg="
		}`),
		},
	}

	noKibanaElasticsearchStore = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "e2e-mercury",
			Name:      "elasticsearch-sample-service-accounts",
		},
		Data: map[string][]byte{
			"service_tokens": []byte(`elastic/fleet-server/e2e-venus_fleet-server:{PBKDF2_STRETCH}10000$0/jnJjtqWqbbWmdxnsLW1x8c6Gkv0UVzCwNNrBL4qv0=$cFHN2D7vUiZuIJr8a2AK2BwC5MXuN61VkkNuSk95svg=\n`),
		},
	}
)

func Test_store_EnsureTokenExists(t *testing.T) {
	type fields struct {
		client                 k8s.Client
		applicationNamespace   string
		elasticsearchNamespace string
		elasticsearchName      string
	}
	type args struct {
		applicationName string
		applicationUID  types.UID
		serviceAccount  Name
	}
	tests := []struct {
		name                             string
		fields                           fields
		args                             args
		wantTokenReference               TokenReference
		wantElasticsearchServiceAccounts corev1.Secret
		wantApplicationServiceAccounts   corev1.Secret
		wantErr                          bool
	}{
		{
			name: "both secrets do not exist, token does not exist",
			fields: fields{
				client:                 k8s.NewFakeClient(noKibanaApplicationStore.DeepCopy(), noKibanaElasticsearchStore.DeepCopy(), existingKibana.DeepCopy()),
				applicationNamespace:   "e2e-venus",
				elasticsearchNamespace: "e2e-mercury",
				elasticsearchName:      "elasticsearch-sample",
			},
			args: args{
				applicationName: "kibana-sample",
				// Kibana resource UID
				applicationUID: existingKibana.UID,
				serviceAccount: "kibana",
			},
		},
		{
			name: "both secrets do not exist, valid token already exists",
			fields: fields{
				client:                 k8s.NewFakeClient(existingApplicationStore.DeepCopy(), existingKibana.DeepCopy()),
				applicationNamespace:   "e2e-venus",
				elasticsearchNamespace: "e2e-mercury",
				elasticsearchName:      "elasticsearch-sample",
			},
			args: args{
				applicationName: "kibana-sample",
				// Kibana resource UID
				applicationUID: existingKibana.UID,
				serviceAccount: "kibana",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := applicationStore{
				elasticsearchStore: &elasticsearchStore{
					client: tt.fields.client,
					es: esv1.Elasticsearch{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: tt.fields.elasticsearchNamespace,
							Name:      tt.fields.elasticsearchName,
						},
					},
				},
				applicationNamespace: tt.fields.applicationNamespace,
			}

			gotTokenReference, err := s.EnsureTokenExists(tt.args.applicationName, tt.args.applicationUID, tt.args.serviceAccount)
			if (err != nil) != tt.wantErr {
				t.Errorf("store.EnsureTokenExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTokenReference, tt.wantTokenReference) {
				t.Errorf("store.EnsureTokenExists() = %v, want %v", gotTokenReference, tt.wantTokenReference)
			}
		})
	}
}

func Test_newApplicationToken(t *testing.T) {
	type args struct {
		serviceAccountName Name
		tokenName          string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Simple test with Key validation",
			args: args{
				serviceAccountName: Kibana,
				tokenName:          "e2e-venus-kibana-kibana-sample",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newApplicationToken(tt.args.serviceAccountName, tt.args.tokenName)
			if (err != nil) != tt.wantErr {
				t.Errorf("newApplicationToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			verifyToken(t, got.Token.Clear(), got.Hash.Clear(), string(tt.args.serviceAccountName), tt.args.tokenName)
		})
	}
}

var (
	secretRegEx     = regexp.MustCompile(`^([-\w]+)\/([-\w]+)\/([-\w]+)\:(\w+)$`)
	pbkdf2HashRegEx = regexp.MustCompile(`^\{PBKDF2_STRETCH\}10000\$(.*)\$(.*)$`)
)

func verifyToken(t *testing.T, b64token, hash, serviceAccount, tokenName string) {
	t.Helper()

	// Validate the hash
	sub := pbkdf2HashRegEx.FindStringSubmatch(hash)
	assert.Equal(t, 3, len(sub), "PBKDF2 hash does not match regexp %s", pbkdf2HashRegEx)
	salt, err := base64.StdEncoding.DecodeString(sub[1])
	assert.NoError(t, err, "Unexpected error while decoding salt")
	assert.Equal(t, len(salt), pbkdf2DefaultSaltLength)
	hashString, err := base64.StdEncoding.DecodeString(sub[2])
	assert.NoError(t, err, "Unexpected error while decoding hash string")
	assert.True(t, len(hashString) > 0, "Hash string should not be empty")

	// Validate the token
	token, err := base64.StdEncoding.DecodeString(b64token)
	// Check token prefix
	assert.Equal(t, byte(0x00), token[0])
	assert.Equal(t, byte(0x01), token[1])
	assert.Equal(t, byte(0x00), token[2])
	assert.Equal(t, byte(0x01), token[3])
	// Decode and check token suffix
	secretSuffix := string(token[4:])
	assert.True(t, len(secretSuffix) > 0, "Secret body should not be empty")
	tokenSuffix := secretRegEx.FindStringSubmatch(secretSuffix)
	assert.Equal(t, 5, len(tokenSuffix), "Secret suffix does not match regexp")
	assert.Equal(t, Namespace, tokenSuffix[1])
	assert.Equal(t, serviceAccount, tokenSuffix[2])
	assert.Equal(t, tokenName, tokenSuffix[3])
	clearTextTokenSecret := tokenSuffix[4]

	verify(t, []byte(clearTextTokenSecret), hash)
}
