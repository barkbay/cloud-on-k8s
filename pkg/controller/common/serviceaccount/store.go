// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

var (
	_ ElasticsearchStore = &elasticsearchStore{}
	_ ApplicationStore   = &applicationStore{}
)

type elasticsearchStore struct {
	client k8s.Client
	es     esv1.Elasticsearch
}

type applicationStore struct {
	*elasticsearchStore
	applicationNamespace string
}

func EnsureElasticsearchStore() error {
	return nil
}

func (s *applicationStore) applicationStoreName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: s.applicationNamespace,
		Name:      fmt.Sprintf("%s-%s-service-accounts", s.es.Namespace, s.es.Name),
	}
}

func applicationStoreLabels(es esv1.Elasticsearch) map[string]string {
	return common.AddCredentialsLabel(map[string]string{
		reconciler.SoftOwnerNamespaceLabel: es.Namespace,
		reconciler.SoftOwnerNameLabel:      es.Name,
		common.TypeLabelName:               "sa-tokens",
	})
}

func esStoreLabels(es esv1.Elasticsearch) map[string]string {
	return map[string]string{
		label.ClusterNamespaceLabelName: es.Namespace,
		label.ClusterNameLabelName:      es.Name,
		common.TypeLabelName:            "sa-tokens",
	}
}

// ensureApplicationStoreExists ensure the tokens application store exists and contains the provided token.
func (s *applicationStore) ensureApplicationStoreExists(
	tokenName string,
	serviceAccount Name,
) (*Token, error) {
	// We first try to read the store in the application namespace.
	applicationStore := corev1.Secret{}
	applicationStoreName := s.applicationStoreName()
	err := s.client.Get(context.TODO(), applicationStoreName, &applicationStore)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}

	if errors.IsNotFound(err) {
		// Create a new token
		token, err := newApplicationToken(serviceAccount, tokenName)
		if err != nil {
			return nil, err
		}
		serializedToken, err := token.UnsecureMarshalJSON()
		if err != nil {
			return nil, err
		}
		// Create a new store from scratch
		applicationStore = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      applicationStoreName.Name,
				Namespace: applicationStoreName.Namespace,
				Labels:    applicationStoreLabels(s.es),
			},
			Data: map[string][]byte{
				tokenName: serializedToken,
			},
		}

		if _, err := reconciler.ReconcileSecretWithOperation(s.client, applicationStore, nil, reconciler.Create); err != nil {
			return nil, err
		}

		return token, nil
	}

	// Update the expected value
	token := getCurrentApplicationToken(applicationStore.Data, tokenName)
	if token == nil {
		// We need to create a new token
		if token, err = newApplicationToken(serviceAccount, tokenName); err != nil {
			return nil, err
		}
		json, err := token.UnsecureMarshalJSON()
		if err != nil {
			return nil, err
		}
		applicationStore.Data[tokenName] = json
		applicationStore.Labels = applicationStoreLabels(s.es)
		if _, err := reconciler.ReconcileSecretWithOperation(s.client, applicationStore, nil, reconciler.Update); err != nil {
			return nil, err
		}
	}
	return token, err
}

// EnsureElasticsearchStoreExists ensures the elasticsearch store exists and hold the provided tokens.
func (s *elasticsearchStore) EnsureElasticsearchStoreExists(
	tokens ...Token,
) error {
	// Elasticsearch secure tokens store
	esStore := corev1.Secret{}
	esStoreName := types.NamespacedName{
		Namespace: s.es.Namespace,
		Name:      esv1.ServiceAccountsSecretSecret(s.es.Name),
	}

	err := s.client.Get(context.TODO(), esStoreName, &esStore)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	createEsStore := false
	if errors.IsNotFound(err) {
		createEsStore = true
	}

	var serviceTokens *ServiceTokens

	// Try to load the existing token
	if esStore.Data != nil {
		serviceTokens, err = NewServiceTokens(esStore.Data["service_tokens"])
		if err != nil {
			return err
		}
	}

	for _, token := range tokens {
		// Ensure the token is up-to-date in the Elasticsearch store
		serviceTokens = serviceTokens.Add(token.ServiceAccountName+"/"+token.TokenName, token.Hash)
	}

	if createEsStore {
		esStore = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      esStoreName.Name,
				Namespace: esStoreName.Namespace,
				Labels:    esStoreLabels(s.es),
			},
			Data: map[string][]byte{
				"service_tokens": serviceTokens.ToBytes(),
			},
		}
		_, err := reconciler.ReconcileSecretWithOperation(s.client, esStore, &s.es, reconciler.Create)
		return err
	}

	// Update existing service_tokens key
	esStore.Data["service_tokens"] = serviceTokens.ToBytes()
	esStore.Labels = esStoreLabels(s.es)
	_, err = reconciler.ReconcileSecretWithOperation(s.client, esStore, &s.es, reconciler.Update)
	return err

}

func (s *applicationStore) EnsureTokenExists(
	applicationName string,
	applicationUID types.UID,
	serviceAccount Name,
) (*TokenReference, error) {

	tokenName := tokenName(s.applicationNamespace, applicationName, applicationUID)
	var token *Token

	token, err := s.ensureApplicationStoreExists(tokenName, serviceAccount)
	if err != nil {
		return nil, err
	}

	if err := s.EnsureElasticsearchStoreExists(*token); err != nil {
		return nil, err
	}

	return &TokenReference{
		SecretRef: s.applicationStoreName(),
		TokenName: tokenName,
	}, nil
}

func getCurrentApplicationToken(secretData map[string][]byte, tokenName string) *Token {
	if secretData == nil {
		return nil
	}
	tokenValue, exists := secretData[tokenName]
	if !exists {
		return nil
	}
	token := &Token{}
	if err := json.Unmarshal(tokenValue, &token); err != nil {
		return nil
	}
	return token
}

var prefix = []byte{0x0, 0x1, 0x0, 0x1}

// newApplicationToken generates a new token for the provided service account.
func newApplicationToken(serviceAccountName Name, tokenName string) (*Token, error) {
	secret := common.RandomBytes(64)
	hash, err := hash(secret)
	if err != nil {
		return nil, err
	}

	fullyQualifiedName := fmt.Sprintf("%s/%s", Namespace, serviceAccountName)
	suffix := []byte(fmt.Sprintf("%s/%s:%s", fullyQualifiedName, tokenName, secret))
	token := base64.StdEncoding.EncodeToString(append(prefix, suffix...))

	return &Token{
		ServiceAccountName: fullyQualifiedName,
		TokenName:          tokenName,
		Token:              SecureString(token),
		Hash:               SecureString(*hash),
	}, nil
}

func (s *applicationStore) DeleteToken() error {
	//TODO implement me
	panic("implement me")
}

func NewApplicationStore(client k8s.Client, es esv1.Elasticsearch, applicationNamespace string) ApplicationStore {
	return &applicationStore{
		elasticsearchStore: &elasticsearchStore{
			client: client,
			es:     es,
		},
		applicationNamespace: applicationNamespace,
	}
}

func NewElasticsearchStore(client k8s.Client, es esv1.Elasticsearch) ElasticsearchStore {
	return &elasticsearchStore{
		client: client,
		es:     es,
	}
}

func tokenName(applicationNamespace, applicationName string, applicationUID types.UID) string {
	return fmt.Sprintf("%s_%s_%s", applicationNamespace, applicationName, applicationUID)
}
