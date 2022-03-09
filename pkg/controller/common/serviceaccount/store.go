// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"context"
	"encoding/base64"
	"fmt"

	"go.elastic.co/apm"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/pkg/utils/log"
)

var (
	log = ulog.Log.WithName("serviceaccount")
)

func applicationSecretLabels(es esv1.Elasticsearch) map[string]string {
	return common.AddCredentialsLabel(map[string]string{
		label.ClusterNamespaceLabelName: es.Namespace,
		label.ClusterNameLabelName:      es.Name,
	})
}

func esStoreLabels(es esv1.Elasticsearch) map[string]string {
	return map[string]string{
		label.ClusterNamespaceLabelName: es.Namespace,
		label.ClusterNameLabelName:      es.Name,
		common.TypeLabelName:            "service-account-token",
	}
}

// reconcileApplicationSecret reconciles the Secret which contains the application token.
func reconcileApplicationSecret(
	ctx context.Context,
	client k8s.Client,
	es esv1.Elasticsearch,
	applicationSecretName types.NamespacedName,
	commonLabels map[string]string,
	tokenName string,
	serviceAccount Name,
) (*Token, error) {
	span, _ := apm.StartSpan(ctx, "reconcile_sa_token_application", tracing.SpanTypeApp)
	defer span.End()

	// We first try to read the store in the application namespace.
	applicationStore := corev1.Secret{}
	err := client.Get(context.Background(), applicationSecretName, &applicationStore)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	var token *Token
	if k8serrors.IsNotFound(err) || len(applicationStore.Data) == 0 {
		// Create a new token
		token, err = newApplicationToken(serviceAccount, tokenName)
		if err != nil {
			return nil, err
		}
	} else {
		// Attempt to read current value
		token = getCurrentApplicationToken(&es, applicationSecretName.Name, applicationStore.Data)
		if token == nil {
			// We need to create a new token
			if token, err = newApplicationToken(serviceAccount, tokenName); err != nil {
				return nil, err
			}
		}
	}

	labels := applicationSecretLabels(es)
	for labelName, labelValue := range commonLabels {
		labels[labelName] = labelValue
	}
	applicationStore = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      applicationSecretName.Name,
			Namespace: applicationSecretName.Namespace,
			Labels:    applicationSecretLabels(es),
		},
		Data: map[string][]byte{
			"name":           []byte(token.TokenName),
			"token":          []byte(token.Token),
			"hash":           []byte(token.Hash),
			"serviceAccount": []byte(token.ServiceAccountName),
		},
	}

	if _, err := reconciler.ReconcileSecret(client, applicationStore, nil); err != nil {
		return nil, err
	}

	return token, err
}

// ensureElasticsearchStoreExists ensures the elasticsearch store exists and hold the provided tokens.
func reconcileElasticsearchSecret(
	ctx context.Context,
	client k8s.Client,
	es esv1.Elasticsearch,
	elasticsearchSecretName types.NamespacedName,
	token Token,
) error {
	span, _ := apm.StartSpan(ctx, "reconcile_sa_token_elasticsearch", tracing.SpanTypeApp)
	defer span.End()
	fullyQualifiedName := token.ServiceAccountName + "/" + token.TokenName
	esSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      elasticsearchSecretName.Name,
			Namespace: elasticsearchSecretName.Namespace,
			Labels:    esStoreLabels(es),
		},
		Data: map[string][]byte{
			"name": []byte(fullyQualifiedName),
			"hash": []byte(token.Hash),
		},
	}
	_, err := reconciler.ReconcileSecret(client, esSecret, &es)
	return err
}

func ReconcileSecrets(
	ctx context.Context,
	client k8s.Client,
	es esv1.Elasticsearch,
	commonLabels map[string]string,
	applicationSecretName types.NamespacedName,
	elasticsearchSecretName types.NamespacedName,
	serviceAccount Name,
	applicationName string,
	applicationUID types.UID,
) (*TokenReference, error) {

	tokenName := tokenName(applicationSecretName.Namespace, applicationName, applicationUID)
	var token *Token

	token, err := reconcileApplicationSecret(ctx, client, es, applicationSecretName, commonLabels, tokenName, serviceAccount)
	if err != nil {
		return nil, err
	}

	if err := reconcileElasticsearchSecret(ctx, client, es, elasticsearchSecretName, *token); err != nil {
		return nil, err
	}

	return &TokenReference{
		SecretRef: applicationSecretName,
		TokenName: tokenName,
	}, nil
}

func getCurrentApplicationToken(es *esv1.Elasticsearch, secretName string, secretData map[string][]byte) *Token {
	if len(secretData) == 0 {
		log.V(1).Info("secret is empty", "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	}
	result := &Token{}
	if tokenName, exists := secretData["name"]; !exists {
		log.V(1).Info("name field is missing", "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	} else {
		result.TokenName = string(tokenName)
	}

	if token, exists := secretData["token"]; !exists {
		log.V(1).Info("token field is missing", "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	} else {
		result.Token = SecureString(token)
	}

	if hash, exists := secretData["hash"]; !exists {
		log.V(1).Info("hash field is missing", "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	} else {
		result.Hash = SecureString(hash)
	}

	if serviceAccount, exists := secretData["serviceAccount"]; !exists {
		log.V(1).Info("serviceAccount field is missing", "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	} else {
		result.ServiceAccountName = string(serviceAccount)
	}

	return result
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

func tokenName(
	applicationNamespace, applicationName string,
	applicationUID types.UID,
) string {
	return fmt.Sprintf("%s_%s_%s", applicationNamespace, applicationName, applicationUID)
}
