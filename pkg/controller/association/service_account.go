// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package association

import (
	"context"
	"fmt"

	"go.elastic.co/apm"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	esuser "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

func applicationSecretLabels(es esv1.Elasticsearch) map[string]string {
	return common.AddCredentialsLabel(map[string]string{
		label.ClusterNamespaceLabelName: es.Namespace,
		label.ClusterNameLabelName:      es.Name,
	})
}

func esSecretsLabels(es esv1.Elasticsearch) map[string]string {
	return map[string]string{
		label.ClusterNamespaceLabelName: es.Namespace,
		label.ClusterNameLabelName:      es.Name,
		common.TypeLabelName:            esuser.ServiceAccountTokenType,
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
	serviceAccount commonv1.ServiceAccountName,
) (*esuser.Token, error) {
	span, _ := apm.StartSpan(ctx, "reconcile_sa_token_application", tracing.SpanTypeApp)
	defer span.End()

	applicationStore := corev1.Secret{}
	err := client.Get(context.Background(), applicationSecretName, &applicationStore)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	var token *esuser.Token
	if k8serrors.IsNotFound(err) || len(applicationStore.Data) == 0 {
		// Secret does not exist or is empty, create a new token
		token, err = esuser.NewApplicationToken(serviceAccount, tokenName)
		if err != nil {
			return nil, err
		}
	} else {
		// Attempt to read current token, create a new one in case of an error.
		token, err = esuser.GetOrCreateToken(&es, applicationSecretName.Name, applicationStore.Data, serviceAccount, tokenName)
		if err != nil {
			return nil, err
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
			Labels:    labels,
		},
		Data: map[string][]byte{
			esuser.ServiceAccountTokenNameField:  []byte(token.TokenName),
			esuser.ServiceAccountTokenValueField: []byte(token.Token),
			esuser.ServiceAccountHashField:       []byte(token.Hash),
			esuser.ServiceAccountNameField:       []byte(token.ServiceAccountName),
		},
	}

	if _, err := reconciler.ReconcileSecret(client, applicationStore, nil); err != nil {
		return nil, err
	}

	return token, err
}

// reconcileElasticsearchSecret ensures the Secret for Elasticsearch exists and hold the expected token.
func reconcileElasticsearchSecret(
	ctx context.Context,
	client k8s.Client,
	es esv1.Elasticsearch,
	elasticsearchSecretName types.NamespacedName,
	commonLabels map[string]string,
	token esuser.Token,
) error {
	span, _ := apm.StartSpan(ctx, "reconcile_sa_token_elasticsearch", tracing.SpanTypeApp)
	defer span.End()
	fullyQualifiedName := token.ServiceAccountName + "/" + token.TokenName
	labels := esSecretsLabels(es)
	for labelName, labelValue := range commonLabels {
		labels[labelName] = labelValue
	}
	esSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      elasticsearchSecretName.Name,
			Namespace: elasticsearchSecretName.Namespace,
			Labels:    labels,
		},
		Data: map[string][]byte{
			esuser.ServiceAccountTokenNameField: []byte(fullyQualifiedName),
			esuser.ServiceAccountHashField:      []byte(token.Hash),
		},
	}
	_, err := reconciler.ReconcileSecret(client, esSecret, &es)
	return err
}

func ReconcileServiceAccounts(
	ctx context.Context,
	client k8s.Client,
	es esv1.Elasticsearch,
	commonLabels map[string]string,
	applicationSecretName types.NamespacedName,
	elasticsearchSecretName types.NamespacedName,
	serviceAccount commonv1.ServiceAccountName,
	applicationName string,
	applicationUID types.UID,
) error {
	tokenName := tokenName(applicationSecretName.Namespace, applicationName, applicationUID)
	token, err := reconcileApplicationSecret(ctx, client, es, applicationSecretName, commonLabels, tokenName, serviceAccount)
	if err != nil {
		return err
	}
	return reconcileElasticsearchSecret(ctx, client, es, elasticsearchSecretName, commonLabels, *token)
}

func tokenName(
	applicationNamespace, applicationName string,
	applicationUID types.UID,
) string {
	return fmt.Sprintf("%s_%s_%s", applicationNamespace, applicationName, applicationUID)
}
