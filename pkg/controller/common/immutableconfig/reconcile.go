// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
)

// ReconcileImmutableSecret creates the immutable secret if it does not already exist.
// Since content is immutable and addressed by hash, if the secret exists it is guaranteed
// to have the correct content, so no update is needed.
// If owner is non-nil, a controller owner reference is set so that Kubernetes garbage-collects
// the secret when the owner is deleted.
func ReconcileImmutableSecret(ctx context.Context, c client.Client, secret corev1.Secret, owner ...client.Object) error {
	if len(owner) > 0 && owner[0] != nil {
		if err := controllerutil.SetControllerReference(owner[0], &secret, scheme.Scheme); err != nil {
			return err
		}
	}

	var existing corev1.Secret
	err := c.Get(ctx, types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}, &existing)
	if err == nil {
		// Already exists with the same content-addressed name; nothing to do.
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	if err := c.Create(ctx, &secret); err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// ReconcileImmutableConfigMap creates the immutable ConfigMap if it does not already exist.
// Since content is immutable and addressed by hash, if the ConfigMap exists it is guaranteed
// to have the correct content, so no update is needed.
// If owner is non-nil, a controller owner reference is set so that Kubernetes garbage-collects
// the ConfigMap when the owner is deleted.
func ReconcileImmutableConfigMap(ctx context.Context, c client.Client, cm corev1.ConfigMap, owner ...client.Object) error {
	if len(owner) > 0 && owner[0] != nil {
		if err := controllerutil.SetControllerReference(owner[0], &cm, scheme.Scheme); err != nil {
			return err
		}
	}

	var existing corev1.ConfigMap
	err := c.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}, &existing)
	if err == nil {
		// Already exists with the same content-addressed name; nothing to do.
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	if err := c.Create(ctx, &cm); err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// GCUnreferencedSecrets deletes immutable secrets that are not in the protected set.
// It lists secrets matching the given labels and deletes any whose name is not in protectedNames.
func GCUnreferencedSecrets(
	ctx context.Context,
	c client.Client,
	namespace string,
	labelSelector client.MatchingLabels,
	protectedNames sets.Set[string],
) error {
	log := crlog.FromContext(ctx)

	var secretList corev1.SecretList
	if err := c.List(ctx, &secretList, client.InNamespace(namespace), labelSelector); err != nil {
		return err
	}

	for i := range secretList.Items {
		secret := &secretList.Items[i]
		if protectedNames.Has(secret.Name) {
			continue
		}
		log.Info("Deleting unreferenced immutable config secret", "secret", secret.Name)
		if err := c.Delete(ctx, secret); err != nil && !errors.IsNotFound(err) && !errors.IsConflict(err) {
			return err
		}
	}
	return nil
}

// GCUnreferencedConfigMaps deletes immutable ConfigMaps that are not in the protected set.
// It lists ConfigMaps matching the given labels and deletes any whose name is not in protectedNames.
func GCUnreferencedConfigMaps(
	ctx context.Context,
	c client.Client,
	namespace string,
	labelSelector client.MatchingLabels,
	protectedNames sets.Set[string],
) error {
	log := crlog.FromContext(ctx)

	var cmList corev1.ConfigMapList
	if err := c.List(ctx, &cmList, client.InNamespace(namespace), labelSelector); err != nil {
		return err
	}

	for i := range cmList.Items {
		cm := &cmList.Items[i]
		if protectedNames.Has(cm.Name) {
			continue
		}
		log.Info("Deleting unreferenced immutable config ConfigMap", "configmap", cm.Name)
		if err := c.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) && !errors.IsConflict(err) {
			return err
		}
	}
	return nil
}
