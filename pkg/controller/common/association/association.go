// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package association

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

// ElasticsearchAuthSettings returns the user and the password to be used by an associated object to authenticate
// against an Elasticsearch cluster.
func ElasticsearchAuthSettings(
	c k8s.Client,
	associated commonv1.Associated,
	associationConf commonv1.AssociationConf,
) (username, password string, err error) {
	if !associationConf.AuthIsConfigured() {
		return "", "", nil
	}
	secretObjKey := types.NamespacedName{Namespace: associated.GetNamespace(), Name: associationConf.AuthSecretName}
	var secret v1.Secret
	if err := c.Get(secretObjKey, &secret); err != nil {
		return "", "", err
	}
	return associationConf.AuthSecretKey, string(secret.Data[associationConf.AuthSecretKey]), nil
}

// IsConfiguredIfSet checks if an association is set in the spec and if it has been configured by an association controller.
// This is used to prevent the deployment of an associated resource while the association is not yet fully configured.
func IsConfiguredIfSet(
	associated commonv1.Associated,
	associationKind commonv1.AssociationKind,
	r record.EventRecorder,
) (bool, error) {
	esRef, err := associated.AssociationRef(associationKind)
	if err != nil {
		return false, err
	}
	assocConf, err := associated.AssociationConf(associationKind)
	if err != nil {
		return false, err
	}
	if (&esRef).IsDefined() && !assocConf.IsConfigured() {
		r.Event(associated, v1.EventTypeWarning, events.EventAssociationError, "Elasticsearch backend is not configured")
		log.Info("Elasticsearch association not established: skipping associated resource deployment reconciliation",
			"kind", associated.GetObjectKind().GroupVersionKind().Kind,
			"namespace", associated.GetNamespace(),
			"name", associated.GetName(),
		)
		return false, nil
	}
	return true, nil
}
