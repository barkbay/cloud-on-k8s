// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package association

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/volume"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

type ConfigurationHelper interface {
	k8s.Client
	commonv1.AssociationManager

	ConfigurationAnnotation() string

	// Configuration generates the concrete configuration of the associated resource
	Configuration() (map[string]interface{}, error)

	// SslVolume returns the volume that contains the TLS artifacts
	SslVolume() volume.SecretVolume
}

// ElasticsearchAuthSettings returns the user and the password to be used by an associated object to authenticate
// against an Elasticsearch cluster.
func ElasticsearchAuthSettings(
	configurationHelper ConfigurationHelper,
) (username, password string, err error) {
	assocConf := configurationHelper.AssociationConf()
	if !assocConf.AuthIsConfigured() {
		return "", "", nil
	}
	secretObjKey := types.NamespacedName{Namespace: configurationHelper.GetNamespace(), Name: assocConf.AuthSecretName}
	var secret v1.Secret
	if err := configurationHelper.Get(secretObjKey, &secret); err != nil {
		return "", "", err
	}
	return assocConf.AuthSecretKey, string(secret.Data[assocConf.AuthSecretKey]), nil
}

// IsConfiguredIfSet checks if an association is set in the spec and if it has been configured by an association controller.
// This is used to prevent the deployment of an associated resource while the association is not yet fully configured.
func IsConfiguredIfSet(
	associated commonv1.Associated,
	associations []ConfigurationHelper,
	r record.EventRecorder,
) (bool, error) {
	for _, association := range associations {
		esRef := association.AssociationRef()
		assocConf := association.AssociationConf()
		if (&esRef).IsDefined() && !assocConf.IsConfigured() {
			r.Event(associated, v1.EventTypeWarning, events.EventAssociationError, "Elasticsearch backend is not configured")
			log.Info("Association not established: skipping associated resource deployment reconciliation",
				"kind", associated.GetObjectKind().GroupVersionKind().Kind,
				"namespace", associated.GetNamespace(),
				"name", associated.GetName(),
			)
			return false, nil
		}
	}
	return true, nil
}
