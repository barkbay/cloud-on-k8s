// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package transport

import (
	"bytes"
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// ReconcileTransportCertsPublicSecret reconciles the Secret containing the publicly available transport CA
// information.
func ReconcileTransportCertsPublicSecret(
	ctx context.Context,
	c k8s.Client,
	es escommon.ElasticsearchCluster,
	ca *certificates.CA,
	additionalCAs []byte,
	meta metadata.Metadata,
) error {
	esNSN := k8s.ExtractNamespacedName(es)

	// Get the appropriate namer for this cluster type
	namer := escommon.StatefulNamer
	if es.IsStateless() {
		namer = escommon.StatelessNamer
	}

	secretMetadata := k8s.ToObjectMeta(PublicCertsSecretRef(esNSN, namer))

	secretMetadata.Labels = meta.Labels
	secretMetadata.Annotations = meta.Annotations

	expected := corev1.Secret{
		ObjectMeta: secretMetadata,
		Data: map[string][]byte{
			certificates.CAFileName: bytes.Join([][]byte{certificates.EncodePEMCert(ca.Cert.Raw), additionalCAs}, nil),
		},
	}

	// Don't set an ownerRef for public transport certs secrets, likely to be copied into different namespaces.
	// See https://github.com/elastic/cloud-on-k8s/issues/3986.
	_, err := reconciler.ReconcileSecretNoOwnerRef(ctx, c, expected, es)
	return err
}

// PublicCertsSecretRef returns the NamespacedName for the Secret containing the publicly available transport CA.
func PublicCertsSecretRef(es types.NamespacedName, namer escommon.Namer) types.NamespacedName {
	return types.NamespacedName{
		Name:      certificates.PublicTransportCertsSecretName(namer, es.Name),
		Namespace: es.Namespace,
	}
}
