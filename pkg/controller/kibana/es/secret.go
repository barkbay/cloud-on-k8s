// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package es

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/volume"
)

var eSCertsVolumeMountPath = "/usr/share/kibana/config/elasticsearch-certs"

// CaCertSecretVolume returns a SecretVolume to hold the Elasticsearch CA certs for the given Kibana resource.
func CaCertSecretVolume(associationConf *commonv1.AssociationConf) volume.SecretVolume {
	return volume.NewSecretVolumeWithMountPath(
		associationConf.GetCASecretName(),
		"elasticsearch-certs",
		eSCertsVolumeMountPath,
	)
}
