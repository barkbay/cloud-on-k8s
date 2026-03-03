// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package immutableconfig

import corev1 "k8s.io/api/core/v1"

// PatchSecretVolumesByClassification updates secret volume references in-place for volumes
// whose names match the given classification in classifier.
func PatchSecretVolumesByClassification(
	volumes []corev1.Volume,
	classifier Classifier,
	classification Classification,
	secretName string,
) {
	for i := range volumes {
		v := &volumes[i]
		if v.Secret == nil {
			continue
		}
		if classifier.Classify(v.Name) == classification {
			v.Secret.SecretName = secretName
		}
	}
}
