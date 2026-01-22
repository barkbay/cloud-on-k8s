// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/volume"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// Constants to use for the `elasticsearch.yml` config file in an ES pod.
const (
	ConfigFileName        = "elasticsearch.yml"
	ConfigVolumeName      = "elastic-internal-elasticsearch-config"
	ConfigVolumeMountPath = "/mnt/elastic-internal/elasticsearch-config"
)

// ConfigSecretName is the name of the secret that holds the ES config for the given StatefulSet or Deployment.
func ConfigSecretName(ssetName string, isStateless bool) string {
	namer := escommon.StatefulNamer
	if isStateless {
		namer = escommon.StatelessNamer
	}
	return namer.Suffix(ssetName, escommon.ConfigSecretSuffix)
}

// ConfigSecretVolume returns a SecretVolume to hold the config of nodes in the given stateful set..
func ConfigSecretVolume(ssetName string, isStateless bool) volume.SecretVolume {
	return volume.NewSecretVolumeWithMountPath(
		ConfigSecretName(ssetName, isStateless),
		ConfigVolumeName,
		ConfigVolumeMountPath,
	)
}

func ConfigSecret(es escommon.ElasticsearchCluster, ssetName string, configData []byte, meta metadata.Metadata) corev1.Secret {
	mergedMeta := meta.Merge(metadata.Metadata{Labels: label.NewConfigLabels(k8s.ExtractNamespacedName(es), ssetName)})
	return corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   es.GetNamespace(),
			Name:        ConfigSecretName(ssetName, es.IsStateless()),
			Labels:      mergedMeta.Labels,
			Annotations: mergedMeta.Annotations,
		},
		Data: map[string][]byte{
			ConfigFileName: configData,
		},
	}
}

// ReconcileConfig ensures the ES config for the pod is set in the apiserver.
func ReconcileConfig(ctx context.Context, client k8s.Client, es escommon.ElasticsearchCluster, ssetName string, config CanonicalConfig, meta metadata.Metadata) error {
	rendered, err := config.Render()
	if err != nil {
		return err
	}
	expected := ConfigSecret(es, ssetName, rendered, meta)
	_, err = reconciler.ReconcileSecret(ctx, client, expected, es)
	return err
}

// DeleteConfig removes the configuration Secret corresponding to the given Statefulset.
func DeleteConfig(ctx context.Context, client k8s.Client, namespace string, ssetName string) error {
	// build a dummy config with no data but the correct Namespace & Name,
	// to target the correct resource for deletion
	cfgSecret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      ConfigSecretName(ssetName, false /* This function should only be required when scaling down StatefulSets*/),
		},
	}
	return client.Delete(ctx, &cfgSecret)
}
