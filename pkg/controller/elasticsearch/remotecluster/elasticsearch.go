// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package remotecluster

import (
	"context"
	"fmt"

	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/hash"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("remotecluster")

// UpdateRemoteCluster updates the remote clusters in the persistent settings by calling the Elasticsearch API.
func UpdateRemoteCluster(
	c k8s.Client,
	esClient esclient.Client,
	es esv1.Elasticsearch,
) error {

	currentRemoteClusters, err := getCurrentRemoteClusters(es)
	if err != nil {
		return err
	}
	if currentRemoteClusters == nil {
		currentRemoteClusters = make(map[string]string)
	}
	expectedRemoteClusters := getExpectedRemoteClusters(es)

	updated := false
	// RemoteClusters to add or update
	for name, remoteCluster := range expectedRemoteClusters {
		if currentConfigHash, ok := currentRemoteClusters[name]; !ok || currentConfigHash != remoteCluster.ConfigHash {
			// Declare remote cluster in ES
			seedHosts := []string{services.ExternalTransportServiceHostname(remoteCluster.ElasticsearchRef.NamespacedName())}
			persistentSettings := newRemoteClusterSetting(name, seedHosts)
			log.Info("Add or update remote cluster",
				"namespace", es.Namespace,
				"es_name", es.Name,
				"remoteCluster", remoteCluster.Name,
				"seeds", seedHosts,
			)
			if err := updateRemoteCluster(esClient, persistentSettings); err != nil {
				return err
			}
			updated = true
		}
	}

	// RemoteClusters to remove
	for name := range currentRemoteClusters {
		if _, ok := expectedRemoteClusters[name]; !ok {
			persistentSettings := newRemoteClusterSetting(name, nil)
			log.Info("Remove remote cluster",
				"namespace", es.Namespace,
				"es_name", es.Name,
				"remoteCluster", name,
			)
			err := updateRemoteCluster(esClient, persistentSettings)
			if err != nil {
				return err
			}
			updated = true
			delete(currentRemoteClusters, name)
		}
	}

	// Save the current list of remote clusters in an annotation
	if updated {
		return annotateWithRemoteClusters(c, es, expectedRemoteClusters)
	}
	return nil
}

func getRemoteClusterKey(remoteCluster v1.ObjectSelector) string {
	return fmt.Sprintf("%s-%s", remoteCluster.Namespace, remoteCluster.Name)
}

// getExpectedRemoteClusters returns a map with the expected remote clusters
// A map is returned here because it will be used to quickly compare with the ones that are new or missing.
func getExpectedRemoteClusters(es esv1.Elasticsearch) map[string]expectedRemoteClusterConfiguration {
	remoteClusters := make(map[string]expectedRemoteClusterConfiguration)
	for _, remoteCluster := range es.Spec.RemoteClusters.K8sLocal {
		if !remoteCluster.IsDefined() {
			continue
		}
		esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(es.Namespace)
		remoteClusterName := getRemoteClusterKey(esRef)
		remoteClusters[remoteClusterName] = expectedRemoteClusterConfiguration{
			remoteClusterState: remoteClusterState{
				Name:       remoteClusterName,
				ConfigHash: hash.HashObject(remoteCluster),
			},
			K8sLocalRemoteCluster: remoteCluster,
		}
	}
	return remoteClusters
}

// newRemoteClusterSetting creates a persistent setting to add or remove a remote cluster.
func newRemoteClusterSetting(name string, seedHosts []string) esclient.Settings {
	return esclient.Settings{
		PersistentSettings: &esclient.SettingsGroup{
			Cluster: esclient.Cluster{
				RemoteClusters: map[string]esclient.RemoteCluster{
					name: {
						Seeds: seedHosts,
					},
				},
			},
		},
	}
}

// updateRemoteCluster makes a call to an Elasticsearch cluster to apply a persistent setting.
func updateRemoteCluster(esClient esclient.Client, persistentSettings esclient.Settings) error {
	ctx, cancel := context.WithTimeout(context.Background(), esclient.DefaultReqTimeout)
	defer cancel()
	return esClient.UpdateSettings(ctx, persistentSettings)
}