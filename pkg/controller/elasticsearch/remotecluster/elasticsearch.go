// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package remotecluster

import (
	"context"
	"encoding/json"
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("remotecluster")

const (
	RemoteClustersAnnotationName = "elasticsearch.k8s.elastic.co/remote-clusters"
)

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
		currentRemoteClusters = make(map[string]types.NamespacedName)
	}
	expectedRemoteClusters, err := getExpectedRemoteClusters(es)
	if err != nil {
		return err
	}

	updated := false
	// RemoteClusters to add
	for name, remoteCluster := range expectedRemoteClusters {
		if _, ok := currentRemoteClusters[name]; !ok {
			// Declare remote cluster in ES
			seedHosts := []string{services.ExternalTransportServiceHostname(remoteCluster)}
			persistentSettings := newRemoteClusterSetting(name, seedHosts)
			log.V(1).Info("Add new remote cluster",
				"localCluster", es.Name,
				"remoteCluster", remoteCluster.Name,
				"seeds", seedHosts,
			)
			err := updateRemoteCluster(esClient, persistentSettings)
			if err != nil {
				return err
			}
			updated = true
		}
	}

	// RemoteClusters to remove
	for name := range currentRemoteClusters {
		if _, ok := expectedRemoteClusters[name]; !ok {
			persistentSettings := newRemoteClusterSetting(name, nil)
			log.V(1).Info("Remove remote cluster",
				"localCluster", es.Name,
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

func getRemoteClusterKey(remoteCluster types.NamespacedName) string {
	return fmt.Sprintf("%s-%s", remoteCluster.Namespace, remoteCluster.Name)
}

// getCurrentRemoteClusters returns a map with the current remote clusters
// A map is returned here because it will be used quickly compare with the ones that are new or missing.
func getCurrentRemoteClusters(es esv1.Elasticsearch) (map[string]types.NamespacedName, error) {
	serializedRemoteClusters, ok := es.Annotations[RemoteClustersAnnotationName]
	if !ok {
		return nil, nil
	}
	var remoteClustersArray []types.NamespacedName
	if err := json.Unmarshal([]byte(serializedRemoteClusters), &remoteClustersArray); err != nil {
		return nil, err
	}

	remoteClusters := make(map[string]types.NamespacedName)
	for _, remoteCluster := range remoteClustersArray {
		remoteClusters[getRemoteClusterKey(remoteCluster)] = remoteCluster
	}

	return remoteClusters, nil
}

// getExpectedRemoteClusters returns a map with the expected remote clusters
// A map is returned here because it will be used quickly compare with the ones that are new or missing.
func getExpectedRemoteClusters(es esv1.Elasticsearch) (map[string]types.NamespacedName, error) {
	remoteClusters := make(map[string]types.NamespacedName)
	for _, remoteCluster := range es.Spec.RemoteClusters.K8sLocal {
		nn := remoteCluster.NamespacedName()
		remoteClusters[getRemoteClusterKey(nn)] = nn
	}
	return remoteClusters, nil
}

func annotateWithRemoteClusters(c k8s.Client, es esv1.Elasticsearch, remoteClusters map[string]types.NamespacedName) error {
	// We don't need to store the map in the annotation
	remoteClustersList := make([]types.NamespacedName, len(remoteClusters))
	i := 0
	for _, remoteCluster := range remoteClusters {
		remoteClustersList[i] = remoteCluster
		i++
	}
	// serialize the remote clusters list and update the object
	serializedRemoteClusters, err := json.Marshal(remoteClustersList)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize configuration")
	}

	if es.Annotations == nil {
		es.Annotations = make(map[string]string)
	}
	// Convert remoteClusters to JSon
	es.Annotations[RemoteClustersAnnotationName] = string(serializedRemoteClusters)
	return c.Update(&es)
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
