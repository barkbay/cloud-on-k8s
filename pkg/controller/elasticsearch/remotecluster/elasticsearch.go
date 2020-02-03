package remotecluster

import (
	"context"

	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

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
