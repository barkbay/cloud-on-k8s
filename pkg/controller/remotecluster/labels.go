// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package remotecluster

import (
	"fmt"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/certificates/remoteca"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// RemoteClusterNamespaceLabelName used to represent the namespace of the RemoteCluster in a TrustRelationship.
	RemoteClusterNamespaceLabelName = "elasticsearch.k8s.elastic.co/remote-cluster-namespace"
	// RemoteClusterNameLabelName used to represent the name of the RemoteCluster in a TrustRelationship.
	RemoteClusterNameLabelName = "elasticsearch.k8s.elastic.co/remote-cluster-name"
	// remoteCASecretSuffix is the suffix added to the aforementioned Secret.
	remoteCASecretSuffix = "remote-ca"
)

func remoteCAObjectMeta(
	name string,
	owner escommon.ElasticsearchCluster,
	remote types.NamespacedName,
) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: owner.GetNamespace(),
		Labels: maps.Merge(
			map[string]string{
				RemoteClusterNamespaceLabelName: remote.Namespace,
				RemoteClusterNameLabelName:      remote.Name,
			},
			remoteca.Labels(owner.GetName()),
		),
	}
}

// remoteCASecretName returns the name of the Secret that contains the transport CA of a remote cluster.
// The secret is named using the local cluster's namer since it's stored in the local cluster's namespace.
func remoteCASecretName(
	localClusterName string,
	remoteCluster types.NamespacedName,
) string {
	// Use StatefulNamer as the default for backward compatibility.
	// The secret name format is consistent regardless of cluster type.
	return escommon.StatefulNamer.Suffix(
		fmt.Sprintf("%s-%s-%s", localClusterName, remoteCluster.Namespace, remoteCluster.Name),
		remoteCASecretSuffix,
	)
}
