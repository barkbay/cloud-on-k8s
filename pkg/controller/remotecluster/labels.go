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
	// RemoteClusterKindLabelName used to represent the kind of the RemoteCluster in a TrustRelationship.
	RemoteClusterKindLabelName = "elasticsearch.k8s.elastic.co/remote-cluster-kind"
	// ClusterKindLabelName used to represent the kind of the local cluster in a TrustRelationship.
	ClusterKindLabelName = "elasticsearch.k8s.elastic.co/cluster-kind"
	// remoteCASecretSuffix is the suffix added to the aforementioned Secret.
	remoteCASecretSuffix = "remote-ca"

	// Kind label values
	kindStateful  = "stateful"
	kindStateless = "stateless"
)

func remoteCAObjectMeta(
	name string,
	owner escommon.ElasticsearchCluster,
	remote types.NamespacedName,
	remoteIsStateless bool,
) metav1.ObjectMeta {
	ownerKind := kindStateful
	if owner.IsStateless() {
		ownerKind = kindStateless
	}
	remoteKind := kindStateful
	if remoteIsStateless {
		remoteKind = kindStateless
	}
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: owner.GetNamespace(),
		Labels: maps.Merge(
			map[string]string{
				RemoteClusterNamespaceLabelName: remote.Namespace,
				RemoteClusterNameLabelName:      remote.Name,
				RemoteClusterKindLabelName:      remoteKind,
				ClusterKindLabelName:            ownerKind,
			},
			remoteca.Labels(owner.GetName(), owner.IsStateless()),
		),
	}
}

// remoteCASecretName returns the name of the Secret that contains the transport CA of a remote cluster.
// It uses the appropriate namer based on whether the local cluster is stateless.
// For backward compatibility, the kind indicator is only added for stateless remote clusters.
func remoteCASecretName(
	localCluster escommon.ElasticsearchCluster,
	remoteCluster types.NamespacedName,
	remoteIsStateless bool,
) string {
	namer := escommon.StatefulNamer
	if localCluster.IsStateless() {
		namer = escommon.StatelessNamer
	}
	// Only add kind indicator for stateless remotes to maintain backward compatibility
	baseName := fmt.Sprintf("%s-%s-%s", localCluster.GetName(), remoteCluster.Namespace, remoteCluster.Name)
	if remoteIsStateless {
		baseName = fmt.Sprintf("%s-ess", baseName)
	}
	return namer.Suffix(baseName, remoteCASecretSuffix)
}
