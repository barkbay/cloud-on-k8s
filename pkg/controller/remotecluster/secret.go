// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package remotecluster

import (
	"context"
	"fmt"

	"go.elastic.co/apm/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/certificates/transport"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

// createOrUpdateCertificateAuthorities creates the two Secrets that are needed to establish a trust relationship between
// two clusters. This is a bidirectional, symmetrical, action. In order to establish the trust relationship between
// a local and a remote cluster we must:
// * Copy the CA of the local cluster to the remote one.
// * Copy the CA of the remote cluster to the local one.
func createOrUpdateCertificateAuthorities(
	ctx context.Context,
	r *baseRemoteClustersReconciler,
	local, remote escommon.ElasticsearchCluster,
) *reconciler.Results {
	span, _ := apm.StartSpan(ctx, "create_or_update_remote_ca", tracing.SpanTypeApp)
	defer span.End()
	results := &reconciler.Results{}

	localClusterKey := k8s.ExtractNamespacedName(local)
	remoteClusterKey := k8s.ExtractNamespacedName(remote)
	localIsStateless := local.IsStateless()
	remoteIsStateless := remote.IsStateless()

	// Add watches on the CA secret of the local cluster.
	if err := addCertificatesAuthorityWatches(r, localClusterKey, remoteClusterKey, localIsStateless, remoteIsStateless, escommon.NamerFor(remote)); err != nil {
		return results.WithError(err)
	}

	// Add watches on the CA secret of the remote cluster.
	if err := addCertificatesAuthorityWatches(r, remoteClusterKey, localClusterKey, remoteIsStateless, localIsStateless, escommon.NamerFor(local)); err != nil {
		return results.WithError(err)
	}

	ulog.FromContext(ctx).V(1).Info(
		"Setting up remote CA",
		"local_namespace", localClusterKey.Namespace,
		"local_name", localClusterKey.Namespace,
		"remote_namespace", remote.GetNamespace(),
		"remote_name", remote.GetName(),
	)

	//  Copy CA from remote (source) to local (target) cluster
	if err := copyCertificateAuthority(ctx, r, remote, local); err != nil {
		if !errors.IsNotFound(err) {
			return results.WithError(err)
		}
		results.WithRequeue()
	}

	// Reciprocally, copy CA from local (source) to remote (target) cluster
	if err := copyCertificateAuthority(ctx, r, local, remote); err != nil {
		if !errors.IsNotFound(err) {
			return results.WithError(err)
		}
		results.WithRequeue()
	}

	return nil
}

// copyCertificateAuthority creates a copy of the CA from a source cluster to a target cluster
func copyCertificateAuthority(
	ctx context.Context,
	r *baseRemoteClustersReconciler,
	source, target escommon.ElasticsearchCluster,
) error {
	sourceKey := k8s.ExtractNamespacedName(source)
	// Check if CA of the source cluster exists
	sourceCA := &corev1.Secret{}
	if err := r.Client.Get(ctx, transport.PublicCertsSecretRef(sourceKey, escommon.NamerFor(source)), sourceCA); err != nil {
		return err
	}

	if len(sourceCA.Data[certificates.CAFileName]) == 0 {
		ulog.FromContext(ctx).Info(
			"Cannot find CA cert",
			"local_namespace", source.GetNamespace(),
			"local_name", source.GetName(),
		)
		r.recorder.Event(source, corev1.EventTypeWarning, EventReasonClusterCaCertNotFound, caCertMissingError(sourceKey))
		// CA secrets are watched, we don't need to requeue.
		// If CA is created later it will trigger a new reconciliation.
		return nil
	}

	// Reconcile the copy to the target cluster
	return reconcileRemoteCA(ctx, r.Client, target, sourceKey, source.IsStateless(), sourceCA.Data[certificates.CAFileName])
}

// deleteCertificateAuthorities deletes all the Secrets needed to establish a trust relationship between two clusters.
// This means that the CA of the local cluster is deleted from the remote one and reciprocally the CA from the
// remote cluster must be deleted from the local one.
// Note: This function uses string-based secret names since the clusters may have been deleted.
// The localIsStateless flag indicates whether the local cluster is stateless.
// Since we may not know the remote cluster's kind, we try both stateless and stateful combinations.
func deleteCertificateAuthorities(
	ctx context.Context,
	r *baseRemoteClustersReconciler,
	local, remote types.NamespacedName,
	localIsStateless bool,
) error {
	span, ctx := apm.StartSpan(ctx, "delete_certificate_authorities", tracing.SpanTypeApp)
	defer span.End()

	// Try to delete secrets with all combinations of stateful and stateless naming conventions
	// since we may not know which type the remote cluster was.
	// The secret name format includes both the local cluster's namer prefix and the remote cluster's kind indicator.
	var secretNamesToDelete []struct {
		namespace string
		name      string
	}

	// We know localIsStateless, but not remoteIsStateless, so try both for remote
	for _, remoteIsStateless := range []bool{false, true} {
		// Secret in local namespace: contains remote's CA, named by local cluster
		secretNamesToDelete = append(secretNamesToDelete, struct {
			namespace string
			name      string
		}{local.Namespace, remoteCASecretNameString(local.Name, remote, localIsStateless, remoteIsStateless)})

		// Secret in remote namespace: contains local's CA, named by remote cluster
		// We don't know if remote was stateless, so try both namers
		for _, remoteLocalIsStateless := range []bool{false, true} {
			secretNamesToDelete = append(secretNamesToDelete, struct {
				namespace string
				name      string
			}{remote.Namespace, remoteCASecretNameString(remote.Name, local, remoteLocalIsStateless, localIsStateless)})
		}
	}

	for _, s := range secretNamesToDelete {
		if err := r.Client.Delete(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: s.namespace,
				Name:      s.name,
			},
		}); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	// Remove watches using kind-aware watch names.
	// Since we may not know the remote cluster's kind, try both possibilities.
	for _, remoteIsStateless := range []bool{false, true} {
		r.watches.Secrets.RemoveHandlerForKey(watchName(local, localIsStateless, remote, remoteIsStateless))
		r.watches.Secrets.RemoveHandlerForKey(watchName(remote, remoteIsStateless, local, localIsStateless))
	}

	return nil
}

// remoteCASecretNameString returns the name of the Secret that contains the transport CA of a remote cluster.
// This is a string-based version for use in deletion when we don't have access to the cluster interface.
// localIsStateless determines which namer to use for the local cluster prefix.
// remoteIsStateless determines which kind indicator to include in the name.
// For backward compatibility, the kind indicator is only added for stateless remote clusters.
func remoteCASecretNameString(
	localClusterName string,
	remoteCluster types.NamespacedName,
	localIsStateless bool,
	remoteIsStateless bool,
) string {
	namer := escommon.StatefulNamer
	if localIsStateless {
		namer = escommon.StatelessNamer
	}
	// Only add kind indicator for stateless remotes to maintain backward compatibility
	baseName := fmt.Sprintf("%s-%s-%s", localClusterName, remoteCluster.Namespace, remoteCluster.Name)
	if remoteIsStateless {
		baseName = fmt.Sprintf("%s-ess", baseName)
	}
	return namer.Suffix(baseName, remoteCASecretSuffix)
}

// reconcileRemoteCA does the reconciliation of the Secret that contains certificate authority from a source cluster.
func reconcileRemoteCA(
	ctx context.Context,
	c k8s.Client,
	target escommon.ElasticsearchCluster,
	source types.NamespacedName,
	sourceIsStateless bool,
	sourceCA []byte,
) error {
	span, ctx := apm.StartSpan(ctx, "reconcile_remote_ca", tracing.SpanTypeApp)
	defer span.End()

	// Define the expected source CA object, it lives in the target namespace with the content of the source cluster CA
	expected := corev1.Secret{
		ObjectMeta: remoteCAObjectMeta(remoteCASecretName(target, source, sourceIsStateless), target, source, sourceIsStateless),
		Data: map[string][]byte{
			certificates.CAFileName: sourceCA,
		},
	}

	_, err := reconciler.ReconcileSecret(ctx, c, expected, target)
	return err
}
