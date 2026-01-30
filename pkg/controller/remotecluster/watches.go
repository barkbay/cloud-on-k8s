// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package remotecluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/certificates/remoteca"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/certificates/transport"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/remotecluster/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/maps"
)

// addWatches sets watches on objects needed to manage the association between a local and a remote cluster.
// This is for stateful Elasticsearch resources.
func addWatches(mgr manager.Manager, c controller.Controller, r *ReconcileRemoteClustersStateful) error {
	// Watch for changes to stateful Elasticsearch
	if err := c.Watch(source.Kind(mgr.GetCache(), &esv1.Elasticsearch{}, &handler.TypedEnqueueRequestForObject[*esv1.Elasticsearch]{})); err != nil {
		return err
	}

	// Emit changes to remote clusters to update API keys.
	// When a stateful ES references another cluster, trigger reconciliation of that cluster.
	if err := c.Watch(
		source.Kind(
			mgr.GetCache(),
			&esv1.Elasticsearch{},
			handler.TypedEnqueueRequestsFromMapFunc[*esv1.Elasticsearch, reconcile.Request](
				func(ctx context.Context, elasticsearch *esv1.Elasticsearch) []reconcile.Request {
					requests := make([]reconcile.Request, 0, len(elasticsearch.Spec.RemoteClusters))
					for _, remoteCluster := range elasticsearch.Spec.RemoteClusters {
						esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(elasticsearch.Namespace)
						// Only trigger if the remote is also stateful (default kind)
						if !esRef.IsStateless() {
							requests = append(requests, reconcile.Request{NamespacedName: esRef.NamespacedName()})
						}
					}
					return requests
				},
			),
		),
	); err != nil {
		return err
	}

	// Watch Secrets that contain:
	//  * Remote certificate authorities managed by this controller.
	//  * API keys
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Secret{},
			handler.TypedEnqueueRequestsFromMapFunc[*corev1.Secret, reconcile.Request](newRequestsFromMatchedLabels(false)),
		)); err != nil {
		return err
	}

	// Dynamically watches the certificate authorities involved in a cluster relationship
	if err := c.Watch(source.Kind(mgr.GetCache(), &corev1.Secret{}, r.watches.Secrets)); err != nil {
		return err
	}

	return r.watches.Secrets.AddHandlers(
		&watches.OwnerWatch[*corev1.Secret]{
			Scheme:       mgr.GetScheme(),
			Mapper:       mgr.GetRESTMapper(),
			OwnerType:    &esv1.Elasticsearch{},
			IsController: true,
		},
	)
}

// addWatchesStateless sets watches on objects needed to manage the association between a local and a remote cluster.
// This is for stateless ElasticsearchStateless resources.
func addWatchesStateless(mgr manager.Manager, c controller.Controller, r *ReconcileRemoteClustersStateless) error {
	// Watch for changes to stateless ElasticsearchStateless
	if err := c.Watch(source.Kind(mgr.GetCache(), &essv1alpha1.ElasticsearchStateless{}, &handler.TypedEnqueueRequestForObject[*essv1alpha1.ElasticsearchStateless]{})); err != nil {
		return err
	}

	// Emit changes to remote clusters to update API keys.
	// When a stateless ES references another cluster, trigger reconciliation of that cluster.
	if err := c.Watch(
		source.Kind(
			mgr.GetCache(),
			&essv1alpha1.ElasticsearchStateless{},
			handler.TypedEnqueueRequestsFromMapFunc[*essv1alpha1.ElasticsearchStateless, reconcile.Request](
				func(ctx context.Context, elasticsearch *essv1alpha1.ElasticsearchStateless) []reconcile.Request {
					requests := make([]reconcile.Request, 0, len(elasticsearch.Spec.RemoteClusters))
					for _, remoteCluster := range elasticsearch.Spec.RemoteClusters {
						esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(elasticsearch.Namespace)
						// Only trigger if the remote is also stateless
						if esRef.IsStateless() {
							requests = append(requests, reconcile.Request{NamespacedName: esRef.NamespacedName()})
						}
					}
					return requests
				},
			),
		),
	); err != nil {
		return err
	}

	// Watch Secrets that contain:
	//  * Remote certificate authorities managed by this controller.
	//  * API keys
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Secret{},
			handler.TypedEnqueueRequestsFromMapFunc[*corev1.Secret, reconcile.Request](newRequestsFromMatchedLabels(true)),
		)); err != nil {
		return err
	}

	// Dynamically watches the certificate authorities involved in a cluster relationship
	if err := c.Watch(source.Kind(mgr.GetCache(), &corev1.Secret{}, r.watches.Secrets)); err != nil {
		return err
	}

	return r.watches.Secrets.AddHandlers(
		&watches.OwnerWatch[*corev1.Secret]{
			Scheme:       mgr.GetScheme(),
			Mapper:       mgr.GetRESTMapper(),
			OwnerType:    &essv1alpha1.ElasticsearchStateless{},
			IsController: true,
		},
	)
}

// newRequestsFromMatchedLabels creates a watch handler function that creates reconcile requests based on the
// labels set on a Secret which contains the remote CA.
// The isStateless parameter determines whether to look for stateless or stateful cluster labels.
func newRequestsFromMatchedLabels(isStateless bool) handler.TypedMapFunc[*corev1.Secret, reconcile.Request] {
	clusterNameLabel := label.ClusterNameLabelName
	expectedRemoteKindLabel := kindStateful
	if isStateless {
		clusterNameLabel = label.StatelessClusterNameLabelName
		expectedRemoteKindLabel = kindStateless
	}

	return func(ctx context.Context, obj *corev1.Secret) []reconcile.Request {
		labels := obj.GetLabels()
		if maps.ContainsKeys(labels, RemoteClusterNameLabelName, RemoteClusterNamespaceLabelName, commonv1.TypeLabelName) {
			// Remote cluster CA
			if labels[commonv1.TypeLabelName] != remoteca.TypeLabelValue {
				return nil
			}
			// Only trigger reconciliation if the remote cluster kind matches what this controller handles.
			// This prevents the stateful controller from trying to reconcile stateless clusters and vice versa.
			if labels[RemoteClusterKindLabelName] != expectedRemoteKindLabel {
				return nil
			}
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{
					Namespace: labels[RemoteClusterNamespaceLabelName],
					Name:      labels[RemoteClusterNameLabelName]},
				},
			}
		}

		if maps.ContainsKeys(labels, clusterNameLabel, commonv1.TypeLabelName) {
			if labels[commonv1.TypeLabelName] != keystore.RemoteClusterAPIKeysType {
				return nil
			}
			// Remote cluster API keys Secret event.
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{
					Namespace: obj.Namespace,
					Name:      labels[clusterNameLabel]},
				},
			}
		}

		return nil
	}
}

func watchName(local types.NamespacedName, localIsStateless bool, remote types.NamespacedName, remoteIsStateless bool) string {
	localKind := "es"
	if localIsStateless {
		localKind = "ess"
	}
	remoteKind := "es"
	if remoteIsStateless {
		remoteKind = "ess"
	}
	return fmt.Sprintf(
		"%s-%s-%s-%s-%s-%s",
		local.Namespace,
		local.Name,
		localKind,
		remote.Namespace,
		remote.Name,
		remoteKind,
	)
}

// addCertificatesAuthorityWatches sets some watches on all secrets containing the certificate of a CA involved in a association.
// The local CA is watched to update the trusted certificates in the remote clusters.
// The remote CAs are watched to update the trusted certificates of the local cluster.
func addCertificatesAuthorityWatches(
	reconcileClusterAssociation *baseRemoteClustersReconciler,
	local, remote types.NamespacedName,
	localIsStateless, remoteIsStateless bool,
	remoteNamer escommon.Namer,
) error {
	secretRef := transport.PublicCertsSecretRef(remote, remoteNamer)
	// Watch the CA secret of Elasticsearch clusters which are involved in a association.
	err := reconcileClusterAssociation.watches.Secrets.AddHandler(watches.NamedWatch[*corev1.Secret]{
		Name:    watchName(local, localIsStateless, remote, remoteIsStateless),
		Watched: []commonv1.KindNamespacedName{{Namespace: secretRef.Namespace, Name: secretRef.Name}},
		Watcher: types.NamespacedName{
			Namespace: local.Namespace,
			Name:      local.Name,
		},
	})
	if err != nil {
		return err
	}

	return nil
}
