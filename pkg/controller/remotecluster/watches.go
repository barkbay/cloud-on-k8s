// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package remotecluster

import (
	"fmt"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/certificates/transport"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// addWatches set watches on objects needed to manage the association between a local and a remote cluster.
func addWatches(c controller.Controller, r *ReconcileRemoteCluster) error {
	// Watch for changes to RemoteCluster
	if err := c.Watch(&source.Kind{Type: &esv1.Elasticsearch{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// Watch Secrets that contain remote certificate authorities managed by this controller
	if err := c.Watch(&source.Kind{Type: &v1.Secret{}}, &handler.EnqueueRequestsFromMapFunc{
		ToRequests: newToRequestsFuncFromSecret(),
	}); err != nil {
		return err
	}

	// Dynamically watch certificate authorities involved in a cluster relationship
	if err := c.Watch(&source.Kind{Type: &v1.Secret{}}, r.watches.Secrets); err != nil {
		return err
	}

	// Add licence watcher
	if err := r.watches.Secrets.AddHandler(license.NewWatch(reconcileAllRemoteClusters(r.Client))); err != nil {
		return err
	}

	return nil
}

// reconcileAllRemoteClusters creates a reconcile request for each currently existing remote cluster resource.
func reconcileAllRemoteClusters(c k8s.Client) handler.ToRequestsFunc {
	return handler.ToRequestsFunc(func(object handler.MapObject) []reconcile.Request {
		var list esv1.ElasticsearchList
		if err := c.List(&list, &client.ListOptions{}); err != nil {
			log.Error(err, "failed to list Elasticsearch clusters in watch handler for enterprise licenses")
			// dropping any errors on the floor here
			return nil
		}
		var reqs []reconcile.Request
		for _, rc := range list.Items {
			log.Info("Synthesizing reconcile for ", "resource", k8s.ExtractNamespacedName(&rc))
			reqs = append(reqs, reconcile.Request{
				NamespacedName: k8s.ExtractNamespacedName(&rc),
			})
		}
		return reqs
	})
}

// newToRequestsFuncFromSecret creates a watch handler function that creates reconcile requests based on the
// labels set on a Secret which contains the remote CA.
func newToRequestsFuncFromSecret() handler.ToRequestsFunc {
	return handler.ToRequestsFunc(func(obj handler.MapObject) []reconcile.Request {
		labels := obj.Meta.GetLabels()
		if secretType, ok := labels[common.TypeLabelName]; !ok || secretType != TypeLabelValue {
			return nil
		}
		clusterAssociationName, ok := labels[RemoteClusterNameLabelName]
		if !ok {
			return nil
		}
		clusterAssociationNamespace, ok := labels[RemoteClusterNamespaceLabelName]
		if !ok {
			return nil
		}
		return []reconcile.Request{
			{NamespacedName: types.NamespacedName{
				Namespace: clusterAssociationNamespace,
				Name:      clusterAssociationName},
			},
		}
	})
}

func watchName(local types.NamespacedName, remote types.NamespacedName) string {
	return fmt.Sprintf(
		"%s-%s-%s-%s",
		local.Namespace,
		local.Name,
		remote.Namespace,
		remote.Name,
	)
}

// addCertificatesAuthorityWatches sets some watches on all secrets containing the certificate of a CA involved in a association.
func addCertificatesAuthorityWatches(
	reconcileClusterAssociation *ReconcileRemoteCluster,
	local, remote types.NamespacedName) error {
	// Watch the CA secret of Elasticsearch clusters which are involved in a association.
	err := reconcileClusterAssociation.watches.Secrets.AddHandler(watches.NamedWatch{
		Name:    watchName(local, remote),
		Watched: []types.NamespacedName{transport.PublicCertsSecretRef(remote)},
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
