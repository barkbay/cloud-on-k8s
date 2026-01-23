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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/association"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/autoops"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/certificates/remoteca"
	esclient "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

// kindNamespacedNameFor creates a KindNamespacedName from an ElasticsearchCluster.
func kindNamespacedNameFor(cluster escommon.ElasticsearchCluster) commonv1.KindNamespacedName {
	kind := commonv1.ElasticsearchKind
	if cluster.IsStateless() {
		kind = commonv1.ElasticsearchStatelessKind
	}
	return commonv1.KindNamespacedName{
		Kind:      kind,
		Namespace: cluster.GetNamespace(),
		Name:      cluster.GetName(),
	}
}

// newKindNamespacedName creates a KindNamespacedName from namespace, name, and isStateless flag.
// This is useful when we don't have access to the cluster object.
func newKindNamespacedName(namespace, name string, isStateless bool) commonv1.KindNamespacedName {
	kind := commonv1.ElasticsearchKind
	if isStateless {
		kind = commonv1.ElasticsearchStatelessKind
	}
	return commonv1.KindNamespacedName{
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
	}
}

// deleteAllRemoteCa deletes all associated remote certificate authorities.
// isStateless indicates whether the deleted cluster was stateless.
func deleteAllRemoteCa(ctx context.Context, r *baseRemoteClustersReconciler, es types.NamespacedName, isStateless bool) (reconcile.Result, error) {
	span, _ := apm.StartSpan(ctx, "delete_all_remote_ca", tracing.SpanTypeApp)
	defer span.End()

	localKey := newKindNamespacedName(es.Namespace, es.Name, isStateless)
	associatedCAs, err := getAssociatedRemoteCAs(ctx, r.Client, localKey)
	if err != nil {
		return reconcile.Result{}, err
	}
	results := &reconciler.Results{}
	for remoteCluster := range associatedCAs {
		if err := deleteCertificateAuthorities(ctx, r, localKey, remoteCluster); err != nil {
			results.WithError(err)
		}
	}
	return results.Aggregate()
}

func doReconcile(
	ctx context.Context,
	r *baseRemoteClustersReconciler,
	remoteServer escommon.ElasticsearchCluster,
) (reconcile.Result, error) {
	log := ulog.FromContext(ctx)

	expectedRemoteClients, err := getExpectedRemoteClientsFor(ctx, r.Client, remoteServer)
	if err != nil {
		return reconcile.Result{}, err
	}

	enabled, err := r.licenseChecker.EnterpriseFeaturesEnabled(ctx)
	if err != nil {
		return reconcile.Result{RequeueAfter: reconciler.DefaultRequeue}, err
	}
	if !enabled && len(expectedRemoteClients) > 0 {
		log.V(1).Info(
			"Remote cluster controller is an enterprise feature. Enterprise features are disabled",
			"namespace", remoteServer.GetNamespace(), "es_name", remoteServer.GetName(),
		)
		return reconcile.Result{}, nil
	}

	// Get all the clusters to which this reconciled cluster is connected to according to the existing remote CAs.
	// associatedRemoteCAs is used to delete the CA certificates and cancel any trust relationships
	// that may have existed in the past but should not exist anymore.
	remoteServerKey := kindNamespacedNameFor(remoteServer)
	associatedRemoteCAs, err := getAssociatedRemoteCAs(ctx, r.Client, remoteServerKey)
	if err != nil {
		return reconcile.Result{}, err
	}

	var (
		activeAPIKeys esclient.CrossClusterAPIKeyList
		esClient      esclient.Client
	)
	remoteServerSupportsClusterAPIKeys, err := remoteServer.SupportsRemoteClusterAPIKeys()
	if err != nil {
		return reconcile.Result{}, err
	}
	results := &reconciler.Results{}
	if remoteServerSupportsClusterAPIKeys.IsTrue() {
		// Check if the ES API is available. We need it to create, update and invalidate
		// API keys in this cluster.
		if !hasEndpoints(ctx, r.Client, remoteServer) {
			log.Info("Elasticsearch API is not available yet")
			return results.WithRequeue().Aggregate()
		}
		// Create a new client
		newEsClient, err := r.esClientProvider(ctx, r.Client, r.Dialer, remoteServer)
		if err != nil {
			return reconcile.Result{}, err
		}
		// Check that the API is available
		esClient = newEsClient
		// Get all the API Keys, for that specific client, on the reconciled cluster.
		crossClusterAPIKeys, err := esClient.GetCrossClusterAPIKeys(ctx, "eck-*")
		if err != nil {
			return reconcile.Result{}, err
		}
		activeAPIKeys = crossClusterAPIKeys
	}

	// apiKeyReconciledRemoteClients is used to track all the client clusters for which API keys have already been reconciled.
	// This is used to garbage collect API keys for clusters which have been deleted and are not in expectedRemoteClusters.
	apiKeyReconciledRemoteClients := sets.New[types.NamespacedName]()

	// Main loop to:
	// 1. Create or update expected remote CA.
	// 2. Create or update API keys and keystores.
	for remoteClientKey, remoteClusterRefs := range expectedRemoteClients {
		// Get the remote/client Elasticsearch cluster associated with this local/reconciled cluster.
		remoteClient, err := getElasticsearchCluster(ctx, r.Client, remoteClientKey)
		if err != nil {
			if errors.IsNotFound(err) {
				// Remote client cluster does not exist, invalidate API keys for that client cluster.
				apiKeyReconciledRemoteClients.Insert(remoteClientKey.NamespacedName())
				results.WithResults(reconcileAPIKeys(ctx, r.Client, activeAPIKeys, remoteServer, nil, nil, esClient, r.keystoreProvider))
				continue
			}
			return reconcile.Result{}, err
		}
		log := log.WithValues(
			"remote_server_namespace", remoteServer.GetNamespace(),
			"remote_server", remoteServer.GetName(),
			"remote_client_namespace", remoteClient.GetNamespace(),
			"remote_client_name", remoteClient.GetName(),
		)
		accessAllowed, err := isRemoteClusterAssociationAllowed(ctx, r.accessReviewer, remoteServer, remoteClient, r.recorder)
		if err != nil {
			return reconcile.Result{}, err
		}
		// if the remote CA exists but isn't allowed anymore, it will be deleted next
		if !accessAllowed {
			// Remove from the expected remote cluster to clean up local keystore.
			delete(expectedRemoteClients, remoteClientKey)
			// Invalidate API keys for that client cluster.
			apiKeyReconciledRemoteClients.Insert(remoteClientKey.NamespacedName())
			results.WithResults(reconcileAPIKeys(ctx, r.Client, activeAPIKeys, remoteServer, remoteClient, nil, esClient, r.keystoreProvider))
			continue
		}
		delete(associatedRemoteCAs, remoteClientKey)
		results.WithResults(createOrUpdateCertificateAuthorities(ctx, r, remoteServer, remoteClient))
		if results.HasError() {
			return results.Aggregate()
		}

		// RCS2, first check that both the reconciled and the client clusters are compatible.
		clientClusterSupportsClusterAPIKeys, err := remoteClient.SupportsRemoteClusterAPIKeys()
		if err != nil {
			results.WithError(err)
			continue
		}

		if !clientClusterSupportsClusterAPIKeys.IsSet() {
			log.Info("Client cluster version is not available in status yet, skipping API keys reconciliation")
			continue
		}

		if !remoteServerSupportsClusterAPIKeys.IsSet() {
			log.Info("Cluster version is not available in status yet, skipping API keys reconciliation")
			continue
		}

		if clientClusterSupportsClusterAPIKeys.IsFalse() && remoteServerSupportsClusterAPIKeys.IsTrue() {
			err := fmt.Errorf("client cluster %s/%s is running version %s which does not support remote cluster keys", remoteClient.GetNamespace(), remoteClient.GetName(), remoteClient.GetVersion())
			log.Error(err, "cannot configure remote cluster settings")
			continue
		}
		// Reconcile the API Keys.
		apiKeyReconciledRemoteClients.Insert(remoteClientKey.NamespacedName())
		results.WithResults(reconcileAPIKeys(ctx, r.Client, activeAPIKeys, remoteServer, remoteClient, remoteClusterRefs, esClient, r.keystoreProvider))
	}

	if remoteServerSupportsClusterAPIKeys.IsTrue() { //nolint:nestif
		// **************************************************************
		// Delete orphaned API keys from clusters which have been deleted
		// **************************************************************
		for _, activeAPIKey := range activeAPIKeys.APIKeys {
			// Skip API keys managed by the autoops controller.
			if autoops.IsManagedByAutoOps(activeAPIKey.Metadata) {
				continue
			}
			clientCluster, err := activeAPIKey.GetElasticsearchName()
			if err != nil {
				results.WithError(err)
				continue
			}
			if _, exists := apiKeyReconciledRemoteClients[clientCluster]; exists {
				// API keys for that client cluster have already been reconciled, skip.
				continue
			}
			// This API key in the local cluster state belongs to an unknown cluster which is not expected and has not been reconciled.
			log.Info(fmt.Sprintf("Invalidating API key %s which belongs to unknown cluster %s", activeAPIKey.Name, clientCluster))
			results.WithError(esClient.InvalidateCrossClusterAPIKey(ctx, activeAPIKey.Name))
		}

		// *********************************************
		// Delete unexpected keys in the local keystore.
		// *********************************************
		expectedAliases := expectedAliases(remoteServer, expectedRemoteClients)
		apiKeyStore, err := r.keystoreProvider.ForCluster(ctx, log, remoteServer)
		if err != nil {
			return results.WithError(err).Aggregate()
		}

		for alias := range apiKeyStore.GetAliases() {
			if expectedAliases.Has(alias) {
				// Expected alias
				continue
			}
			// Unexpected
			log.Info(fmt.Sprintf("Removing unexpected remote API key %s", alias))
			apiKeyStore.Delete(alias)
		}
		results.WithResults(apiKeyStore.Save(ctx, r.Client, remoteServer))
	}

	// Delete existing but not expected remote CA
	for toDelete := range associatedRemoteCAs {
		log.V(1).Info("Deleting remote CA",
			"local_namespace", remoteServer.GetNamespace(),
			"local_name", remoteServer.GetName(),
			"remote_namespace", toDelete.Namespace,
			"remote_name", toDelete.Name,
		)
		results.WithError(deleteCertificateAuthorities(ctx, r, remoteServerKey, toDelete))
	}
	return results.WithResult(association.RequeueRbacCheck(r.accessReviewer)).Aggregate()
}

// getElasticsearchCluster retrieves an Elasticsearch cluster by KindNamespacedName.
// It returns either a stateful Elasticsearch or stateless ElasticsearchStateless based on the Kind.
func getElasticsearchCluster(ctx context.Context, c k8s.Client, key commonv1.KindNamespacedName) (escommon.ElasticsearchCluster, error) {
	if key.IsStateless() {
		ess := &essv1alpha1.ElasticsearchStateless{}
		if err := c.Get(ctx, key.NamespacedName(), ess); err != nil {
			return nil, err
		}
		return ess, nil
	}
	es := &esv1.Elasticsearch{}
	if err := c.Get(ctx, key.NamespacedName(), es); err != nil {
		return nil, err
	}
	return es, nil
}

// hasEndpoints checks if the Elasticsearch cluster has available endpoints.
func hasEndpoints(ctx context.Context, c k8s.Client, es escommon.ElasticsearchCluster) bool {
	// For stateful clusters, use the existing URL provider
	if !es.IsStateless() {
		return services.NewElasticsearchURLProvider(es, c).HasEndpoints()
	}
	// For stateless clusters, check if there are running pods with the appropriate labels
	pods, err := k8s.PodsMatchingLabels(c, es.GetNamespace(), label.NewLabelSelectorForElasticsearch(es))
	if err != nil {
		return false
	}
	return len(k8s.RunningPods(pods)) > 0
}

func expectedAliases(
	localCluster escommon.ElasticsearchCluster,
	expectedRemoteCluster map[commonv1.KindNamespacedName][]escommon.RemoteCluster,
) sets.Set[string] {
	aliases := sets.New[string]()
	for _, remoteCluster := range localCluster.GetRemoteClusters() {
		esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(localCluster.GetNamespace())
		clientClusterKey := commonv1.KindNamespacedNameFromRef(esRef)
		if _, ok := expectedRemoteCluster[clientClusterKey]; !ok {
			// Not expected, might have been filtered by RBAC rules
			continue
		}
		if remoteCluster.APIKey == nil {
			// Not using remote cluster server.
			continue
		}
		aliases.Insert(remoteCluster.Name)
	}
	return aliases
}

func caCertMissingError(cluster types.NamespacedName) string {
	return fmt.Sprintf("Cannot find CA certificate cluster %s/%s", cluster.Namespace, cluster.Name)
}

// getExpectedRemoteClientsFor returns all the remote cluster keys for which a remote ca and an API Key should be created.
// The CA certificates must be copied from the remote cluster to the local one and vice versa.
// The API Key is created in the remote cluster and injected in the keystore of the local cluster.
// This function looks up both stateful Elasticsearch and stateless ElasticsearchStateless resources.
func getExpectedRemoteClientsFor(
	ctx context.Context,
	c k8s.Client,
	associatedEs escommon.ElasticsearchCluster,
) (map[commonv1.KindNamespacedName][]escommon.RemoteCluster, error) {
	span, _ := apm.StartSpan(ctx, "get_expected_remote_clusters", tracing.SpanTypeApp)
	defer span.End()
	expectedRemoteClusters := make(map[commonv1.KindNamespacedName][]escommon.RemoteCluster)

	// AddKey remote clusters declared in the Spec
	for _, remoteCluster := range associatedEs.GetRemoteClusters() {
		if !remoteCluster.ElasticsearchRef.IsDefined() {
			continue
		}
		esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(associatedEs.GetNamespace())
		kindKey := commonv1.KindNamespacedNameFromRef(esRef)
		expectedRemoteClusters[kindKey] = nil
	}

	// Seek for stateful Elasticsearch resources where this cluster is declared as a remote cluster
	var statefulList esv1.ElasticsearchList
	if err := c.List(ctx, &statefulList, &client.ListOptions{}); err != nil {
		return nil, err
	}
	for _, es := range statefulList.Items {
		es := es
		for _, remoteCluster := range es.Spec.RemoteClusters {
			if !remoteCluster.ElasticsearchRef.IsDefined() {
				continue
			}
			esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(es.Namespace)
			// Check if the reference points to the current cluster (considering kind)
			refKind := esRef.GetKindOrDefault(commonv1.ElasticsearchKind)
			currentKind := commonv1.ElasticsearchKind
			if associatedEs.IsStateless() {
				currentKind = commonv1.ElasticsearchStatelessKind
			}
			if esRef.Namespace == associatedEs.GetNamespace() &&
				esRef.Name == associatedEs.GetName() &&
				refKind == currentKind {
				clientKey := commonv1.KindNamespacedName{
					Kind:      commonv1.ElasticsearchKind,
					Namespace: es.Namespace,
					Name:      es.Name,
				}
				expectedRemoteClusters[clientKey] = append(expectedRemoteClusters[clientKey], remoteCluster)
			}
		}
	}

	// Seek for stateless ElasticsearchStateless resources where this cluster is declared as a remote cluster
	var statelessList essv1alpha1.ElasticsearchStatelessList
	if err := c.List(ctx, &statelessList, &client.ListOptions{}); err != nil {
		return nil, err
	}
	for _, ess := range statelessList.Items {
		ess := ess
		for _, remoteCluster := range ess.Spec.RemoteClusters {
			if !remoteCluster.ElasticsearchRef.IsDefined() {
				continue
			}
			esRef := remoteCluster.ElasticsearchRef.WithDefaultNamespace(ess.Namespace)
			// Check if the reference points to the current cluster (considering kind)
			refKind := esRef.GetKindOrDefault(commonv1.ElasticsearchKind)
			currentKind := commonv1.ElasticsearchKind
			if associatedEs.IsStateless() {
				currentKind = commonv1.ElasticsearchStatelessKind
			}
			if esRef.Namespace == associatedEs.GetNamespace() &&
				esRef.Name == associatedEs.GetName() &&
				refKind == currentKind {
				clientKey := commonv1.KindNamespacedName{
					Kind:      commonv1.ElasticsearchStatelessKind,
					Namespace: ess.Namespace,
					Name:      ess.Name,
				}
				expectedRemoteClusters[clientKey] = append(expectedRemoteClusters[clientKey], remoteCluster)
			}
		}
	}

	return expectedRemoteClusters, nil
}

// getAssociatedRemoteCAs returns for a given Elasticsearch cluster all the Elasticsearch keys for which
// the remote certificate authorities have been copied, i.e. all the other Elasticsearch clusters for which this cluster
// has been involved in a remote cluster association.
// In order to get all of them we:
// 1. List all the remote CA copied locally.
// 2. List all the other Elasticsearch clusters for which the CA of the given cluster has been copied.
// The returned map includes the Kind of each remote cluster, determined from the RemoteClusterKindLabelName label.
func getAssociatedRemoteCAs(
	ctx context.Context,
	c k8s.Client,
	es commonv1.KindNamespacedName,
) (map[commonv1.KindNamespacedName]struct{}, error) {
	span, _ := apm.StartSpan(ctx, "get_current_remote_ca", tracing.SpanTypeApp)
	defer span.End()

	currentRemoteClusters := make(map[commonv1.KindNamespacedName]struct{})

	// 1. Get clusters whose CA has been copied into the local namespace.
	var remoteCAList corev1.SecretList
	if err := c.List(ctx,
		&remoteCAList,
		client.InNamespace(es.Namespace),
		remoteca.Labels(es.Name, es.IsStateless()),
	); err != nil {
		return nil, err
	}
	for _, remoteCA := range remoteCAList.Items {
		remoteNs := remoteCA.Labels[RemoteClusterNamespaceLabelName]
		remoteEs := remoteCA.Labels[RemoteClusterNameLabelName]
		remoteKind := kindLabelToKind(remoteCA.Labels[RemoteClusterKindLabelName])
		currentRemoteClusters[commonv1.KindNamespacedName{
			Kind:      remoteKind,
			Namespace: remoteNs,
			Name:      remoteEs,
		}] = struct{}{}
	}

	// 2. Get clusters for which the CA of the local cluster has been copied.
	// Filter by remote cluster namespace, name, and kind to only get CAs for this specific cluster.
	if err := c.List(ctx,
		&remoteCAList,
		client.MatchingLabels(map[string]string{
			commonv1.TypeLabelName:          remoteca.TypeLabelValue,
			RemoteClusterNamespaceLabelName: es.Namespace,
			RemoteClusterNameLabelName:      es.Name,
			RemoteClusterKindLabelName:      kindToKindLabel(es.Kind),
		}),
	); err != nil {
		return nil, err
	}
	for _, remoteCA := range remoteCAList.Items {
		// Determine the owner cluster's kind from ClusterKindLabelName and read the appropriate name label
		ownerKind := kindLabelToKind(remoteCA.Labels[ClusterKindLabelName])
		var ownerName string
		if ownerKind == commonv1.ElasticsearchStatelessKind {
			ownerName = remoteCA.Labels[label.StatelessClusterNameLabelName]
		} else {
			ownerName = remoteCA.Labels[label.ClusterNameLabelName]
		}
		if ownerName != "" {
			currentRemoteClusters[commonv1.KindNamespacedName{
				Kind:      ownerKind,
				Namespace: remoteCA.Namespace,
				Name:      ownerName,
			}] = struct{}{}
		}
	}

	return currentRemoteClusters, nil
}

// kindLabelToKind converts a kind label value to the corresponding Kind constant.
// Defaults to ElasticsearchKind for backward compatibility with secrets that don't have the label.
func kindLabelToKind(kindLabel string) string {
	if kindLabel == kindStateless {
		return commonv1.ElasticsearchStatelessKind
	}
	return commonv1.ElasticsearchKind
}

// kindToKindLabel converts a Kind constant to the corresponding label value.
func kindToKindLabel(kind string) string {
	if kind == commonv1.ElasticsearchStatelessKind {
		return kindStateless
	}
	return kindStateful
}
