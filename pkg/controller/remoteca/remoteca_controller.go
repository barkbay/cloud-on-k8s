// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package remoteca

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/elastic/cloud-on-k8s/pkg/utils/rbac"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/certificates/transport"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/pkg/utils/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
)

const (
	name = "remotecluster-controller"

	EventReasonLocalCaCertNotFound = "LocalClusterCaNotFound"
	EventReasonRemoteCACertMissing = "RemoteClusterCaNotFound"
	CaCertMissingError             = "Cannot find CA certificate for %s cluster %s/%s"
)

var (
	log            = logf.Log.WithName(name)
	defaultRequeue = reconcile.Result{Requeue: true, RequeueAfter: 20 * time.Second}
)

// Add creates a new RemoteCluster Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, accessReviewer rbac.AccessReviewer, parameter operator.Parameters) error {
	r := newReconciler(mgr, accessReviewer, parameter.OperatorNamespace)
	c, err := add(mgr, r)
	if err != nil {
		return err
	}
	return addWatches(c, r)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, accessReviewer rbac.AccessReviewer, operatorNs string) *ReconcileRemoteCA {
	c := k8s.WrapClient(mgr.GetClient())
	return &ReconcileRemoteCA{
		Client:         c,
		accessReviewer: accessReviewer,
		scheme:         mgr.GetScheme(),
		watches:        watches.NewDynamicWatches(),
		recorder:       mgr.GetEventRecorderFor(name),
		licenseChecker: license.NewLicenseChecker(c, operatorNs),
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) (controller.Controller, error) {
	// Create a new controller
	c, err := controller.New(name, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return c, err
	}
	return c, nil
}

var _ reconcile.Reconciler = &ReconcileRemoteCA{}

// ReconcileRemoteCA reconciles a RemoteCluster object.
type ReconcileRemoteCA struct {
	k8s.Client
	accessReviewer rbac.AccessReviewer
	scheme         *runtime.Scheme
	recorder       record.EventRecorder
	watches        watches.DynamicWatches
	licenseChecker license.Checker

	// iteration is the number of times this controller has run its Reconcile method
	iteration int64
}

// Reconcile reads that state of the cluster for a RemoteCluster object and makes changes based on the state read
// and what is in the RemoteCluster.Spec
func (r *ReconcileRemoteCA) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// atomically update the iteration to support concurrent runs.
	currentIteration := atomic.AddInt64(&r.iteration, 1)
	iterationStartTime := time.Now()
	log.Info("Start reconcile iteration", "iteration", currentIteration)
	defer func() {
		log.Info("End reconcile iteration", "iteration", currentIteration, "took", time.Since(iterationStartTime))
	}()

	// Fetch the local Elasticsearch spec
	instance := esv1.Elasticsearch{}
	err := r.Get(request.NamespacedName, &instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// nothing to do
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if common.IsPaused(instance.ObjectMeta) {
		log.Info("Paused : skipping reconciliation", "iteration", currentIteration)
		return common.PauseRequeue, nil
	}

	enabled, err := r.licenseChecker.EnterpriseFeaturesEnabled()
	if err != nil {
		return defaultRequeue, err
	}
	if !enabled {
		log.Info(
			"Remote cluster controller is an enterprise feature. Enterprise features are disabled",
			"iteration", currentIteration,
		)
		return reconcile.Result{}, nil
	}

	// Use the driver to create the remote cluster
	return doReconcile(r, &instance)
}

func doReconcile(
	r *ReconcileRemoteCA,
	localEs *esv1.Elasticsearch,
) (reconcile.Result, error) {

	// Get current clusters according to the existing remote CAs
	currentRemoteClusters, err := getCurrentRemoteCa(r.Client, localEs)
	if err != nil {
		return reconcile.Result{}, err
	}

	expectedRemoteClusters, err := getExpectedRemoteClusters(r.Client, localEs)
	if err != nil {
		return reconcile.Result{}, err
	}

	results := &reconciler.Results{}
	// Create or update expected remote CA
	for remoteCluster := range expectedRemoteClusters {
		delete(currentRemoteClusters, remoteCluster)
		// Get the remote Elasticsearch cluster
		remoteEs := &esv1.Elasticsearch{}
		if err := r.Client.Get(remoteCluster, remoteEs); err != nil {
			if !errors.IsNotFound(err) {
				return reconcile.Result{}, err
			}
			// Remote cluster does not exist, skip it
			continue
		}
		results.WithResults(createOrUpdateCertificateAuthorities(r, localEs, remoteEs))
		if results.HasError() {
			return results.Aggregate()
		}
	}

	// Delete existing but not expected remote CA
	localClusterKey := k8s.ExtractNamespacedName(localEs)
	for toDelete := range currentRemoteClusters {
		log.V(1).Info("Delete remote CA",
			"localNamespace", localEs.Namespace,
			"localName", localEs.Name,
			"remoteNamespace", toDelete.Namespace,
			"remoteName", toDelete.Name,
		)
		results.WithError(deleteCertificateAuthorities(r, localClusterKey, toDelete))
	}

	return results.Aggregate()
}

func deleteCertificateAuthorities(
	r *ReconcileRemoteCA,
	local, remote types.NamespacedName,
) error {
	// Delete local secret
	if err := r.Client.Delete(&v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: local.Namespace,
			Name:      remoteCASecretName(local.Name, remote),
		},
	}); err != nil && !errors.IsNotFound(err) {
		return err
	}
	// Delete remote secret
	if err := r.Client.Delete(&v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: remote.Namespace,
			Name:      remoteCASecretName(remote.Name, local),
		},
	}); err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Remove watches
	r.watches.Secrets.RemoveHandlerForKey(watchName(local, remote))
	r.watches.Secrets.RemoveHandlerForKey(watchName(remote, local))

	return nil
}

// createOrUpdateCertificateAuthorities creates the Secrets that contains the remote certificate authorities
// A boolean is returned to check if the a new reconcile loop should be scheduled.
func createOrUpdateCertificateAuthorities(
	r *ReconcileRemoteCA,
	local, remote *esv1.Elasticsearch,
) *reconciler.Results {

	results := &reconciler.Results{}
	// Add watches on the CA secret of the local cluster.
	localClusterKey := k8s.ExtractNamespacedName(local)
	remoteClusterKey := k8s.ExtractNamespacedName(remote)
	if err := addCertificatesAuthorityWatches(r, remoteClusterKey, localClusterKey); err != nil {
		return results.WithError(err)
	}

	// Add watches on the CA secret of the remote cluster.
	if err := addCertificatesAuthorityWatches(r, localClusterKey, remoteClusterKey); err != nil {
		return results.WithError(err)
	}

	log.V(1).Info(
		"Setting up remote cluster",
		"local_namespace", localClusterKey.Namespace,
		"local_name", localClusterKey.Namespace,
		"remote_namespace", remote.Namespace,
		"remote_name", remote.Name,
	)

	// Check if local CA exists
	localCA := &v1.Secret{}
	if err := r.Client.Get(transport.PublicCertsSecretRef(localClusterKey), localCA); err != nil {
		if !errors.IsNotFound(err) {
			return results.WithError(err)
		}
		results.WithResult(defaultRequeue)
	}

	if len(localCA.Data[certificates.CAFileName]) == 0 {
		message := caCertMissingError("local", localClusterKey)
		log.Error(fmt.Errorf("cannot find local Ca cert"), message)
		r.recorder.Event(local, v1.EventTypeWarning, EventReasonLocalCaCertNotFound, message)
		// CA secrets are watched, we don't need to requeue.
		// If CA is created later it will trigger a new reconciliation.
		return nil
	}

	// Check if remote CA exists
	remoteCA := &v1.Secret{}
	if err := r.Client.Get(transport.PublicCertsSecretRef(remoteClusterKey), remoteCA); err != nil {
		if !errors.IsNotFound(err) {
			return results.WithError(err)
		}
		results.WithResult(defaultRequeue)
	}

	// Check if remote CA exists
	if len(remoteCA.Data[certificates.CAFileName]) == 0 {
		message := caCertMissingError("remote", remoteClusterKey)
		log.Error(fmt.Errorf("cannot find remote Ca cert"), message)
		r.recorder.Event(local, v1.EventTypeWarning, EventReasonRemoteCACertMissing, message)
		return nil
	}

	// Create local relationship
	if err := reconcileRemoteCA(r.Client, local, remoteClusterKey, remoteCA.Data[certificates.CAFileName]); err != nil {
		return results.WithError(err)
	}

	// Create remote relationship
	if err := reconcileRemoteCA(r.Client, remote, localClusterKey, localCA.Data[certificates.CAFileName]); err != nil {
		return results.WithError(err)
	}

	return nil
}

// reconcileRemoteCA copies certificates authorities across 2 clusters
func reconcileRemoteCA(
	c k8s.Client,
	owner *esv1.Elasticsearch,
	remote types.NamespacedName,
	remoteCA []byte,
) error {

	// Define the desired remote CA object, it lives in the remote namespace.
	expected := v1.Secret{
		ObjectMeta: remoteCAObjectMeta(remoteCASecretName(owner.Name, remote), owner, remote),
		Data: map[string][]byte{
			certificates.CAFileName: remoteCA,
		},
	}

	var reconciled v1.Secret
	return reconciler.ReconcileResource(reconciler.Params{
		Client:     c,
		Scheme:     scheme.Scheme,
		Owner:      owner,
		Expected:   &expected,
		Reconciled: &reconciled,
		NeedsUpdate: func() bool {
			return !maps.IsSubset(expected.Labels, reconciled.Labels) || !reflect.DeepEqual(expected.Data, reconciled.Data)
		},
		UpdateReconciled: func() {
			reconciled.Labels = maps.Merge(reconciled.Labels, expected.Labels)
			reconciled.Data = expected.Data
		},
	})
}

func caCertMissingError(location string, cluster types.NamespacedName) string {
	return fmt.Sprintf(
		CaCertMissingError,
		location,
		cluster.Namespace,
		cluster.Name,
	)
}

// getExpectedRemoteCa returns all the remote clusters for which a remote ca should created
func getExpectedRemoteClusters(c k8s.Client, associated *esv1.Elasticsearch) (map[types.NamespacedName]struct{}, error) {
	expectedRemoteClusters := make(map[types.NamespacedName]struct{})

	// Add remote clusters declared in the Spec
	for _, remoteCluster := range associated.Spec.RemoteClusters.K8sLocal {
		expectedRemoteClusters[remoteCluster.NamespacedName()] = struct{}{}
	}

	var list esv1.ElasticsearchList
	if err := c.List(&list, &client.ListOptions{}); err != nil {
		return nil, err
	}

	// Seek for Elasticsearch resources where this cluster is declared as a remote cluster
	for _, es := range list.Items {
		for _, remoteCluster := range es.Spec.RemoteClusters.K8sLocal {
			if remoteCluster.Namespace == associated.Namespace && remoteCluster.Name == associated.Name {
				expectedRemoteClusters[k8s.ExtractNamespacedName(&es)] = struct{}{}
			}
		}
	}

	return expectedRemoteClusters, nil
}

// getCurrentRemoteCa returns all the remote clusters for which a remote ca has been created
func getCurrentRemoteCa(c k8s.Client, es *esv1.Elasticsearch) (map[types.NamespacedName]struct{}, error) {
	remoteCAs := make(map[types.NamespacedName]struct{})

	// Get the remoteCA in the current namespace
	var remoteCAList v1.SecretList
	if err := c.List(
		&remoteCAList,
		client.InNamespace(es.Namespace),
		GetRemoteCAMatchingLabel(es.Name),
	); err != nil {
		return nil, err
	}
	for _, remoteCA := range remoteCAList.Items {
		if remoteCA.Labels == nil {
			continue
		}
		remoteNs := remoteCA.Labels[RemoteClusterNamespaceLabelName]
		remoteEs := remoteCA.Labels[RemoteClusterNameLabelName]
		remoteCAs[types.NamespacedName{
			Namespace: remoteNs,
			Name:      remoteEs,
		}] = struct{}{}
	}

	// Get the remoteCA where this cluster is involved in other namespaces
	if err := c.List(
		&remoteCAList,
		client.MatchingLabels(map[string]string{
			common.TypeLabelName:            TypeLabelValue,
			RemoteClusterNamespaceLabelName: es.Namespace,
			RemoteClusterNameLabelName:      es.Name,
		}),
	); err != nil {
		return nil, err
	}
	for _, remoteCA := range remoteCAList.Items {
		if remoteCA.Labels == nil {
			continue
		}
		remoteEs := remoteCA.Labels[label.ClusterNameLabelName]
		remoteCAs[types.NamespacedName{
			Namespace: remoteCA.Namespace,
			Name:      remoteEs,
		}] = struct{}{}
	}

	return remoteCAs, nil
}
