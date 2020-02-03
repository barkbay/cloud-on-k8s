// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package remotecluster

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/certificates/transport"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/remotecluster"
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
func Add(mgr manager.Manager, parameter operator.Parameters) error {
	r := newReconciler(mgr, parameter.OperatorNamespace)
	c, err := add(mgr, r)
	if err != nil {
		return err
	}
	return addWatches(c, r)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, operatorNs string) *ReconcileRemoteCluster {
	c := k8s.WrapClient(mgr.GetClient())
	return &ReconcileRemoteCluster{
		Client:         c,
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

var _ reconcile.Reconciler = &ReconcileRemoteCluster{}

// ReconcileRemoteCluster reconciles a RemoteCluster object.
type ReconcileRemoteCluster struct {
	k8s.Client
	scheme              *runtime.Scheme
	recorder            record.EventRecorder
	watches             watches.DynamicWatches
	licenseChecker      license.Checker
	remoteClusterLister remotecluster.RemoteClusterLister

	// iteration is the number of times this controller has run its Reconcile method
	iteration int64
}

// Reconcile reads that state of the cluster for a RemoteCluster object and makes changes based on the state read
// and what is in the RemoteCluster.Spec
func (r *ReconcileRemoteCluster) Reconcile(request reconcile.Request) (reconcile.Result, error) {
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
	r *ReconcileRemoteCluster,
	localEs *esv1.Elasticsearch,
) (reconcile.Result, error) {

	// Get current clusters
	//currentRemoteClusters, _ := r.remoteClusterLister.GetRemoteClusters()
	expectedRemoteClusters := localEs.Spec.RemoteClusters.K8sLocal

	// TODO: Delete non existing expected

	results := &reconciler.Results{}
	for _, remoteCluster := range expectedRemoteClusters {
		// Get the remote Elasticsearch cluster
		remoteEs := &esv1.Elasticsearch{}
		if err := r.Client.Get(remoteCluster.NamespacedName(), remoteEs); err != nil {
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
	return results.Aggregate()
}

// createOrUpdateCertificateAuthorities creates the Secrets that contains the remote certificate authorities
// A boolean is returned to check if the a new reconcile loop should be scheduled.
func createOrUpdateCertificateAuthorities(
	r *ReconcileRemoteCluster,
	local, remote *esv1.Elasticsearch,
) *reconciler.Results {

	results := &reconciler.Results{}
	// Add watches on the CA secret of the local cluster.
	localClusterKey := k8s.ExtractNamespacedName(local)
	remoteClusterKey := k8s.ExtractNamespacedName(local)
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

	// Check if local CA exists
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
	remoteSubject := certificates.GetSubjectName(remote.Name, remote.Namespace)
	if err := reconcileTrustRelationShip(r.Client, local, remoteClusterKey, remoteCA.Data[certificates.CAFileName], remoteSubject); err != nil {
		return results.WithError(err)
	}

	// Create remote relationship
	localSubject := certificates.GetSubjectName(local.Name, local.Namespace)
	if err := reconcileTrustRelationShip(r.Client, remote, localClusterKey, localCA.Data[certificates.CAFileName], localSubject); err != nil {
		return results.WithError(err)
	}

	return nil
}

// reconcileTrustRelationShip creates a TrustRelationShip from a local cluster to a remote one.
func reconcileTrustRelationShip(
	c k8s.Client,
	owner *esv1.Elasticsearch,
	remote types.NamespacedName,
	remoteCA []byte,
	subjectName []string,
) error {

	log.V(1).Info( // TODO: Is it really necessary ?
		"Reconcile TrustRelationShip",
		"name", name,
		"local-namespace", owner.Namespace,
		"local-name", owner.Name,
		"remote-namespace", remote.Namespace,
		"remote-name", remote.Name,
	)

	// Subjectname is an array, convert it into a json string
	subjectNameAsJson, err := json.Marshal(subjectName)
	if err != nil {
		return err
	}

	// Define the desired TrustRelationship object, it lives in the remote namespace.
	expected := v1.Secret{
		ObjectMeta: trustRelationshipObjectMeta(name, owner, remote),
		Data: map[string][]byte{
			certificates.CAFileName: remoteCA,
			"subjectName":           subjectNameAsJson,
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
