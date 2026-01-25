// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package license

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

const (
	name = "license-controller"
)

// Reconcile reads the cluster license for the cluster being reconciled. If found, it checks whether it is still valid.
// If there is none it assigns a new one.
// In any case it schedules a new reconcile request to be processed when the license is about to expire.
// This happens independently from any watch triggered reconcile request.
func (r *ReconcileLicenses) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = common.NewReconciliationContext(ctx, &r.iteration, r.Tracer, name, "es_name", request)
	defer common.LogReconciliationRun(ulog.FromContext(ctx))()
	defer tracing.EndContextTransaction(ctx)

	results := r.reconcileInternal(ctx, request)
	current, err := results.Aggregate()
	ulog.FromContext(ctx).V(1).Info("Reconcile result", "requeueAfter", current.RequeueAfter)
	return current, err
}

// AddStateful creates a new EnterpriseLicense Controller for stateful Elasticsearch and adds it to the Manager with default RBAC.
// The Manager will set fields on the Controller and Start it when the Manager is Started.
func AddStateful(mgr manager.Manager, p operator.Parameters) error {
	r := newReconciler(mgr, p)
	c, err := common.NewController(mgr, name, r, p)
	if err != nil {
		return err
	}
	return addStatefulWatches(mgr, c, r.Client)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, params operator.Parameters) *ReconcileLicenses {
	c := mgr.GetClient()
	return &ReconcileLicenses{
		Client:     c,
		Parameters: params,
		checker:    license.NewLicenseChecker(c, params.OperatorNamespace),
		recorder:   mgr.GetEventRecorderFor(name),
	}
}

// addStatefulWatches adds watches for stateful Elasticsearch resources
func addStatefulWatches(mgr manager.Manager, c controller.Controller, k8sClient k8s.Client) error {
	log := ulog.Log // no context available for contextual logging
	// Watch for changes to Elasticsearch clusters.
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &esv1.Elasticsearch{}, &handler.TypedEnqueueRequestForObject[*esv1.Elasticsearch]{})); err != nil {
		return err
	}

	if err := c.Watch(source.Kind(mgr.GetCache(), &corev1.Secret{},
		handler.TypedEnqueueRequestsFromMapFunc[*corev1.Secret](func(ctx context.Context, secret *corev1.Secret) []reconcile.Request {
			if !license.IsOperatorLicense(*secret) {
				return nil
			}

			// if a license is added/modified we want to update for potentially all clusters managed by this instance
			// of ECK which is why we are listing all Elasticsearch clusters here and trigger a reconciliation
			rs, err := reconcileRequestsForAllClusters(k8sClient, log)
			if err != nil {
				// dropping the event(s) at this point
				log.Error(err, "failed to list affected clusters in enterprise license watch")
				return nil
			}
			return rs
		}),
	)); err != nil {
		return err
	}
	return nil
}

var _ reconcile.Reconciler = &ReconcileLicenses{}

// ReconcileLicenses reconciles EnterpriseLicenses with existing Elasticsearch clusters and creates ClusterLicenses for them.
type ReconcileLicenses struct {
	k8s.Client
	operator.Parameters
	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
	checker   license.Checker
	recorder  record.EventRecorder
}

// reconcileClusterLicense upserts a cluster license in the namespace of the given Elasticsearch cluster.
// Returns time to next reconciliation, bool whether a license is configured at all and optional error.
func (r *ReconcileLicenses) reconcileClusterLicense(ctx context.Context, cluster esv1.Elasticsearch) (time.Time, bool, error) {
	log := ulog.FromContext(ctx)

	var noResult time.Time
	minVer, err := minVersion(r, &cluster)
	if err != nil {
		return noResult, true, err
	}
	matchingSpec, parent, found := findLicense(ctx, r, r.checker, r.recorder, minVer)
	if !found {
		// no matching license found, delete cluster level license if it exists to revert to basic
		clusterLicenseNSN := types.NamespacedName{Namespace: cluster.Namespace, Name: escommon.LicenseSecretName(&cluster)}
		log.V(1).Info("No enterprise license found. Attempting to remove cluster license secret", "namespace", cluster.Namespace, "es_name", cluster.Name)
		err := k8s.DeleteSecretIfExists(ctx, r.Client, clusterLicenseNSN)
		return noResult, false, err
	}
	log.V(1).Info("Found license for cluster", "eck_license", parent, "es_license", matchingSpec.UID, "license_type", matchingSpec.Type, "namespace", cluster.Namespace, "es_name", cluster.Name)
	// make sure the signature secret is created in the cluster's namespace
	if err := reconcileClusterLicenseSecret(ctx, r, &cluster, parent, matchingSpec); err != nil {
		return noResult, false, err
	}
	return matchingSpec.ExpiryTime(), false, nil
}

func (r *ReconcileLicenses) reconcileInternal(ctx context.Context, request reconcile.Request) *reconciler.Results {
	res := &reconciler.Results{}

	// Fetch the cluster to ensure it still exists
	cluster := esv1.Elasticsearch{}
	err := r.Get(ctx, request.NamespacedName, &cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// nothing to do no cluster
			return res
		}
		return res.WithError(err)
	}

	if !cluster.DeletionTimestamp.IsZero() {
		// cluster is being deleted nothing to do
		return res
	}

	newExpiry, noLicense, err := r.reconcileClusterLicense(ctx, cluster)
	if err != nil {
		return res.WithError(err)
	}
	margin := defaultSafetyMargin
	if noLicense {
		// don't apply safety margin if we don't have a license but use requested requeue time as specified in newExpiry
		margin = 0
	}
	return res.WithRequeue(nextReconcile(newExpiry, margin))
}
