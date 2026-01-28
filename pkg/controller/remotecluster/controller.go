// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package remotecluster

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common"
	commonesclient "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/esclient"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/remotecluster/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/rbac"
)

const (
	name          = "remotecluster-controller"
	nameStateless = "remotecluster-stateless-controller"

	EventReasonClusterCaCertNotFound = "ClusterCaCertNotFound"
)

// RemoteClustersReconciler is the interface that both stateful and stateless reconcilers implement.
// This interface is used by the watches package to access common fields.
type RemoteClustersReconciler interface {
	reconcile.Reconciler
	GetClient() k8s.Client
	GetWatches() watches.DynamicWatches
	GetRecorder() record.EventRecorder
}

// baseRemoteClustersReconciler contains shared fields and logic for both stateful and stateless reconcilers.
type baseRemoteClustersReconciler struct {
	k8s.Client
	operator.Parameters
	accessReviewer   rbac.AccessReviewer
	recorder         record.EventRecorder
	watches          watches.DynamicWatches
	licenseChecker   license.Checker
	esClientProvider commonesclient.Provider
	keystoreProvider *keystore.Provider
	iteration        uint64
}

// GetClient returns the k8s client.
func (r *baseRemoteClustersReconciler) GetClient() k8s.Client {
	return r.Client
}

// GetWatches returns the dynamic watches.
func (r *baseRemoteClustersReconciler) GetWatches() watches.DynamicWatches {
	return r.watches
}

// GetRecorder returns the event recorder.
func (r *baseRemoteClustersReconciler) GetRecorder() record.EventRecorder {
	return r.recorder
}

// ReconcileRemoteClustersStateful reconciles remote clusters for stateful Elasticsearch resources.
type ReconcileRemoteClustersStateful struct {
	baseRemoteClustersReconciler
}

var _ reconcile.Reconciler = &ReconcileRemoteClustersStateful{}
var _ RemoteClustersReconciler = &ReconcileRemoteClustersStateful{}

// ReconcileRemoteClustersStateless reconciles remote clusters for stateless ElasticsearchStateless resources.
type ReconcileRemoteClustersStateless struct {
	baseRemoteClustersReconciler
}

var _ reconcile.Reconciler = &ReconcileRemoteClustersStateless{}
var _ RemoteClustersReconciler = &ReconcileRemoteClustersStateless{}

// Add creates a new ReconcileRemoteClustersStateful Controller for stateful Elasticsearch and adds it to the manager with default RBAC.
func Add(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) error {
	r := NewReconcilerStateful(mgr, accessReviewer, params)
	c, err := common.NewController(mgr, name, r, params)
	if err != nil {
		return err
	}
	return addWatches(mgr, c, r)
}

// NewReconcilerStateful returns a new reconcile.Reconciler for stateful Elasticsearch.
func NewReconcilerStateful(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) *ReconcileRemoteClustersStateful {
	c := mgr.GetClient()
	return &ReconcileRemoteClustersStateful{
		baseRemoteClustersReconciler: baseRemoteClustersReconciler{
			Client:           c,
			accessReviewer:   accessReviewer,
			keystoreProvider: keystore.NewProvider(c),
			watches:          watches.NewDynamicWatches(),
			recorder:         mgr.GetEventRecorderFor(name),
			licenseChecker:   license.NewLicenseChecker(c, params.OperatorNamespace),
			Parameters:       params,
			esClientProvider: commonesclient.NewClient,
		},
	}
}

// AddStateless creates a new ReconcileRemoteClustersStateless Controller for stateless ElasticsearchStateless and adds it to the manager with default RBAC.
func AddStateless(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) error {
	r := NewReconcilerStateless(mgr, accessReviewer, params)
	c, err := common.NewController(mgr, nameStateless, r, params)
	if err != nil {
		return err
	}
	return addWatchesStateless(mgr, c, r)
}

// NewReconcilerStateless returns a new reconcile.Reconciler for stateless ElasticsearchStateless.
func NewReconcilerStateless(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) *ReconcileRemoteClustersStateless {
	c := mgr.GetClient()
	return &ReconcileRemoteClustersStateless{
		baseRemoteClustersReconciler: baseRemoteClustersReconciler{
			Client:           c,
			accessReviewer:   accessReviewer,
			keystoreProvider: keystore.NewProvider(c),
			watches:          watches.NewDynamicWatches(),
			recorder:         mgr.GetEventRecorderFor(nameStateless),
			licenseChecker:   license.NewLicenseChecker(c, params.OperatorNamespace),
			Parameters:       params,
			esClientProvider: commonesclient.NewClient,
		},
	}
}

// Reconcile reads that state of the cluster for the expected remote clusters in this Kubernetes cluster.
// It copies the remote CA Secrets so they can be trusted by every peer Elasticsearch clusters.
func (r *ReconcileRemoteClustersStateful) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = common.NewReconciliationContext(ctx, &r.iteration, r.Tracer, name, "es_name", request)
	defer common.LogReconciliationRun(ulog.FromContext(ctx))()
	defer tracing.EndContextTransaction(ctx)

	// Fetch the stateful Elasticsearch resource
	es := &esv1.Elasticsearch{}
	err := r.Get(ctx, request.NamespacedName, es)
	if err != nil {
		if errors.IsNotFound(err) {
			r.keystoreProvider.ForgetCluster(commonv1.KindNamespacedName{
				Kind:      commonv1.ElasticsearchKind,
				Namespace: request.Namespace,
				Name:      request.Name,
			})
			return deleteAllRemoteCa(ctx, &r.baseRemoteClustersReconciler, request.NamespacedName, false)
		}
		return reconcile.Result{}, err
	}

	if common.IsUnmanaged(ctx, es) {
		ulog.FromContext(ctx).Info("Object is currently not managed by this controller. Skipping reconciliation", "namespace", es.Namespace, "es_name", es.Name)
		return reconcile.Result{}, nil
	}
	return doReconcile(ctx, &r.baseRemoteClustersReconciler, es)
}

// Reconcile reads that state of the cluster for the expected remote clusters in this Kubernetes cluster.
// It copies the remote CA Secrets so they can be trusted by every peer Elasticsearch clusters.
func (r *ReconcileRemoteClustersStateless) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = common.NewReconciliationContext(ctx, &r.iteration, r.Tracer, nameStateless, "es_name", request)
	defer common.LogReconciliationRun(ulog.FromContext(ctx))()
	defer tracing.EndContextTransaction(ctx)

	// Fetch the stateless ElasticsearchStateless resource
	ess := &essv1alpha1.ElasticsearchStateless{}
	err := r.Get(ctx, request.NamespacedName, ess)
	if err != nil {
		if errors.IsNotFound(err) {
			r.keystoreProvider.ForgetCluster(commonv1.KindNamespacedName{
				Kind:      commonv1.ElasticsearchStatelessKind,
				Namespace: request.Namespace,
				Name:      request.Name,
			})
			return deleteAllRemoteCa(ctx, &r.baseRemoteClustersReconciler, request.NamespacedName, true)
		}
		return reconcile.Result{}, err
	}

	if common.IsUnmanaged(ctx, ess) {
		ulog.FromContext(ctx).Info("Object is currently not managed by this controller. Skipping reconciliation", "namespace", ess.Namespace, "es_name", ess.Name)
		return reconcile.Result{}, nil
	}
	return doReconcile(ctx, &r.baseRemoteClustersReconciler, ess)
}
