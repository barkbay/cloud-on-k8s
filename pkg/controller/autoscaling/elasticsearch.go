// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

const (
	name = "elasticsearch-autoscaling"
)

var log = logf.Log.WithName(name)

// ReconcileElasticsearch reconciles autoscaling policies and Elasticsearch specifications based on autoscaling decisions.
type ReconcileElasticsearch struct {
	k8s.Client
	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
}

func (r *ReconcileElasticsearch) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	defer common.LogReconciliationRun(log, request, "es_name", &r.iteration)()
	results := r.reconcileInternal(request)
	current, err := results.Aggregate()
	log.V(1).Info("Reconcile result", "requeue", current.Requeue, "requeueAfter", current.RequeueAfter)
	return current, err
}

// Add creates a new EnterpriseLicense Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, p operator.Parameters) error {
	r := newReconciler(mgr, p)
	c, err := common.NewController(mgr, name, r, p)
	if err != nil {
		return err
	}
	// Watch for changes on Elasticsearch clusters.
	if err := c.Watch(
		&source.Kind{Type: &esv1.Elasticsearch{}}, &handler.EnqueueRequestForObject{},
	); err != nil {
		return err
	}
	return nil
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, params operator.Parameters) *ReconcileElasticsearch {
	c := k8s.WrapClient(mgr.GetClient())
	return &ReconcileElasticsearch{
		Client: c,
	}
}

func (r *ReconcileElasticsearch) reconcileInternal(request reconcile.Request) *reconciler.Results {
	res := &reconciler.Results{}

	return res
}
