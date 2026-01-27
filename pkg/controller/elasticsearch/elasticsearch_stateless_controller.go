// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package elasticsearch

import (
	"context"
	"reflect"
	"sync/atomic"

	pkgerrors "github.com/pkg/errors"
	"go.elastic.co/apm/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/finalizer"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	commonversion "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/certificates/transport"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/driver/stateless"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/observer"
	esreconcile "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/reconcile"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/user"
	esversion "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/maps"
)

const statelessControllerName = "elasticsearch-stateless-controller"

// AddStateless creates a new ElasticsearchStateless Controller and adds it to the Manager with default RBAC.
// The Manager will set fields on the Controller and Start it when the Manager is Started.
func AddStateless(mgr manager.Manager, params operator.Parameters) error {
	reconciler := newStatelessReconciler(mgr, params)
	c, err := common.NewController(mgr, statelessControllerName, reconciler, params)
	if err != nil {
		return err
	}
	return addStatelessWatches(mgr, c, reconciler)
}

// newStatelessReconciler returns a new reconcile.Reconciler
func newStatelessReconciler(mgr manager.Manager, params operator.Parameters) *ReconcileElasticsearchStateless {
	client := mgr.GetClient()
	return &ReconcileElasticsearchStateless{
		Client:         client,
		recorder:       mgr.GetEventRecorderFor(statelessControllerName),
		licenseChecker: license.NewLicenseChecker(client, params.OperatorNamespace),
		esObservers:    observer.NewManager(params.ElasticsearchObservationInterval, params.Tracer),

		dynamicWatches: watches.NewDynamicWatches(),
		expectations:   expectations.NewClustersExpectations(client, &appsv1.Deployment{}),

		Parameters: params,
	}
}

func addStatelessWatches(mgr manager.Manager, c controller.Controller, r *ReconcileElasticsearchStateless) error {
	// Watch for changes to ElasticsearchStateless
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &essv1alpha1.ElasticsearchStateless{}, &handler.TypedEnqueueRequestForObject[*essv1alpha1.ElasticsearchStateless]{})); err != nil {
		return err
	}

	// Watch Deployments (instead of StatefulSets for stateful)
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &appsv1.Deployment{}, handler.TypedEnqueueRequestForOwner[*appsv1.Deployment](mgr.GetScheme(), mgr.GetRESTMapper(), &essv1alpha1.ElasticsearchStateless{}, handler.OnlyControllerOwner()))); err != nil {
		return err
	}

	// Watch pods belonging to ES clusters
	if err := watches.WatchPods(mgr, c, label.StatelessClusterNameLabelName); err != nil {
		return err
	}

	// Watch services
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &corev1.Service{}, handler.TypedEnqueueRequestForOwner[*corev1.Service](mgr.GetScheme(), mgr.GetRESTMapper(), &essv1alpha1.ElasticsearchStateless{}, handler.OnlyControllerOwner()))); err != nil {
		return err
	}

	// Watch config maps for dynamic watches (currently used for additional CAs trust)
	if err := c.Watch(source.Kind(mgr.GetCache(), &corev1.ConfigMap{}, r.dynamicWatches.ConfigMaps)); err != nil {
		return err
	}

	// Watch PodDisruptionBudgets
	if err := c.Watch(
		source.Kind(mgr.GetCache(), &policyv1.PodDisruptionBudget{}, handler.TypedEnqueueRequestForOwner[*policyv1.PodDisruptionBudget](mgr.GetScheme(), mgr.GetRESTMapper(), &essv1alpha1.ElasticsearchStateless{}, handler.OnlyControllerOwner()))); err != nil {
		return err
	}

	// Watch owned and soft-owned secrets
	if err := c.Watch(source.Kind(mgr.GetCache(), &corev1.Secret{}, r.dynamicWatches.Secrets)); err != nil {
		return err
	}
	if err := r.dynamicWatches.Secrets.AddHandler(&watches.OwnerWatch[*corev1.Secret]{
		IsController: true,
		OwnerType:    &essv1alpha1.ElasticsearchStateless{},
		Scheme:       mgr.GetScheme(),
		Mapper:       mgr.GetRESTMapper(),
	},
	); err != nil {
		return err
	}
	if err := watches.WatchSoftOwnedSecrets(mgr, c, essv1alpha1.Kind); err != nil {
		return err
	}

	// Trigger a reconciliation when observers report a cluster health change
	return c.Watch(observer.WatchClusterHealthChange(r.esObservers))
}

var _ reconcile.Reconciler = &ReconcileElasticsearchStateless{}

// ReconcileElasticsearchStateless reconciles an ElasticsearchStateless object
type ReconcileElasticsearchStateless struct {
	k8s.Client
	operator.Parameters
	recorder       record.EventRecorder
	licenseChecker license.Checker

	esObservers *observer.Manager

	dynamicWatches watches.DynamicWatches

	// expectations help dealing with inconsistencies in our client cache,
	// by marking resources updates as expected, and skipping some operations if the cache is not up-to-date.
	expectations *expectations.ClustersExpectation

	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
}

// Reconcile reads the state of the cluster for an ElasticsearchStateless object and makes changes based on the state read
// and what is in the ElasticsearchStateless.Spec
func (r *ReconcileElasticsearchStateless) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = common.NewReconciliationContext(ctx, &r.iteration, r.Tracer, statelessControllerName, "ess_name", request)
	defer common.LogReconciliationRun(ulog.FromContext(ctx))()
	defer tracing.EndContextTransaction(ctx)

	log := ulog.FromContext(ctx)
	// Fetch the ElasticsearchStateless instance
	var ess essv1alpha1.ElasticsearchStateless
	requeue, err := r.fetchElasticsearchStatelessWithAssociations(ctx, request, &ess)
	if err != nil || requeue {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if common.IsUnmanaged(ctx, &ess) {
		log.Info("Object is currently not managed by this controller. Skipping reconciliation", "namespace", ess.Namespace, "ess_name", ess.Name)
		return reconcile.Result{}, nil
	}

	// Remove any previous Finalizers
	if err := finalizer.RemoveAll(ctx, r.Client, &ess); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	state, err := esreconcile.NewState(&ess)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// ReconciliationComplete is initially set to True until another condition with the same type is reported.
	state.ReportCondition(essv1alpha1.ReconciliationComplete, corev1.ConditionTrue, "")

	results := r.internalReconcile(ctx, ess, state)

	// Update orchestration related annotations
	if err := r.annotateResource(ctx, ess, state); err != nil {
		if apierrors.IsConflict(err) {
			log.V(1).Info("Conflict while updating annotations", "namespace", ess.Namespace, "ess_name", ess.Name)
			results.WithReconciliationState(reconciler.Requeue.WithReason("Conflict while updating annotations"))
		} else {
			log.Error(err, "Error while updating annotations", "namespace", ess.Namespace, "ess_name", ess.Name)
			results.WithError(err)
			k8s.MaybeEmitErrorEvent(r.recorder, err, &ess, events.EventReconciliationError, "Reconciliation error: %v", err)
		}
	}

	if isReconciled, message := results.IsReconciled(); !isReconciled {
		state.UpdateWithPhase(essv1alpha1.ElasticsearchApplyingChangesPhase)
		state.ReportCondition(essv1alpha1.ReconciliationComplete, corev1.ConditionFalse, message)
	} else {
		state.UpdateWithPhase(essv1alpha1.ElasticsearchReadyPhase)
	}

	// Last step of the reconciliation loop is always to update the ElasticsearchStateless resource status.
	err = r.updateStatus(ctx, ess, state)
	if err != nil {
		if apierrors.IsConflict(err) {
			log.V(1).Info("Conflict while updating status", "namespace", ess.Namespace, "ess_name", ess.Name)
			return reconcile.Result{RequeueAfter: reconciler.DefaultRequeue}, nil
		}
		k8s.MaybeEmitErrorEvent(r.recorder, err, &ess, events.EventReconciliationError, "Reconciliation error: %v", err)
	}
	return results.WithError(err).Aggregate()
}

func (r *ReconcileElasticsearchStateless) fetchElasticsearchStatelessWithAssociations(ctx context.Context, request reconcile.Request, ess *essv1alpha1.ElasticsearchStateless) (bool, error) {
	span, ctx := apm.StartSpan(ctx, "fetch_elasticsearch_stateless", tracing.SpanTypeApp)
	defer span.End()

	if err := r.Client.Get(ctx, request.NamespacedName, ess); err != nil {
		if apierrors.IsNotFound(err) {
			// Object not found, cleanup in-memory state. Children resources are garbage-collected either by
			// the operator (see `onDelete`), either by k8s through the ownerReference mechanism.
			return true, r.onDelete(ctx,
				types.NamespacedName{
					Namespace: request.Namespace,
					Name:      request.Name,
				})
		}
		// Error reading the object - requeue the request.
		return true, err
	}
	return false, nil
}

func (r *ReconcileElasticsearchStateless) internalReconcile(
	ctx context.Context,
	ess essv1alpha1.ElasticsearchStateless,
	reconcileState *esreconcile.State,
) *reconciler.Results {
	results := reconciler.NewResult(ctx)
	log := log.FromContext(ctx)
	if ess.IsMarkedForDeletion() {
		// resource will be deleted, nothing to reconcile
		return results.WithError(r.onDelete(ctx, k8s.ExtractNamespacedName(&ess)))
	}

	span, ctx := apm.StartSpan(ctx, "validate", tracing.SpanTypeApp)
	// TODO: Add validation for stateless Elasticsearch
	// _, err := validation.ValidateElasticsearchStateless(ctx, ess, r.licenseChecker, r.ExposedNodeLabels)
	span.End()

	ver, err := commonversion.Parse(ess.Spec.Version)
	if err != nil {
		return results.WithError(err)
	}
	supported := esversion.SupportedVersions(ver)
	if supported == nil {
		return results.WithError(pkgerrors.Errorf("unsupported version: %s", ver))
	}

	// Validation warnings
	// TODO: Add warning checks for stateless
	_ = log

	return stateless.NewDriver(driver.Parameters{
		OperatorParameters: r.Parameters,
		ES:                 &ess,
		ReconcileState:     reconcileState,
		Client:             r.Client,
		Recorder:           r.recorder,
		Version:            ver,
		Expectations:       r.expectations.ForCluster(k8s.ExtractNamespacedName(&ess)),
		Observers:          r.esObservers,
		DynamicWatches:     r.dynamicWatches,
		SupportedVersions:  *supported,
		LicenseChecker:     r.licenseChecker,
	}, ess).Reconcile(ctx)
}

func (r *ReconcileElasticsearchStateless) updateStatus(
	ctx context.Context,
	ess essv1alpha1.ElasticsearchStateless,
	reconcileState *esreconcile.State,
) error {
	defer tracing.Span(&ctx)()
	log := ulog.FromContext(ctx)

	events, cluster := reconcileState.Apply()
	for _, evt := range events {
		log.V(1).Info("Recording event", "event", evt)
		r.recorder.Event(&ess, evt.EventType, evt.Reason, evt.Message)
	}
	if cluster == nil {
		return nil
	}
	log.V(1).Info("Updating status",
		"iteration", atomic.LoadUint64(&r.iteration),
		"namespace", ess.Namespace,
		"ess_name", ess.Name,
	)
	return common.UpdateStatus(ctx, r.Client, cluster)
}

// annotateResource adds the orchestration hints annotation to the ElasticsearchStateless resource.
func (r *ReconcileElasticsearchStateless) annotateResource(
	ctx context.Context,
	ess essv1alpha1.ElasticsearchStateless,
	reconcileState *esreconcile.State,
) error {
	span, _ := apm.StartSpan(ctx, "update_hints_annotations", tracing.SpanTypeApp)
	defer span.End()

	log := ulog.FromContext(ctx)

	newAnnotations, err := reconcileState.OrchestrationHints().AsAnnotation()
	if err != nil {
		return err
	}

	expected := maps.Merge(ess.ObjectMeta.DeepCopy().Annotations, newAnnotations)
	if reflect.DeepEqual(expected, ess.Annotations) {
		log.V(1).Info("Skipping annotation update", "ess_name", ess.Name, "namespace", ess.Namespace)
		return nil
	}
	ess.SetAnnotations(expected)
	return r.Update(ctx, &ess)
}

// onDelete garbage collect resources when an ElasticsearchStateless cluster is deleted
func (r *ReconcileElasticsearchStateless) onDelete(ctx context.Context, ess types.NamespacedName) error {
	r.expectations.RemoveCluster(ess)
	r.esObservers.StopObserving(ess)
	r.dynamicWatches.Secrets.RemoveHandlerForKey(keystore.SecureSettingsWatchName(ess))
	r.dynamicWatches.Secrets.RemoveHandlerForKey(certificates.CertificateWatchKey(essv1alpha1.ESSNamer, ess.Name))
	r.dynamicWatches.Secrets.RemoveHandlerForKey(transport.CustomTransportCertsWatchKey(ess))
	r.dynamicWatches.Secrets.RemoveHandlerForKey(user.UserProvidedRolesWatchName(ess))
	r.dynamicWatches.Secrets.RemoveHandlerForKey(user.UserProvidedFileRealmWatchName(ess))
	r.dynamicWatches.ConfigMaps.RemoveHandlerForKey(transport.AdditionalCAWatchKey(ess))
	return reconciler.GarbageCollectSoftOwnedSecrets(ctx, r.Client, ess, essv1alpha1.Kind)
}
