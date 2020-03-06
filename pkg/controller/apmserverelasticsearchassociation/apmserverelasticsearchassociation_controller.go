// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package apmserverelasticsearchassociation

import (
	"context"
	"reflect"
	"time"

	"github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/config"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/utils/rbac"
	"go.elastic.co/apm"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apmv1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	kbv1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/labels"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/user"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

const (
	name                        = "apm-es-association-controller"
	elasticsearchCASecretSuffix = "apm-es-ca" // nolint
	kibanaCASecretSuffix        = "apm-kb-ca" // nolint
)

var (
	log            = logf.Log.WithName(name)
	defaultRequeue = reconcile.Result{Requeue: true, RequeueAfter: 10 * time.Second}
)

// Add creates a new ApmServerElasticsearchAssociation Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) error {
	r := newReconciler(mgr, accessReviewer, params)
	c, err := add(mgr, r)
	if err != nil {
		return err
	}
	return addWatches(c, r)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) *ReconcileApmServerElasticsearchAssociation {
	client := k8s.WrapClient(mgr.GetClient())
	return &ReconcileApmServerElasticsearchAssociation{
		Client:         client,
		accessReviewer: accessReviewer,
		scheme:         mgr.GetScheme(),
		watches:        watches.NewDynamicWatches(),
		recorder:       mgr.GetEventRecorderFor(name),
		Parameters:     params,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) (controller.Controller, error) {
	// Create a new controller
	c, err := controller.New(name, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func addWatches(c controller.Controller, r *ReconcileApmServerElasticsearchAssociation) error {
	// Watch for changes to ApmServers
	if err := c.Watch(&source.Kind{Type: &apmv1.ApmServer{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// Watch Elasticsearch cluster objects
	if err := c.Watch(&source.Kind{Type: &esv1.Elasticsearch{}}, r.watches.ElasticsearchClusters); err != nil {
		return err
	}

	// Watch Kibana objects
	if err := c.Watch(&source.Kind{Type: &kbv1.Kibana{}}, r.watches.Kibanas); err != nil {
		return err
	}

	// Dynamically watch Elasticsearch public CA secrets for referenced ES clusters
	if err := c.Watch(&source.Kind{Type: &corev1.Secret{}}, r.watches.Secrets); err != nil {
		return err
	}

	// Watch Secrets owned by an ApmServer resource
	if err := c.Watch(&source.Kind{Type: &corev1.Secret{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &apmv1.ApmServer{},
		IsController: true,
	}); err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileApmServerElasticsearchAssociation{}

// ReconcileApmServerElasticsearchAssociation reconciles a ApmServerElasticsearchAssociation object
type ReconcileApmServerElasticsearchAssociation struct {
	k8s.Client
	accessReviewer rbac.AccessReviewer
	scheme         *runtime.Scheme
	recorder       record.EventRecorder
	watches        watches.DynamicWatches
	operator.Parameters
	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
}

func (r *ReconcileApmServerElasticsearchAssociation) onDelete(obj types.NamespacedName) error {
	// Clean up memory
	r.watches.Kibanas.RemoveHandlerForKey(kibanaWatchName(obj))
	r.watches.ElasticsearchClusters.RemoveHandlerForKey(elasticsearchWatchName(obj))
	r.watches.Secrets.RemoveHandlerForKey(esCAWatchName(obj))
	r.watches.Secrets.RemoveHandlerForKey(kibanaWatchName(obj))
	// Delete users
	return user.DeleteUser(r.Client, NewUserLabelSelector(obj))
}

// Reconcile reads that state of the cluster for a ApmServerElasticsearchAssociation object and makes changes based on the state read
// and what is in the ApmServerElasticsearchAssociation.Spec
func (r *ReconcileApmServerElasticsearchAssociation) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	defer common.LogReconciliationRun(log, request, "as_name", &r.iteration)()
	tx, ctx := tracing.NewTransaction(r.Tracer, request.NamespacedName, "apm-es-association")
	defer tracing.EndTransaction(tx)

	var apmServer apmv1.ApmServer
	configurationHelpers := config.ConfigurationHelpers(r.Client, &apmServer)
	if err := association.FetchWithAssociations(ctx, r.Client, request, &apmServer, configurationHelpers...); err != nil {
		if apierrors.IsNotFound(err) {
			// APM Server has been deleted, remove artifacts related to the association.
			return reconcile.Result{}, r.onDelete(types.NamespacedName{
				Namespace: request.Namespace,
				Name:      request.Name,
			})
		}
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if common.IsPaused(apmServer.ObjectMeta) {
		log.Info("Object is paused. Skipping reconciliation", "namespace", apmServer.Namespace, "as_name", apmServer.Name)
		return common.PauseRequeue, nil
	}

	// ApmServer is being deleted, short-circuit reconciliation and remove artifacts related to the association.
	if !apmServer.DeletionTimestamp.IsZero() {
		apmName := k8s.ExtractNamespacedName(&apmServer)
		return reconcile.Result{}, tracing.CaptureError(ctx, r.onDelete(apmName))
	}

	if compatible, err := r.isCompatible(ctx, &apmServer); err != nil || !compatible {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if err := annotation.UpdateControllerVersion(ctx, r.Client, &apmServer, r.OperatorInfo.BuildInfo.Version); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	results := reconciler.NewResult(ctx)
	var newStatus commonv1.AssociationStatus
	for _, associationResolver := range configurationHelpers {
		var err error
		switch resolver := associationResolver.(type) {
		case *config.EsAssociationConfigurationHelper:
			newStatus, err = r.reconcileEsAssociation(ctx, &apmServer, resolver)
		case *config.KibanaAssociationConfigurationHelper:
			newStatus, err = r.reconcileKibanaAssociation(ctx, &apmServer, resolver)
		}
		if err != nil {
			results.WithError(err)
		}
	}

	if err := deleteOrphanedResources(ctx, r, &apmServer, configurationHelpers); err != nil {
		log.Error(err, "Error while trying to delete orphaned resources. Continuing.", "namespace", apmServer.GetNamespace(), "as_name", apmServer.GetName())
	}

	// we want to attempt a status update even in the presence of errors
	if err := r.updateStatus(ctx, apmServer, newStatus); err != nil {
		return defaultRequeue, tracing.CaptureError(ctx, err)
	}
	return results.
		WithResult(association.RequeueRbacCheck(r.accessReviewer)).
		WithResult(resultFromStatus(newStatus)).
		Aggregate()
}

func (r *ReconcileApmServerElasticsearchAssociation) updateStatus(ctx context.Context, apmServer apmv1.ApmServer, newStatus commonv1.AssociationStatus) error {
	span, _ := apm.StartSpan(ctx, "update_association", tracing.SpanTypeApp)
	defer span.End()

	oldStatus := apmServer.Status.Association
	if !reflect.DeepEqual(oldStatus, newStatus) {
		apmServer.Status.Association = newStatus
		if err := r.Status().Update(&apmServer); err != nil {
			return err
		}
		r.recorder.AnnotatedEventf(&apmServer,
			annotation.ForAssociationStatusChange(oldStatus, newStatus),
			corev1.EventTypeNormal,
			events.EventAssociationStatusChange,
			"Association status changed from [%s] to [%s]", oldStatus, newStatus)

	}
	return nil
}

func resultFromStatus(status commonv1.AssociationStatus) reconcile.Result {
	switch status {
	case commonv1.AssociationPending:
		return defaultRequeue // retry
	default:
		return reconcile.Result{} // we are done or there is not much we can do
	}
}

func (r *ReconcileApmServerElasticsearchAssociation) isCompatible(ctx context.Context, apmServer *apmv1.ApmServer) (bool, error) {
	selector := map[string]string{labels.ApmServerNameLabelName: apmServer.Name}
	compat, err := annotation.ReconcileCompatibility(ctx, r.Client, apmServer, selector, r.OperatorInfo.BuildInfo.Version)
	if err != nil {
		k8s.EmitErrorEvent(r.recorder, err, apmServer, events.EventCompatCheckError, "Error during compatibility check: %v", err)
	}
	return compat, err
}

func (r *ReconcileApmServerElasticsearchAssociation) getElasticsearch(
	ctx context.Context,
	apmServer commonv1.Associated,
	backendRef commonv1.ObjectSelector,
	backend runtime.Object,
	cfgAnnotation string,
) (commonv1.AssociationStatus, error) {
	span, _ := apm.StartSpan(ctx, "get_backend", tracing.SpanTypeApp)
	defer span.End()
	err := r.Get(backendRef.NamespacedName(), backend)
	if err != nil {
		k8s.EmitErrorEvent(r.recorder, err, apmServer, events.EventAssociationError,
			"Failed to find referenced backend %s: %v", backendRef.NamespacedName(), err)
		if apierrors.IsNotFound(err) {
			// ES is not found, remove any existing backend configuration and retry in a bit.
			if err := association.RemoveAssociationConf(r.Client, cfgAnnotation, apmServer); err != nil && !errors.IsConflict(err) {
				log.Error(err, "Failed to remove backend output from APMServer object", "namespace", apmServer.GetNamespace(), "name", apmServer.GetName(), "annotation", cfgAnnotation)
				return commonv1.AssociationPending, err
			}
			return commonv1.AssociationPending, nil
		}
		return commonv1.AssociationFailed, err
	}
	return "", nil
}

func (r *ReconcileApmServerElasticsearchAssociation) updateEsAssocConf(ctx context.Context, expectedAssocConf *commonv1.AssociationConf, apmServer commonv1.Associated, cfgHelper association.ConfigurationHelper) (commonv1.AssociationStatus, error) {
	span, _ := apm.StartSpan(ctx, "update_apm_es_assoc", tracing.SpanTypeApp)
	defer span.End()

	associationConf := cfgHelper.AssociationConf()
	if !reflect.DeepEqual(expectedAssocConf, associationConf) {
		log.Info("Updating APMServer spec with association configuration", "namespace", apmServer.GetNamespace(), "name", apmServer.GetName())
		if err := association.UpdateAssociationConf(r.Client, cfgHelper.ConfigurationAnnotation(), apmServer, expectedAssocConf); err != nil {
			if errors.IsConflict(err) {
				return commonv1.AssociationPending, nil
			}
			log.Error(err, "Failed to update APMServer association configuration", "namespace", apmServer.GetNamespace(), "name", apmServer.GetNamespace())
			return commonv1.AssociationPending, err
		}
		cfgHelper.SetAssociationConf(expectedAssocConf)
	}
	return "", nil
}

// Unbind removes the association resources
func (r *ReconcileApmServerElasticsearchAssociation) Unbind(apm commonv1.Associated, cfgAnnotation string) error {
	apmKey := k8s.ExtractNamespacedName(apm)
	// Ensure that user in Elasticsearch is deleted to prevent illegitimate access
	if err := user.DeleteUser(r.Client, NewUserLabelSelector(apmKey)); err != nil {
		return err
	}
	// Also remove the association configuration
	return association.RemoveAssociationConf(r.Client, cfgAnnotation, apm)
}

// deleteOrphanedResources deletes resources created by this association that are left over from previous reconciliation
// attempts. If a user changes namespace on a vertex of an association the standard reconcile mechanism will not delete the
// now redundant old user object/secret. This function lists all resources that don't match the current name/namespace
// combinations and deletes them.
func deleteOrphanedResources(ctx context.Context, c k8s.Client, as commonv1.Associated, cfgHelpers []association.ConfigurationHelper) error {
	span, _ := apm.StartSpan(ctx, "delete_orphaned_resources", tracing.SpanTypeApp)
	defer span.End()

	var secrets corev1.SecretList
	ns := client.InNamespace(as.GetNamespace())
	matchLabels := client.MatchingLabels(NewResourceLabels(as.GetName()))
	if err := c.List(&secrets, ns, matchLabels); err != nil {
		return err
	}

	for _, s := range secrets.Items {
		controlledBy := metav1.IsControlledBy(&s, as)
		isBackendInUse := isBackendInUse(&s, cfgHelpers)
		if controlledBy && !isBackendInUse {
			log.Info("Deleting secret", "namespace", s.Namespace, "secret_name", s.Name, "as_name", as.GetName())
			if err := c.Delete(&s); err != nil {
				return err
			}
		}
	}
	return nil
}

func isBackendInUse(object metav1.Object, cfgHelpers []association.ConfigurationHelper) bool {
	annotations := object.GetLabels()
	if annotations == nil {
		return true // Safe side
	}
	value, ok := annotations[AssociationLabelType]
	if !ok {
		return true // Safe side
	}
	for _, cfgHelper := range cfgHelpers {
		if value == cfgHelper.AssociationTypeValue() {
			backendRef := cfgHelper.AssociationRef()
			return backendRef.IsDefined()
		}
	}
	return true // Safe side
}
