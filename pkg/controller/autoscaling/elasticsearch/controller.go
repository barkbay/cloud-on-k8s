// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package elasticsearch

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"time"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/elasticsearch/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	logconf "github.com/elastic/cloud-on-k8s/pkg/utils/log"
	"github.com/elastic/cloud-on-k8s/pkg/utils/net"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type EsClientProvider func(c k8s.Client, dialer net.Dialer, es esv1.Elasticsearch) (esclient.Client, error)

const (
	controllerName = "elasticsearch-autoscaling"
)

var defaultReconcile = reconcile.Result{
	Requeue:      true,
	RequeueAfter: 10 * time.Second,
}

// ReconcileElasticsearch reconciles autoscaling policies and Elasticsearch resources specifications based on autoscaling decisions.
type ReconcileElasticsearch struct {
	k8s.Client
	operator.Parameters
	esClientProvider EsClientProvider
	recorder         record.EventRecorder
	licenseChecker   license.Checker

	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
}

// NewReconciler returns a new reconcile.Reconciler
func NewReconciler(mgr manager.Manager, params operator.Parameters) *ReconcileElasticsearch {
	c := k8s.WrapClient(mgr.GetClient())
	return &ReconcileElasticsearch{
		Client:           c,
		Parameters:       params,
		esClientProvider: newElasticsearchClient,
		recorder:         mgr.GetEventRecorderFor(controllerName),
		licenseChecker:   license.NewLicenseChecker(c, params.OperatorNamespace),
	}
}

// Reconcile updates the ResourceRequirements and PersistentVolumeClaim fields for each elasticsearch container in a
// NodeSet managed by an autoscaling policy. ResourceRequirements are updated according to the response of the Elasticsearch
// _autoscaling/capacity API and given the constraints provided by the user in the autoscaling specification.
func (r *ReconcileElasticsearch) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx := common.NewReconciliationContext(&r.iteration, r.Tracer, controllerName, "es_name", request)
	defer common.LogReconciliationRunNoSideEffects(logconf.FromContext(ctx))()
	defer tracing.EndContextTransaction(ctx)

	// Fetch the Elasticsearch instance
	var es esv1.Elasticsearch
	requeue, err := r.fetchElasticsearch(ctx, request, &es)
	if err != nil || requeue {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if !es.IsAutoscalingDefined() {
		return reconcile.Result{}, nil
	}

	log := logconf.FromContext(ctx)
	if common.IsUnmanaged(&es) {
		log.Info("Object is currently not managed by this controller. Skipping reconciliation", "namespace", es.Namespace, "es_name", es.Name)
		return reconcile.Result{}, nil
	}

	selector := map[string]string{label.ClusterNameLabelName: es.Name}
	compat, err := annotation.ReconcileCompatibility(ctx, r.Client, &es, selector, r.OperatorInfo.BuildInfo.Version)
	if err != nil {
		k8s.EmitErrorEvent(r.recorder, err, &es, events.EventCompatCheckError, "Error during compatibility check: %v", err)
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if !compat {
		// this resource is not able to be reconciled by this version of the controller, so we will skip it and not requeue
		return reconcile.Result{}, nil
	}

	// Get resource policies from the Elasticsearch spec
	autoscalingSpecification, err := es.GetAutoscalingSpecification()
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Validate autoscaling spec
	errs := autoscalingSpecification.Validate()
	if len(errs) > 0 {
		for _, err := range errs.ToAggregate().Errors() {
			log.Error(err, "invalid autoscaling specification")
		}
		return reconcile.Result{}, tracing.CaptureError(ctx, fmt.Errorf("autoscaling spec is invalid"))
	}

	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	if !v.IsSameOrAfter(esv1.ElasticsearchMinAutoscalingVersion) {
		compatibilityErr := fmt.Errorf("autoscaling requires version %s of Elasticsearch, current version is %s", esv1.ElasticsearchMinAutoscalingVersion, v)
		k8s.EmitErrorEvent(r.recorder, compatibilityErr, &es, events.EventCompatCheckError, "Error during compatibility check: %v", compatibilityErr)
	}
	// Build status from annotation or existing resources
	autoscalingStatus, err := status.GetStatus(es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if len(autoscalingSpecification.AutoscalingPolicySpecs) == 0 && len(autoscalingStatus.AutoscalingPolicyStatuses) == 0 {
		// This cluster is not managed by the autoscaler
		return reconcile.Result{}, nil
	}

	// Compute named tiers
	namedTiers, namedTiersErr := autoscalingSpecification.GetAutoscaledNodeSets()
	if namedTiersErr != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	log.V(1).Info("Named tiers", "named_tiers", namedTiers)

	// Import existing resources in the actual Status if the cluster is managed by some autoscaling policies but
	// the status annotation does not exist.
	if err := autoscalingStatus.ImportExistingResources(log, r.Client, autoscalingSpecification, namedTiers); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Call the main function
	current, err := r.reconcileInternal(ctx, autoscalingStatus, namedTiers, autoscalingSpecification, es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	results := &reconciler.Results{}
	return results.WithResult(defaultReconcile).WithResult(current).Aggregate()
}

func newElasticsearchClient(
	c k8s.Client,
	dialer net.Dialer,
	es esv1.Elasticsearch,
) (esclient.Client, error) {
	url := services.ExternalServiceURL(es)
	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return nil, err
	}
	// Get user Secret
	var controllerUserSecret corev1.Secret
	key := types.NamespacedName{
		Namespace: es.Namespace,
		Name:      esv1.InternalUsersSecret(es.Name),
	}
	if err := c.Get(key, &controllerUserSecret); err != nil {
		return nil, err
	}
	password, ok := controllerUserSecret.Data[user.ControllerUserName]
	if !ok {
		return nil, fmt.Errorf("controller user %s not found in Secret %s/%s", user.ControllerUserName, key.Namespace, key.Name)
	}

	// Get public certs
	var caSecret corev1.Secret
	key = types.NamespacedName{
		Namespace: es.Namespace,
		Name:      certificates.PublicCertsSecretName(esv1.ESNamer, es.Name),
	}
	if err := c.Get(key, &caSecret); err != nil {
		return nil, err
	}
	trustedCerts, ok := caSecret.Data[certificates.CertFileName]
	if !ok {
		return nil, fmt.Errorf("%s not found in Secret %s/%s", certificates.CertFileName, key.Namespace, key.Name)
	}
	caCerts, err := certificates.ParsePEMCerts(trustedCerts)
	if err != nil {
		return nil, err
	}
	return esclient.NewElasticsearchClient(
		dialer,
		url,
		esclient.BasicAuth{
			Name:     user.ControllerUserName,
			Password: string(password),
		},
		*v,
		caCerts,
		esclient.Timeout(es),
	), nil
}
