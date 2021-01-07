// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/autoscaler"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"

	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"

	"github.com/go-logr/logr"

	"go.elastic.co/apm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/validation"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

const (
	name = "elasticsearch-autoscaling"
)

// ReconcileElasticsearch reconciles autoscaling policies and Elasticsearch specifications based on autoscaling decisions.
type ReconcileElasticsearch struct {
	k8s.Client
	operator.Parameters
	recorder       record.EventRecorder
	licenseChecker license.Checker
	dynamicWatches watches.DynamicWatches

	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
}

var defaultReconcile = reconcile.Result{
	Requeue:      true,
	RequeueAfter: 15 * time.Second,
}

// Reconcile attempts to update the capacity fields (count and memory request for now) in the currentNodeSets of the Elasticsearch
// resource according to the result of the Elasticsearch capacity API and given the constraints provided by the user in
// the resource policies.
func (r *ReconcileElasticsearch) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// TODO: remove custom log/tracing or refactor it based on https://github.com/elastic/cloud-on-k8s/issues/2491#issuecomment-696678067
	currentIteration := atomic.AddUint64(&r.iteration, 1)
	startTime := time.Now()
	tx, ctx := tracing.NewTransaction(r.Tracer, request.NamespacedName, name)
	defer tracing.EndTransaction(tx)
	keyValues := tracing.TraceContextKV(ctx)
	// Create logger
	log := logf.Log.WithName(name).
		WithValues("event.sequence", currentIteration, "event.dataset", name).
		WithValues("labels", map[string]interface{}{
			"reconcile.name":      request.Name,
			"reconcile.namespace": request.Namespace,
		}).
		WithValues(keyValues...)
	log.Info("Starting reconciliation run")
	defer func() {
		totalTime := time.Since(startTime)
		log.Info("Ending reconciliation run", "event.duration", totalTime)
	}()
	ctx = logf.IntoContext(ctx, log)

	// Fetch the Elasticsearch instance
	var es esv1.Elasticsearch
	requeue, err := r.fetchElasticsearch(ctx, request, &es)
	if err != nil || requeue {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if !es.IsAutoscalingDefined() {
		return reconcile.Result{}, nil
	}

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
	autoscalingSpecifications, err := es.GetAutoscalingSpecifications()
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Validate autoscaling spec
	errs := autoscalingSpecifications.Validate()
	if len(errs) > 0 {
		for _, err := range errs.ToAggregate().Errors() {
			log.Error(err, "invalid autoscaling specification")
		}
		return reconcile.Result{}, tracing.CaptureError(ctx, fmt.Errorf("autoscaling spec is invalid"))
	}

	autoscalingStatus, err := status.GetStatus(es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if len(autoscalingSpecifications.AutoscalingPolicySpecs) == 0 && len(autoscalingStatus.PolicyStates) == 0 {
		// This cluster is not managed by the autoscaler
		return reconcile.Result{}, nil
	}

	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	if !v.IsSameOrAfter(validation.ElasticsearchMinAutoscalingVersion) {
		compatibilityErr := fmt.Errorf("autoscaling requires version %s of Elasticsearch, current version is %s", validation.ElasticsearchMinAutoscalingVersion, v)
		k8s.EmitErrorEvent(r.recorder, compatibilityErr, &es, events.EventCompatCheckError, "Error during compatibility check: %v", compatibilityErr)
	}

	// Compute named tiers
	namedTiers, err := autoscalingSpecifications.GetAutoscaledNodeSets()
	// Configuration does not exist yet, retry later.
	if apierrors.IsNotFound(err) {
		return defaultReconcile, nil
	}
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	log.V(1).Info("Named tiers", "named_tiers", namedTiers)

	// Call the main function
	current, err := r.reconcileInternal(ctx, autoscalingStatus, namedTiers, autoscalingSpecifications, es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	results := &reconciler.Results{}
	return results.WithResult(defaultReconcile).WithResult(current).Aggregate()
}

func (r *ReconcileElasticsearch) reconcileInternal(
	ctx context.Context,
	autoscalingStatus status.Status,
	namedTiers esv1.AutoscaledNodeSets,
	autoscalingSpecs esv1.AutoscalingSpec,
	es esv1.Elasticsearch,
) (reconcile.Result, error) {
	results := &reconciler.Results{}
	log := logf.FromContext(ctx)

	if esReachable, err := r.isElasticsearchReachable(ctx, es); !esReachable || err != nil {
		// Elasticsearch is not reachable, or we got an error while checking Elasticsearch availability, follow up with an offline reconciliation.
		if err != nil {
			log.V(1).Info("error while checking if Elasticsearch is available, attempting offline reconciliation", "error.message", err.Error())
		}
		return r.doOfflineReconciliation(ctx, autoscalingStatus, namedTiers, autoscalingSpecs, es, results)
	}

	// Cluster is supposed to be online
	result, err := r.attemptOnlineReconciliation(ctx, autoscalingStatus, namedTiers, autoscalingSpecs, es, results)
	if err != nil {
		log.Error(tracing.CaptureError(ctx, err), "autoscaling online reconciliation failed")
		// Attempt an offline reconciliation
		if _, err := r.doOfflineReconciliation(ctx, autoscalingStatus, namedTiers, autoscalingSpecs, es, results); err != nil {
			log.Error(tracing.CaptureError(ctx, err), "autoscaling offline reconciliation failed")
		}
	}
	return result, err
}

// Check if the Service is available
func (r *ReconcileElasticsearch) isElasticsearchReachable(ctx context.Context, es esv1.Elasticsearch) (bool, error) {
	span, _ := apm.StartSpan(ctx, "is_es_reachable", tracing.SpanTypeApp)
	defer span.End()
	externalService, err := services.GetExternalService(r.Client, es)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, tracing.CaptureError(ctx, err)
	}
	esReachable, err := services.IsServiceReady(r.Client, externalService)
	if err != nil {
		return false, tracing.CaptureError(ctx, err)
	}
	return esReachable, nil
}

// attemptOnlineReconciliation attempts an online autoscaling reconciliation with a call the Elasticsearch autoscaling API.
func (r *ReconcileElasticsearch) attemptOnlineReconciliation(
	ctx context.Context,
	actualAutoscalingStatus status.Status,
	namedTiers esv1.AutoscaledNodeSets,
	autoscalingSpecs esv1.AutoscalingSpec,
	es esv1.Elasticsearch,
	results *reconciler.Results,
) (reconcile.Result, error) {
	span, _ := apm.StartSpan(ctx, "online_reconciliation", tracing.SpanTypeApp)
	defer span.End()
	log := logf.FromContext(ctx)
	log.V(1).Info("Starting online autoscaling reconciliation")
	esClient, err := r.newElasticsearchClient(r.Client, es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Update named policies in Elasticsearch
	if err := updatePolicies(log, ctx, autoscalingSpecs, esClient); err != nil {
		log.Error(err, "Error while updating the autoscaling policies")
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Get capacity requirements from the Elasticsearch capacity API
	decisions, err := esClient.GetAutoscalingCapacity(ctx)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	statusBuilder := status.NewPolicyStatesBuilder()

	// nextNodeSetsResources holds the resources computed by the autoscaling algorithm for each nodeSet.
	var nextNodeSetsResources nodesets.ClusterResources

	// For each autoscaling policy we compute the resources to be applied to the related nodeSets.
	for _, autoscalingPolicy := range autoscalingSpecs.AutoscalingPolicySpecs {
		// Get the currentNodeSets
		nodeSetList, exists := namedTiers[autoscalingPolicy.Name]
		if !exists {
			// TODO: Remove once validation is implemented, this situation should be caught by validation
			err := fmt.Errorf("no nodeSets for tier %s", autoscalingPolicy.Name)
			log.Error(err, "no nodeSet for a tier", "policy", autoscalingPolicy.Name)
			results.WithError(fmt.Errorf("no nodeSets for tier %s", autoscalingPolicy.Name))
			statusBuilder.ForPolicy(autoscalingPolicy.Name).WithPolicyState(status.NoNodeSet, err.Error())
			continue
		}

		// Get the decision from the Elasticsearch API
		var nodeSetsResources nodesets.NamedTierResources
		switch capacity, hasCapacity := decisions.Policies[autoscalingPolicy.Name]; hasCapacity && !capacity.RequiredCapacity.IsEmpty() {
		case false:
			// We didn't receive a decision for this tier, or the decision is empty. We can only ensure that resources are within the allowed ranges.
			log.V(1).Info("No decision for tier, ensure min. are set", "policy", autoscalingPolicy.Name)
			statusBuilder.ForPolicy(autoscalingPolicy.Name).WithPolicyState(status.EmptyResponse, "No required capacity from Elasticsearch")
			nodeSetsResources = autoscaler.GetOfflineNodeSetsResources(log, nodeSetList.Names(), autoscalingPolicy, actualAutoscalingStatus, statusBuilder)
		case true:
			// We received a capacity decision from Elasticsearch for this policy.
			log.Info("Required capacity for policy", "policy", autoscalingPolicy.Name, "required_capacity", capacity.RequiredCapacity)
			// Ensure that the user provides the related resources policies
			if !canDecide(log, capacity.RequiredCapacity, autoscalingPolicy, statusBuilder) {
				continue
			}
			nodeSetsResources = autoscaler.GetScaleDecision(log, nodeSetList.Names(), actualAutoscalingStatus, capacity.RequiredCapacity, autoscalingPolicy, statusBuilder)
			// Apply cooldown filter
			applyCoolDownFilters(log, es, nodeSetsResources, autoscalingPolicy, actualAutoscalingStatus, statusBuilder)
		}
		// Add the result to the list of the next resources
		nextNodeSetsResources = append(nextNodeSetsResources, nodeSetsResources)
	}

	// Replace currentNodeSets in the Elasticsearch manifest
	if err := updateElasticsearch(log, &es, statusBuilder, nextNodeSetsResources, actualAutoscalingStatus); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// TODO: Check if we got a decision for an unknown tier

	if results.HasError() {
		return results.Aggregate()
	}

	// Apply the update Elasticsearch manifest
	if err := r.Client.Update(&es); err != nil {
		if apierrors.IsConflict(err) {
			return results.WithResult(reconcile.Result{Requeue: true}).Aggregate()
		}
		return results.WithError(err).Aggregate()
	}
	return reconcile.Result{}, nil
}

// canDecide ensures that the user has provided resource ranges to apply Elasticsearch autoscaling decision.
// Expected ranges are not consistent across all deciders. For example ml may only require memory limits, while processing
// data deciders response may require storage limits.
func canDecide(log logr.Logger, requiredCapacity esclient.RequiredCapacity, spec esv1.AutoscalingPolicySpec, statusBuilder *status.PolicyStatesBuilder) bool {
	result := true
	if (requiredCapacity.Node.Memory != nil || requiredCapacity.Total.Memory != nil) && !spec.IsMemoryDefined() {
		log.Error(fmt.Errorf("min and max memory must be specified"), "Min and max memory must be specified", "policy", spec.Name)
		statusBuilder.ForPolicy(spec.Name).WithPolicyState(status.MemoryRequired, "Min and max memory must be specified")
		result = false
	}
	if (requiredCapacity.Node.Storage != nil || requiredCapacity.Total.Storage != nil) && !spec.IsStorageDefined() {
		log.Error(fmt.Errorf("min and max memory must be specified"), "Min and max storage must be specified", "policy", spec.Name)
		statusBuilder.ForPolicy(spec.Name).WithPolicyState(status.StorageRequired, "Min and max storage must be specified")
		result = false
	}
	return result
}

// doOfflineReconciliation runs an autoscaling reconciliation if the autoscaling API is not ready (yet).
func (r *ReconcileElasticsearch) doOfflineReconciliation(
	ctx context.Context,
	actualAutoscalingStatus status.Status,
	namedTiers esv1.AutoscaledNodeSets,
	autoscalingSpecs esv1.AutoscalingSpec,
	es esv1.Elasticsearch,
	results *reconciler.Results,
) (reconcile.Result, error) {
	span, _ := apm.StartSpan(ctx, "offline_reconciliation", tracing.SpanTypeApp)
	defer span.End()
	log := logf.FromContext(ctx)
	log.V(1).Info("Starting offline autoscaling reconciliation")
	statusBuilder := status.NewPolicyStatesBuilder()
	var clusterNodeSetsResources nodesets.ClusterResources
	// Elasticsearch is not reachable, we still want to ensure that min. requirements are set
	for _, autoscalingSpec := range autoscalingSpecs.AutoscalingPolicySpecs {
		nodeSets, exists := namedTiers[autoscalingSpec.Name]
		if !exists {
			return results.WithError(fmt.Errorf("no nodeSets for tier %s", autoscalingSpec.Name)).Aggregate()
		}
		nodeSetsResources := autoscaler.GetOfflineNodeSetsResources(log, nodeSets.Names(), autoscalingSpec, actualAutoscalingStatus, statusBuilder)
		clusterNodeSetsResources = append(clusterNodeSetsResources, nodeSetsResources)
	}
	// Update the Elasticsearch manifest
	if err := updateElasticsearch(log, &es, statusBuilder, clusterNodeSetsResources, actualAutoscalingStatus); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Apply the updated Elasticsearch manifest
	if err := r.Client.Update(&es); err != nil {
		if apierrors.IsConflict(err) {
			return results.WithResult(reconcile.Result{Requeue: true}).Aggregate()
		}
		return results.WithError(err).Aggregate()
	}
	return results.WithResult(defaultReconcile).Aggregate()
}

// updateElasticsearch updates the resources in the NodeSets of an Elasticsearch spec according to the NamedTierResources
// computed by the autoscaling algorithm. It also updates the autoscaling status annotation.
func updateElasticsearch(
	log logr.Logger,
	es *esv1.Elasticsearch,
	statusBuilder *status.PolicyStatesBuilder,
	nextNodeSetsResources nodesets.ClusterResources,
	actualAutoscalingStatus status.Status,
) error {
	nextResourcesByNodeSet := nextNodeSetsResources.ByNodeSet()
	for i := range es.Spec.NodeSets {
		name := es.Spec.NodeSets[i].Name
		nodeSetResources, ok := nextResourcesByNodeSet[name]
		if !ok {
			log.V(1).Info("Skipping nodeset update", "nodeset", name)
			continue
		}

		container, containers := getContainer(esv1.ElasticsearchContainerName, es.Spec.NodeSets[i].PodTemplate.Spec.Containers)
		// Create a copy to compare if some changes have been made.
		actualContainer := container.DeepCopy()
		if container == nil {
			container = &corev1.Container{
				Name: esv1.ElasticsearchContainerName,
			}
		}

		// Update desired count
		es.Spec.NodeSets[i].Count = nodeSetResources.NodeCount

		if container.Resources.Requests == nil {
			container.Resources.Requests = corev1.ResourceList{}
		}
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}

		// Update memory requests and limits
		if nodeSetResources.HasRequest(corev1.ResourceMemory) {
			container.Resources.Requests[corev1.ResourceMemory] = nodeSetResources.GetRequest(corev1.ResourceMemory)
			//TODO: apply request/memory ratio
			container.Resources.Limits[corev1.ResourceMemory] = nodeSetResources.GetRequest(corev1.ResourceMemory)
		}
		if nodeSetResources.HasRequest(corev1.ResourceCPU) {
			container.Resources.Requests[corev1.ResourceCPU] = nodeSetResources.GetRequest(corev1.ResourceCPU)
			//TODO: apply request/memory ratio
		}

		if nodeSetResources.HasRequest(corev1.ResourceStorage) {
			// Update storage claim
			if len(es.Spec.NodeSets[i].VolumeClaimTemplates) == 0 {
				es.Spec.NodeSets[i].VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*volume.DefaultDataVolumeClaim.DeepCopy()}
			}
			for _, claimTemplate := range es.Spec.NodeSets[i].VolumeClaimTemplates {
				if claimTemplate.Name == volume.ElasticsearchDataVolumeName {
					// Storage may have been managed outside of the scope of the autoscaler, we still want to ensure here that
					// we are not scaling down the storage capacity.
					nextStorage := nodeSetResources.GetRequest(corev1.ResourceStorage)
					actualStorage, hasStorage := claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
					if hasStorage && actualStorage.Cmp(nextStorage) > 0 {
						continue
					}
					claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage] = nextStorage
				}
			}
		}

		es.Spec.NodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)

		if !apiequality.Semantic.DeepEqual(actualContainer, container) {
			log.V(1).Info("Updating nodeset with resources", "nodeset", name, "resources", nextNodeSetsResources)
		}
	}

	// Update autoscaling status
	return status.UpdateAutoscalingStatus(es, statusBuilder, nextNodeSetsResources, actualAutoscalingStatus)
}

func (r *ReconcileElasticsearch) fetchElasticsearch(
	ctx context.Context,
	request reconcile.Request,
	es *esv1.Elasticsearch,
) (bool, error) {
	span, _ := apm.StartSpan(ctx, "fetch_elasticsearch", tracing.SpanTypeApp)
	defer span.End()

	err := r.Get(request.NamespacedName, es)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		// Error reading the object - requeue the request.
		return true, err
	}
	return false, nil
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
	// Dynamically Watch for changes in the configuration Secret
	if err := c.Watch(&source.Kind{Type: &corev1.Secret{}}, r.dynamicWatches.Secrets); err != nil {
		return err
	}
	if err := r.dynamicWatches.Secrets.AddHandler(&watches.OwnerWatch{
		EnqueueRequestForOwner: handler.EnqueueRequestForOwner{
			IsController: true,
			OwnerType:    &esv1.Elasticsearch{},
		},
	}); err != nil {
		return err
	}
	return nil
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, params operator.Parameters) *ReconcileElasticsearch {
	c := k8s.WrapClient(mgr.GetClient())
	return &ReconcileElasticsearch{
		Client:         c,
		Parameters:     params,
		recorder:       mgr.GetEventRecorderFor(name),
		licenseChecker: license.NewLicenseChecker(c, params.OperatorNamespace),
		dynamicWatches: watches.NewDynamicWatches(),
	}
}

func (r *ReconcileElasticsearch) newElasticsearchClient(c k8s.Client, es esv1.Elasticsearch) (client.Client, error) {
	url := services.ExternalServiceURL(es)
	// use a fake service for now
	//url := stringsutil.Concat("http://autoscaling-mock-api", ".", es.Namespace, ".svc", ":", strconv.Itoa(network.HTTPPort))
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

	return esclient.NewElasticsearchClient(
		r.Parameters.Dialer,
		url,
		client.BasicAuth{
			Name:     user.ControllerUserName,
			Password: string(password),
		},
		*v,
		caCerts,
		esclient.Timeout(es),
	), nil
}

func getContainer(name string, containers []corev1.Container) (*corev1.Container, []corev1.Container) {
	for i, container := range containers {
		if container.Name == name {
			// Remove the container
			return &container, append(containers[:i], containers[i+1:]...)
		}
	}
	return nil, containers
}
