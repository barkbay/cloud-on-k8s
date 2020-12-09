// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

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
	RequeueAfter: 1 * time.Minute,
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
			"reconcile.policyName": request.Name,
			"reconcile.namespace":  request.Namespace,
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

	autoscalingStatus, err := GetAutoscalingStatus(es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	if len(autoscalingSpecifications) == 0 && len(autoscalingStatus) == 0 {
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
	namedTiers, err := autoscalingSpecifications.GetNamedTiers(es)
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
	autoscalingStatus NodeSetsStatus,
	namedTiers esv1.NamedTiers,
	autoscalingSpecs esv1.AutoscalingSpecs,
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
		// Attempt an offline reconciliation
		if _, err := r.doOfflineReconciliation(ctx, autoscalingStatus, namedTiers, autoscalingSpecs, es, results); err != nil {
			logf.FromContext(ctx).Error(err, "error while trying an offline reconciliation")
		}
	}
	if apierrors.IsNotFound(err) {
		// Some required Secrets might not exist yet
		return reconcile.Result{
			Requeue:      true,
			RequeueAfter: 10 * time.Second,
		}, nil
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
	autoscalingStatus NodeSetsStatus,
	namedTiers esv1.NamedTiers,
	autoscalingSpecs esv1.AutoscalingSpecs,
	es esv1.Elasticsearch,
	results *reconciler.Results,
) (reconcile.Result, error) {
	span, _ := apm.StartSpan(ctx, "online_reconciliation", tracing.SpanTypeApp)
	defer span.End()
	log := logf.FromContext(ctx)
	esClient, err := r.newElasticsearchClient(r.Client, es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Update named policies in Elasticsearch
	if err := updatePolicies(ctx, autoscalingSpecs, esClient); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// 3. Get resource requirements from the Elasticsearch capacity API
	decisions, err := esClient.GetAutoscalingCapacity(ctx)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	statusBuilder := NewPolicyStatesBuilder()
	var clusterNodeSetsResources NodeSetsResources
	// For each resource autoscalingSpec:
	// 1. Get the associated nodeSets
	for _, autoscalingSpec := range autoscalingSpecs {
		log.V(1).Info("Autoscaling resources", "tier", autoscalingSpec.Name)
		// Get the currentNodeSets
		nodeSets, exists := namedTiers[autoscalingSpec.Name]
		if !exists {
			return results.WithError(fmt.Errorf("no nodeSets for tier %s", autoscalingSpec.Name)).Aggregate()
		}
		// Save the nodeSets in the status
		statusBuilder.ForPolicy(autoscalingSpec.Name).SetNodeSets(nodeSets)

		// Get the decision from the Elasticsearch API
		var nodeSetsResources NodeSetsResources
		switch decision, gotDecision := decisions.Policies[autoscalingSpec.Name]; gotDecision {
		case false:
			log.V(1).Info("No decision for tier, ensure min. are set", "tier", autoscalingSpec.Name)
			nodeSetsResources = ensureResourcePolicies(nodeSets, autoscalingSpec, autoscalingStatus)
		case true:
			nodeSetsResources = getScaleDecision(log, nodeSets, decision.RequiredCapacity, autoscalingSpec, statusBuilder)
		}
		clusterNodeSetsResources = append(clusterNodeSetsResources, nodeSetsResources...)
	}
	// Replace currentNodeSets in the Elasticsearch manifest
	updateNodeSets(ctx, &es, clusterNodeSetsResources)
	// Update autoscaling status
	if err := UpdateAutoscalingStatus(&es, statusBuilder, clusterNodeSetsResources); err != nil {
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

// doOfflineReconciliation runs an autoscaling reconciliation if the autoscaling API is not ready (yet).
func (r *ReconcileElasticsearch) doOfflineReconciliation(
	ctx context.Context,
	autoscalingStatus NodeSetsStatus,
	namedTiers esv1.NamedTiers,
	autoscalingSpecs esv1.AutoscalingSpecs,
	es esv1.Elasticsearch,
	results *reconciler.Results,
) (reconcile.Result, error) {
	span, _ := apm.StartSpan(ctx, "offline_reconciliation", tracing.SpanTypeApp)
	defer span.End()
	log := logf.FromContext(ctx)
	statusBuilder := NewPolicyStatesBuilder()
	var clusterNodeSetsResources NodeSetsResources
	// Elasticsearch is not reachable, we still want to ensure that min. requirements are set
	for _, autoscalingSpec := range autoscalingSpecs {
		log.V(1).Info("Autoscaling resources", "tier", autoscalingSpec.Name)
		nodeSets, exists := namedTiers[autoscalingSpec.Name]
		if !exists {
			return results.WithError(fmt.Errorf("no nodeSets for tier %s", autoscalingSpec.Name)).Aggregate()
		}
		nodeSetsResources := ensureResourcePolicies(nodeSets, autoscalingSpec, autoscalingStatus)
		clusterNodeSetsResources = append(clusterNodeSetsResources, nodeSetsResources...)
	}
	// Replace currentNodeSets in the Elasticsearch manifest
	updateNodeSets(ctx, &es, clusterNodeSetsResources)
	// Update autoscaling status
	if err := UpdateAutoscalingStatus(&es, statusBuilder, clusterNodeSetsResources); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	// Apply the update Elasticsearch manifest with the minimums
	if err := r.Client.Update(&es); err != nil {
		if apierrors.IsConflict(err) {
			return results.WithResult(reconcile.Result{Requeue: true}).Aggregate()
		}
		return results.WithError(err).Aggregate()
	}
	return results.WithResult(defaultReconcile).Aggregate()
}

// updateNodeSets updates the resources in the NodeSets of an Elasticsearch spec according to the NodeSetsResources
// computed by the autoscaling algorithm.
func updateNodeSets(
	ctx context.Context,
	es *esv1.Elasticsearch,
	clusterNodeSetsResources NodeSetsResources,
) {
	log := logf.FromContext(ctx)
	resourcesByNodeSet := clusterNodeSetsResources.byNodeSet()
	for i, nodeSet := range es.Spec.NodeSets {
		nodeSetResources, ok := resourcesByNodeSet[nodeSet.Name]
		if !ok {
			continue
		}

		container, containers := getContainer(esv1.ElasticsearchContainerName, es.Spec.NodeSets[i].PodTemplate.Spec.Containers)
		if container == nil {
			container = &corev1.Container{
				Name: esv1.ElasticsearchContainerName,
			}
		}

		// Update desired count
		es.Spec.NodeSets[i].Count = nodeSetResources.Count

		if container.Resources.Requests == nil {
			container.Resources.Requests = corev1.ResourceList{}
		}
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}

		// Update memory requests and limits
		if nodeSetResources.Memory != nil {
			container.Resources.Requests[corev1.ResourceMemory] = *nodeSetResources.Memory
			//TODO: apply request/memory ratio
			container.Resources.Limits[corev1.ResourceMemory] = *nodeSetResources.Memory
		}
		if nodeSetResources.Cpu != nil {
			container.Resources.Requests[corev1.ResourceCPU] = *nodeSetResources.Cpu
			//TODO: apply request/memory ratio
		}

		if nodeSetResources.Storage != nil {
			// Update storage claim
			if len(es.Spec.NodeSets[i].VolumeClaimTemplates) == 0 {
				es.Spec.NodeSets[i].VolumeClaimTemplates = []corev1.PersistentVolumeClaim{volume.DefaultDataVolumeClaim}
			}
			for _, claimTemplate := range es.Spec.NodeSets[i].VolumeClaimTemplates {
				if claimTemplate.Name == volume.ElasticsearchDataVolumeName &&
					claimTemplate.Spec.Resources.Requests != nil {
					previousStorageCapacity, ok := claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
					if !ok {
						break
					}
					if !previousStorageCapacity.Equal(*nodeSetResources.Storage) {
						log.V(1).Info("Increase storage capacity", "node_set", es.Spec.NodeSets[i].Name, "current_capacity", previousStorageCapacity, "new_capacity", *nodeSetResources.Storage)
						claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage] = *nodeSetResources.Storage
					}
				}
			}
		}

		es.Spec.NodeSets[i].PodTemplate.Spec.Containers = append(containers, *container)
	}
}

func (r *ReconcileElasticsearch) internalReconcile(
	ctx context.Context,
	es esv1.Elasticsearch,
) *reconciler.Results {
	results := reconciler.NewResult(ctx)

	if es.IsMarkedForDeletion() {
		r.dynamicWatches.Secrets.RemoveHandlerForKey(nodesConfigurationWatchName(k8s.ExtractNamespacedName(&es)))
		return results
	}

	// Set watches on config Secrets
	configSecrets := make([]string, len(es.Spec.NodeSets))
	for i, nodeSet := range es.Spec.NodeSets {
		sset := esv1.StatefulSet(es.Name, nodeSet.Name)
		configSecrets[i] = esv1.ConfigSecret(sset)
	}
	namespacedName := k8s.ExtractNamespacedName(&es)
	if err := watches.WatchUserProvidedSecrets(namespacedName, r.dynamicWatches, nodesConfigurationWatchName(namespacedName), configSecrets); err != nil {
		return results.WithError(err)
	}

	return results
}

// nodesConfigurationWatchName returns the watch name according to the deployment name.
// It is unique per APM or Kibana deployment.
func nodesConfigurationWatchName(namespacedName types.NamespacedName) string {
	return fmt.Sprintf("%s-%s-config", namespacedName.Namespace, namespacedName.Name)
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
			r.dynamicWatches.Secrets.RemoveHandlerForKey(nodesConfigurationWatchName(request.NamespacedName))
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
