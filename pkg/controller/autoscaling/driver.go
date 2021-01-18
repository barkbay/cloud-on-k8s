// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaling

import (
	"context"
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/autoscaler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/resources"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	logconf "github.com/elastic/cloud-on-k8s/pkg/utils/log"
	"github.com/go-logr/logr"
	"go.elastic.co/apm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func (r *ReconcileElasticsearch) reconcileInternal(
	ctx context.Context,
	autoscalingStatus status.Status,
	namedTiers esv1.AutoscaledNodeSets,
	autoscalingSpecs esv1.AutoscalingSpec,
	es esv1.Elasticsearch,
) (reconcile.Result, error) {
	defer tracing.Span(&ctx)()
	results := &reconciler.Results{}
	log := logconf.FromContext(ctx)

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
	log := logconf.FromContext(ctx)
	log.V(1).Info("Starting online autoscaling reconciliation")
	esClient, err := r.esClientProvider(r.Client, r.Dialer, es)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Update Machine Learning settings
	mlNodes, maxMemory, err := autoscalingSpecs.GetMLNodesCount()
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}
	if err := esClient.UpdateMLNodesSettings(ctx, mlNodes, maxMemory); err != nil {
		// Trace the error but do not prevent the policies to be updated
		log.Error(tracing.CaptureError(ctx, err), "Error while updating the ML settings")
	}

	// Update named policies in Elasticsearch
	if err := updatePolicies(ctx, log, autoscalingSpecs, esClient); err != nil {
		log.Error(err, "Error while updating the autoscaling policies")
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	// Get capacity requirements from the Elasticsearch capacity API
	decisions, err := esClient.GetAutoscalingCapacity(ctx)
	if err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

	statusBuilder := status.NewAutoscalingStatusBuilder()

	// nextNodeSetsResources holds the resources computed by the autoscaling algorithm for each nodeSet.
	var nextNodeSetsResources resources.ClusterResources

	// For each autoscaling policy we compute the resources to be applied to the related nodeSets.
	for _, autoscalingPolicy := range autoscalingSpecs.AutoscalingPolicySpecs {
		// Get the currentNodeSets
		nodeSetList, exists := namedTiers[autoscalingPolicy.Name]
		if !exists {
			// TODO: Remove once validation is implemented, this situation should be caught by validation
			err := fmt.Errorf("no nodeSets for tier %s", autoscalingPolicy.Name)
			log.Error(err, "no nodeSet for a tier", "policy", autoscalingPolicy.Name)
			results.WithError(fmt.Errorf("no nodeSets for tier %s", autoscalingPolicy.Name))
			statusBuilder.ForPolicy(autoscalingPolicy.Name).WithEvent(status.NoNodeSet, err.Error())
			continue
		}

		// Get the decision from the Elasticsearch API
		var nodeSetsResources resources.NamedTierResources
		switch capacity, hasCapacity := decisions.Policies[autoscalingPolicy.Name]; hasCapacity && !capacity.RequiredCapacity.IsEmpty() {
		case false:
			// We didn't receive a decision for this tier, or the decision is empty. We can only ensure that resources are within the allowed ranges.
			log.V(1).Info("No decision for tier, ensure min. are set", "policy", autoscalingPolicy.Name)
			statusBuilder.ForPolicy(autoscalingPolicy.Name).WithEvent(status.EmptyResponse, "No required capacity from Elasticsearch")
			nodeSetsResources = autoscaler.GetOfflineNodeSetsResources(log, nodeSetList.Names(), autoscalingPolicy, actualAutoscalingStatus)
		case true:
			// We received a capacity decision from Elasticsearch for this policy.
			log.Info(
				"Required capacity for policy",
				"policy", autoscalingPolicy.Name,
				"required_capacity", capacity.RequiredCapacity,
				"current_capacity", capacity.CurrentCapacity,
				"current_capacity.count", len(capacity.CurrentNodes),
				"current_nodes", capacity.CurrentNodes)
			// Ensure that the user provides the related resources policies
			if !canDecide(log, capacity.RequiredCapacity, autoscalingPolicy, statusBuilder) {
				continue
			}
			ctx := autoscaler.Context{
				Log:                     log,
				AutoscalingSpec:         autoscalingPolicy,
				NodeSets:                nodeSetList,
				ActualAutoscalingStatus: actualAutoscalingStatus,
				RequiredCapacity:        capacity.RequiredCapacity,
				StatusBuilder:           statusBuilder,
			}
			nodeSetsResources = ctx.GetScaleDecision()
		}
		// Add the result to the list of the next resources
		nextNodeSetsResources = append(nextNodeSetsResources, nodeSetsResources)
	}

	// Emit the K8S events
	status.EmitEvents(es, r.recorder, statusBuilder.Build())

	// Update the Elasticsearch resource with the calculated resources.
	if err := reconcileElasticsearch(log, &es, statusBuilder, nextNodeSetsResources, actualAutoscalingStatus); err != nil {
		return reconcile.Result{}, tracing.CaptureError(ctx, err)
	}

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
// Only memory and storage are supported since CPU is not part of the autoscaling API specification.
func canDecide(log logr.Logger, requiredCapacity esclient.PolicyCapacityInfo, spec esv1.AutoscalingPolicySpec, statusBuilder *status.AutoscalingStatusBuilder) bool {
	result := true
	if (requiredCapacity.Node.Memory != nil || requiredCapacity.Total.Memory != nil) && !spec.IsMemoryDefined() {
		log.Error(fmt.Errorf("min and max memory must be specified"), "Min and max memory must be specified", "policy", spec.Name)
		statusBuilder.ForPolicy(spec.Name).WithEvent(status.MemoryRequired, "Min and max memory must be specified")
		result = false
	}
	if (requiredCapacity.Node.Storage != nil || requiredCapacity.Total.Storage != nil) && !spec.IsStorageDefined() {
		log.Error(fmt.Errorf("min and max memory must be specified"), "Min and max storage must be specified", "policy", spec.Name)
		statusBuilder.ForPolicy(spec.Name).WithEvent(status.StorageRequired, "Min and max storage must be specified")
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
	defer tracing.Span(&ctx)()
	log := logconf.FromContext(ctx)
	log.V(1).Info("Starting offline autoscaling reconciliation")
	statusBuilder := status.NewAutoscalingStatusBuilder()
	var clusterNodeSetsResources resources.ClusterResources
	// Elasticsearch is not reachable, we still want to ensure that min. requirements are set
	for _, autoscalingSpec := range autoscalingSpecs.AutoscalingPolicySpecs {
		nodeSets, exists := namedTiers[autoscalingSpec.Name]
		if !exists {
			return results.WithError(fmt.Errorf("no nodeSets for tier %s", autoscalingSpec.Name)).Aggregate()
		}
		nodeSetsResources := autoscaler.GetOfflineNodeSetsResources(log, nodeSets.Names(), autoscalingSpec, actualAutoscalingStatus)
		clusterNodeSetsResources = append(clusterNodeSetsResources, nodeSetsResources)
	}

	// Emit the K8S events
	status.EmitEvents(es, r.recorder, statusBuilder.Build())

	// Update the Elasticsearch manifest
	if err := reconcileElasticsearch(log, &es, statusBuilder, clusterNodeSetsResources, actualAutoscalingStatus); err != nil {
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
