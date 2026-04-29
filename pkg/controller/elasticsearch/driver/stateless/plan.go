// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package stateless

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	commondeployment "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/bootstrap"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/deployment"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
)

// Plan is what the driver should do on this reconcile pass, computed as a
// pure function of (observed Deployments, ES spec). The reconcile loop is
// a dumb interpreter of a Plan: reconcile everything in Apply, protect
// everything named in Keep from GC, delete anything observed that isn't
// in either, and requeue if Transitioning.
//
// Two-set split: Apply vs Keep
//
// A single "Desired" set of Deployments would mix two things the reconcile
// loop handles differently:
//
//   - Deployments we want to actively reconcile this pass (Apply): for
//     these we build a fresh template, reconcile the per-NodeSet immutable
//     config secret, and upsert the Deployment. This may trigger a rolling
//     update of the underlying ReplicaSet.
//   - Deployments we observed in the cluster but don't want to touch this
//     pass (Keep): these must not be GC'd even though they may not be in
//     the spec-derived target. The most common reason is the outgoing
//     master tier during a removal: the Deployment is not in the target,
//     but deleting it before the index tier has acquired master eligibility
//     would break quorum.
//
// Keeping them separate lets the loop avoid the "is this a rebuild?
// should I reconcile its config?" decision at every step: Apply is always
// rebuilt; Keep is always left alone.
type Plan struct {
	// Apply is the set of NodeSet resources to actively reconcile this
	// pass (config secret + Deployment upsert).
	Apply []nodeSetResources
	// Keep is the set of observed Deployment names to protect from GC
	// even though they're not in Apply. Empty in steady state.
	Keep []string
	// Transitioning signals that the driver must requeue: the cluster
	// state is still moving toward the spec-derived target and we want
	// the next reconcile pass to observe the next step promptly (rather
	// than wait on a Kubernetes resource event).
	Transitioning bool
	// Reason is a short log-ready description of why this pass is not at
	// the final target. Empty iff Transitioning is false.
	Reason string
}

// plan computes the Plan for this reconcile pass. It is intentionally a
// thin wrapper so call sites read as "plan what to do now" rather than
// "run the state machine".
//
// Inputs:
//   - observed: the Deployments the API server currently reports for this
//     cluster.
//   - target:   the full spec-derived target (freshly built this pass).
//
// The decision tree below encodes the driver's two ordering constraints:
//
//  1. Dedicated-master-tier lifecycle (add / remove). Master-quorum safety
//     trumps everything else: while a master tier is being added, only the
//     master tier rolls; while one is being removed, only the index tier
//     rolls (and the outgoing master is held). Once the priority tier has
//     rolled out, the remaining tiers are pushed in a single pass.
//
//  2. Version upgrade ordering. Outside of a master-tier transition, when
//     a version upgrade is pending, the search tier rolls first (stateless
//     readers must be at the new version before writers switch).
//
// Any other case is steady state: push the full target, let the upsert
// diff against observed, trust Kubernetes to no-op identical templates.
func (d *Driver) plan(
	ctx context.Context,
	observed []appsv1.Deployment,
	target []nodeSetResources,
) Plan {
	// Bootstrap: no observed Deployments yet, push the full target in
	// one shot. Nothing to keep, nothing to hold back.
	if len(observed) == 0 {
		return Plan{Apply: target}
	}

	targetDeployments := deploymentsOf(target)
	masterTier := d.newMasterTierState(observed, targetDeployments)

	switch {
	case masterTier.adding():
		// Hold the index tier (it currently carries master role in its
		// config) and push only the new master tier. Once the master
		// tier is rolled out, adding() flips to false and the next pass
		// moves the other tiers to their target.
		return stagePriorityTier(target, observed, esv1.MasterTier, masterTier.reason())

	case masterTier.removing():
		// Push the index tier (target now re-includes the master role)
		// and hold the outgoing master tier through GC. Search is held
		// as well: we bring master eligibility back on the index tier
		// first, then deal with search on the next pass. The outgoing
		// master Deployment is automatically included in Keep because
		// it is observed but not in the priority (index) tier.
		return stagePriorityTier(target, observed, esv1.IndexTier, masterTier.reason())

	case masterTier.removalFinishing():
		// Index tier is at target, master is still observed but not in
		// target → push the full target, GC will delete the outgoing
		// master. We still flag Transitioning so the next reconcile
		// verifies the master is gone.
		return Plan{
			Apply:         target,
			Transitioning: true,
			Reason:        masterTier.reason(),
		}
	}

	// Version upgrade ordering: only active outside a master-tier
	// transition. Uses the same check as the stateful driver
	// (ShouldGroupDeploymentReconciliation), with "statelessHasMasterRole"
	// telling it which tier carries the master role under the current spec.
	if deployment.ShouldGroupDeploymentReconciliation(
		ctx,
		observed,
		targetDeployments,
		bootstrap.AnnotatedForBootstrap(d.ES),
		statelessHasMasterRole(d.ES),
	) {
		return stagePriorityTier(target, observed, esv1.SearchTier, "version upgrade in progress or pending (waiting for search tier)")
	}

	// Steady state: push the full target, GC anything observed that
	// no longer corresponds to a NodeSet.
	return Plan{Apply: target}
}

// stagePriorityTier returns the Plan for one staged rollout pass.
//
// When the priority tier is not yet at the target template hash or not
// rolled out, it emits only the priority tier's target resources in
// Apply, and lists every *other* observed Deployment name in Keep so
// nothing else is touched or GC'd.
//
// Once the priority tier is at target hash AND rolled out, it emits the
// full target in Apply so the trailing tiers can proceed in the same
// pass. This mirrors the "if group 0 rolled out, reconcile group 1 in
// the same pass" behaviour of the old group loop.
func stagePriorityTier(
	target []nodeSetResources,
	observed []appsv1.Deployment,
	priorityTier esv1.StatelessTier,
	reason string,
) Plan {
	priority := filterByTier(target, priorityTier)
	if priorityAtTarget(observed, priority) {
		return Plan{
			Apply:         target,
			Transitioning: true,
			Reason:        reason + " (priority rolled out, pushing remaining tiers)",
		}
	}
	keep := []string{}
	priorityNames := map[string]struct{}{}
	for i := range priority {
		priorityNames[priority[i].deployment.Name] = struct{}{}
	}
	for i := range observed {
		if _, isPriority := priorityNames[observed[i].Name]; isPriority {
			continue
		}
		keep = append(keep, observed[i].Name)
	}
	return Plan{
		Apply:         priority,
		Keep:          keep,
		Transitioning: true,
		Reason:        reason,
	}
}

// priorityAtTarget reports whether every priority-tier Deployment is
// observed at the expected template hash AND rolled out. If the priority
// tier is absent from observed (e.g. master tier while adding), this
// returns false: we haven't finished the first step yet.
func priorityAtTarget(observed []appsv1.Deployment, priority []nodeSetResources) bool {
	if len(priority) == 0 {
		// Nothing to push as priority: trivially "done".
		return true
	}
	expectedHash := map[string]string{}
	for i := range priority {
		hashed := commondeployment.WithTemplateHash(priority[i].deployment)
		expectedHash[priority[i].deployment.Name] = hash.GetTemplateHashLabel(hashed.Labels)
	}
	matched := 0
	for i := range observed {
		want, ok := expectedHash[observed[i].Name]
		if !ok {
			continue
		}
		if hash.GetTemplateHashLabel(observed[i].Labels) != want {
			return false
		}
		if !deployment.IsRolledOut(&observed[i]) {
			return false
		}
		matched++
	}
	return matched == len(expectedHash)
}

// filterByTier returns the subset of resources whose tier matches.
func filterByTier(resources []nodeSetResources, tier esv1.StatelessTier) []nodeSetResources {
	out := make([]nodeSetResources, 0, len(resources))
	for i := range resources {
		if resources[i].tier == tier {
			out = append(out, resources[i])
		}
	}
	return out
}

// deploymentsOf flattens per-NodeSet resources into the slice of
// Deployments used by plan() for hash/rollout comparisons.
func deploymentsOf(resources []nodeSetResources) []appsv1.Deployment {
	out := make([]appsv1.Deployment, 0, len(resources))
	for i := range resources {
		out = append(out, resources[i].deployment)
	}
	return out
}

// planForReconcile is a thin adapter that builds the spec-derived target
// once and passes it to plan. It exists so reconcileNodeSets doesn't need
// to know about the build step ordering.
func (d *Driver) planForReconcile(ctx context.Context, meta metadata.Metadata) (Plan, []appsv1.Deployment, error) {
	target, err := d.buildAllNodeSetResources(ctx, meta)
	if err != nil {
		return Plan{}, nil, err
	}
	observed, err := d.observedDeployments(ctx)
	if err != nil {
		return Plan{}, nil, err
	}
	return d.plan(ctx, observed, target), observed, nil
}

// observedDeployments returns the existing Deployments belonging to this
// Elasticsearch cluster, matched by the cluster-name and deployment-name
// labels used for stateless Deployments.
func (d *Driver) observedDeployments(ctx context.Context) ([]appsv1.Deployment, error) {
	var list appsv1.DeploymentList
	if err := d.Client.List(ctx, &list,
		client.InNamespace(d.ES.Namespace),
		label.NewLabelSelectorForElasticsearchClusterName(d.ES.Name),
		client.HasLabels{label.DeploymentNameLabelName},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}
