// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package association

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/watches"
)

// Watch names below are based on the associated resource (e.g., Kibana), not the referenced resource (e.g., Elasticsearch).
// This means watch names don't need to include Kind because:
// 1. Each association type (es-monitoring, ess-monitoring, kb-es, etc.) has its own controller with separate DynamicWatches
// 2. A single watch can contain multiple KindNamespacedName entries in its Watched slice
// 3. Kind filtering happens at event-handler time via apiutil.GVKForObject, not at watch registration time
//
// Example: If both Elasticsearch "quickstart" and ElasticsearchStateless "quickstart" exist with monitoring associations,
// they are handled by different controllers (es-monitoring vs ess-monitoring), each with its own watches instance.
// Even though the watch names are identical, there's no collision because they're in separate DynamicWatches.

// referencedResourceWatchName is the name of the watch set on the referenced resource.
func referencedResourceWatchName(associated types.NamespacedName) string {
	return fmt.Sprintf("%s-%s-referenced-resource-watch", associated.Namespace, associated.Name)
}

// referencedResourceCASecretWatchName is the name of the watch set on Secret containing the CA of the referenced resource.
func referencedResourceCASecretWatchName(associated types.NamespacedName) string {
	return fmt.Sprintf("%s-%s-referenced-resource-ca-secret-watch", associated.Namespace, associated.Name)
}

// esUserWatchName returns the name of the watch setup on the ES user secret.
func esUserWatchName(associated types.NamespacedName) string {
	return fmt.Sprintf("%s-%s-es-user-watch", associated.Namespace, associated.Name)
}

// serviceWatchName returns the name of the watch setup on the custom service to be used to make requests to the
// referenced resource.
func serviceWatchName(associated types.NamespacedName) string {
	return fmt.Sprintf("%s-%s-svc-watch", associated.Namespace, associated.Name)
}

// additionalSecretWatchName returns the name of the watch setup on any additional secrets that
// are copied during the association reconciliation.
func additionalSecretWatchName(associated types.NamespacedName) string {
	return fmt.Sprintf("%s-%s-secrets-watch", associated.Namespace, associated.Name)
}

// reconcileWatches sets up dynamic watches for:
//   - the referenced resource(s) managed or not by ECK (e.g. Elasticsearch or ElasticsearchStateless for Kibana -> ES associations).
//     For managed resources, ReconcileWatchWithKind is used to include the Kind in each watched entry. This ensures that when
//     both Elasticsearch "foo" and ElasticsearchStateless "foo" exist, only changes to the specifically referenced Kind trigger
//     a reconciliation. The Kind is resolved at event time using apiutil.GVKForObject with the scheme.
//   - the CA secret of the referenced resource in the referenced resource namespace
//   - the referenced service to access the referenced resource
//   - the referenced secret to access the referenced resource
//   - if there's an ES user to create, watch the user Secret in ES namespace
//
// All watches for all given associations are set under the same watch name and replaced with each reconciliation.
// The given associations are expected to be of the same type (e.g. Kibana -> Elasticsearch, not Kibana -> Enterprise Search).
func (r *Reconciler) reconcileWatches(ctx context.Context, associated types.NamespacedName, associations []commonv1.Association) error {
	managedElasticRef := filterManagedElasticRef(associations)
	unmanagedElasticRef := filterUnmanagedElasticRef(associations)

	// we have 2 modes (exclusive) for the referenced resource: managed or not managed by ECK and referencedResourceWatchName is shared between both.
	// either watch the referenced resource managed by ECK (with Kind for precise matching)
	if err := ReconcileWatchWithKind(associated, managedElasticRef, r.watches.ReferencedResources, referencedResourceWatchName(associated), r.scheme, func(association commonv1.Association) commonv1.KindNamespacedName {
		ref := association.AssociationRef()
		return commonv1.KindNamespacedName{
			// Kind is set to distinguish between Elasticsearch and ElasticsearchStateless when both exist
			// with the same name. Without Kind, a change to either would trigger reconciliation.
			Kind:      association.AssociationRefKind(),
			Namespace: ref.Namespace,
			Name:      ref.NameOrSecretName(),
		}
	}); err != nil {
		return err
	}
	// or watch the custom user secret that describes how to connect to the referenced resource not managed by ECK
	if err := ReconcileWatch(associated, unmanagedElasticRef, r.watches.Secrets, referencedResourceWatchName(associated), func(association commonv1.Association) types.NamespacedName {
		return association.AssociationRef().NamespacedName()
	}); err != nil {
		return err
	}

	// watch the CA secret of the referenced resource in the referenced resource namespace
	if err := ReconcileWatch(associated, managedElasticRef, r.watches.Secrets, referencedResourceCASecretWatchName(associated), func(association commonv1.Association) types.NamespacedName {
		ref := association.AssociationRef()
		kind := association.AssociationRefKind()
		namer := r.AssociationInfo.ReferencedResourceNamer(kind)
		return types.NamespacedName{
			Name:      certificates.PublicCertsSecretName(namer, ref.NameOrSecretName()),
			Namespace: ref.Namespace,
		}
	}); err != nil {
		return err
	}

	// watch the custom services users may have setup to be able to react to updates on services that are not error related
	// (error related updates are covered by re-queueing on unsuccessful reconciliation)
	if err := ReconcileWatch(associated, filterWithServiceName(associations), r.watches.Services, serviceWatchName(associated), func(association commonv1.Association) types.NamespacedName {
		ref := association.AssociationRef()
		return types.NamespacedName{
			Name:      ref.ServiceName,
			Namespace: ref.Namespace,
		}
	}); err != nil {
		return err
	}

	// watch the Elasticsearch user secret in the Elasticsearch namespace, if needed
	if r.ElasticsearchUserCreation != nil {
		if err := ReconcileWatch(associated, managedElasticRef, r.watches.Secrets, esUserWatchName(associated), func(association commonv1.Association) types.NamespacedName {
			return UserKey(association, association.AssociationRef().Namespace, r.ElasticsearchUserCreation.UserSecretSuffix)
		}); err != nil {
			return err
		}
	}

	if r.AdditionalSecrets != nil {
		if err := reconcileAdditionalSecretsWatch(ctx, r, associated, associations); err != nil {
			return err
		}
	}

	return nil
}

func reconcileAdditionalSecretsWatch(ctx context.Context, r *Reconciler, associated types.NamespacedName, associations []commonv1.Association) error {
	managedElasticRef := filterManagedElasticRef(associations)
	if len(managedElasticRef) == 0 {
		RemoveWatch(r.watches.Secrets, additionalSecretWatchName(associated))
		return nil
	}

	var toWatch []commonv1.KindNamespacedName
	for _, association := range associations {
		secs, err := r.AdditionalSecrets(ctx, r.Client, association)
		if err != nil {
			return err
		}
		// Watch the source secrets
		for _, sec := range secs {
			toWatch = append(toWatch, commonv1.KindNamespacedName{Namespace: sec.Namespace, Name: sec.Name})
		}
		// Also watch the target secrets
		for _, sec := range secs {
			toWatch = append(toWatch, commonv1.KindNamespacedName{
				Name:      sec.Name,
				Namespace: association.GetNamespace(),
			})
		}
	}

	return r.watches.Secrets.AddHandler(watches.NamedWatch[*corev1.Secret]{
		Name:    additionalSecretWatchName(associated),
		Watched: toWatch,
		Watcher: associated,
	})
}

// ReconcileWatch sets or removes `watchName` watch in `dynamicRequest` based on `associated` and `associations` and
// `watchedFunc`. No watch is added if watchedFunc(association) refers to an empty namespaced name.
// This version does not include Kind filtering - use ReconcileWatchWithKind when Kind disambiguation is needed.
func ReconcileWatch[T client.Object](
	associated types.NamespacedName,
	associations []commonv1.Association,
	dynamicRequest *watches.DynamicEnqueueRequest[T],
	watchName string,
	watchedFunc func(association commonv1.Association) types.NamespacedName,
) error {
	if len(associations) == 0 {
		RemoveWatch(dynamicRequest, watchName)
		return nil
	}

	toWatch := make([]commonv1.KindNamespacedName, 0, len(associations))
	for _, association := range associations {
		watched := watchedFunc(association)
		if watched.Name != "" {
			toWatch = append(toWatch, commonv1.NewKindNamespacedName(watched))
		}
	}

	return dynamicRequest.AddHandler(watches.NamedWatch[T]{
		Name:    watchName,
		Watched: toWatch,
		Watcher: associated,
	})
}

// ReconcileWatchWithKind sets or removes `watchName` watch in `dynamicRequest` based on `associated` and `associations` and
// `watchedFunc`. No watch is added if watchedFunc(association) refers to an empty name.
// This version includes Kind in the watch for precise matching when multiple resource types may share the same name/namespace.
// The scheme is required for Kind resolution using apiutil.GVKForObject.
func ReconcileWatchWithKind[T client.Object](
	associated types.NamespacedName,
	associations []commonv1.Association,
	dynamicRequest *watches.DynamicEnqueueRequest[T],
	watchName string,
	scheme *runtime.Scheme,
	watchedFunc func(association commonv1.Association) commonv1.KindNamespacedName,
) error {
	if len(associations) == 0 {
		RemoveWatch(dynamicRequest, watchName)
		return nil
	}

	toWatch := make([]commonv1.KindNamespacedName, 0, len(associations))
	for _, association := range associations {
		watched := watchedFunc(association)
		if watched.Name != "" {
			toWatch = append(toWatch, watched)
		}
	}

	return dynamicRequest.AddHandler(watches.NamedWatch[T]{
		Name:    watchName,
		Watched: toWatch,
		Watcher: associated,
		Scheme:  scheme,
	})
}

// RemoveWatch removes `watchName` watch from `dynamicRequest`.
func RemoveWatch[T client.Object](dynamicRequest *watches.DynamicEnqueueRequest[T], watchName string) {
	dynamicRequest.RemoveHandlerForKey(watchName)
}

func (r *Reconciler) removeWatches(associated types.NamespacedName) {
	// - referenced resource
	RemoveWatch(r.watches.ReferencedResources, referencedResourceWatchName(associated))
	// - CA secret in referenced resource namespace
	RemoveWatch(r.watches.Secrets, referencedResourceCASecretWatchName(associated))
	// - custom service watch in resource namespace
	RemoveWatch(r.watches.Services, serviceWatchName(associated))
	// - ES user secret
	RemoveWatch(r.watches.Secrets, esUserWatchName(associated))
	// - Additional secrets (typically in the case of Agent -> Fleet Server -> Elasticsearch)
	RemoveWatch(r.watches.Secrets, additionalSecretWatchName(associated))
}
