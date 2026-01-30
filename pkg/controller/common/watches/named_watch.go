// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package watches

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
)

// NamedWatch is an event handler that allows watching a specific resource identified by
// Watched. Events will be handled by Watcher.
type NamedWatch[T client.Object] struct {
	// Name identifies this watch for easier removal and deduplication.
	Name string
	// Watched are the resources being watched. The Kind field can be used for disambiguation
	// when multiple resource types may share the same name and namespace.
	// If Kind is empty, the watch will match any Kind with the same name and namespace.
	Watched []commonv1.KindNamespacedName
	// Watcher is the receiver of the reconcile.Request
	Watcher types.NamespacedName
	// Scheme is used to determine the Kind of objects when Kind filtering is enabled.
	// Required when any Watched entry has a non-empty Kind field.
	Scheme *runtime.Scheme
}

var _ handler.EventHandler = &NamedWatch[client.Object]{}

func (w NamedWatch[T]) Create(_ context.Context, evt event.TypedCreateEvent[T], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, req := range w.toReconcileRequest(evt.Object) {
		q.Add(req)
	}
}

func (w NamedWatch[T]) Update(_ context.Context, evt event.TypedUpdateEvent[T], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, req := range w.toReconcileRequest(evt.ObjectOld) {
		q.Add(req)
	}
	for _, req := range w.toReconcileRequest(evt.ObjectNew) {
		q.Add(req)
	}
}

func (w NamedWatch[T]) Delete(_ context.Context, evt event.TypedDeleteEvent[T], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, req := range w.toReconcileRequest(evt.Object) {
		q.Add(req)
	}
}

func (w NamedWatch[T]) Generic(_ context.Context, evt event.TypedGenericEvent[T], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, req := range w.toReconcileRequest(evt.Object) {
		q.Add(req)
	}
}

func (w NamedWatch[T]) EventHandler() handler.TypedEventHandler[T, reconcile.Request] {
	return w
}

// Key identifies this transformer.
func (w NamedWatch[T]) Key() string {
	return w.Name
}

// EventHandler transforms the event for object to one or many reconcile.Request if relevant.
func (w NamedWatch[T]) toReconcileRequest(object metav1.Object) []reconcile.Request {
	for _, watched := range w.Watched {
		if object.GetName() == watched.Name && object.GetNamespace() == watched.Namespace {
			// If Kind is specified, only match if it matches the object's Kind
			if watched.Kind != "" {
				// Use the scheme to get the GVK for the object.
				// GetObjectKind().GroupVersionKind().Kind is often empty for objects from the cache
				// because TypeMeta is stripped by the API server during decoding.
				runtimeObj, ok := object.(runtime.Object)
				if !ok {
					// Can't determine Kind, skip this match
					continue
				}
				if w.Scheme == nil {
					// Kind filtering requested but no scheme provided - this is a programming error.
					// Skip the match to avoid incorrect reconciliations.
					log.Error(nil, "NamedWatch has Kind filter but no Scheme - skipping match",
						"watch", w.Name, "kind", watched.Kind, "name", watched.Name, "namespace", watched.Namespace)
					continue
				}
				gvk, err := apiutil.GVKForObject(runtimeObj, w.Scheme)
				if err != nil {
					log.Error(err, "Failed to get GVK for object", "watch", w.Name)
					continue
				}
				if gvk.Kind != watched.Kind {
					continue
				}
			}
			return []reconcile.Request{
				{
					NamespacedName: w.Watcher,
				},
			}
		}
	}
	return nil
}

var _ HandlerRegistration[client.Object] = &NamedWatch[client.Object]{}
