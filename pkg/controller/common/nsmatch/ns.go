// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package nsmatch

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// NamespaceMatcher evaluates a label selector against namespace labels,
// tracks each namespace's match state, and broadcasts to subscribers whenever
// that state flips.
//
// When the selector is nil (disabled) the notifier is a no-op: Matches always
// returns true and Broadcast is a no-op. This preserves legacy / static-
// resolution behaviour without any code changes in callers.
//
// Two namespaces bypass selector evaluation in ObserveNamespace: the empty
// string (cluster-scoped events) and the operator's own namespace — both
// always return true so the operator can reconcile its own resources
// regardless of the configured selector.
//
// Matches returns false for any namespace whose state has not yet been
// recorded by ObserveNamespace. The namespace flip-state controller seeds
// all existing namespaces at startup and re-enqueues
// CRs whenever a namespace's match state changes. Seeding runs concurrently
// with the controllers (the manager does not order runnables), so events
// dropped while a namespace is not yet seeded are backfilled by the broadcast
// its seeding emits.
//
// The match-state map is maintained on every operator replica (the namespace
// controller and its seeder do not require leader election) so that consumers
// that run on non-leaders — the webhook server and the namespace-filtering
// client — see the same state as the leader. Broadcasting to subscribers, in
// contrast, only happens on the elected leader; see Broadcast and SetElected.
type NamespaceMatcher struct {
	selector                labels.Selector
	alwaysManagedNamespaces map[string]struct{} // namespaces excluded from label-selector evaluation.
	matchedNamespacesMutex  sync.Mutex
	matchedNamespaces       map[string]struct{} // namespace name -> present if namespace matches to the selector
	sinksMutex              sync.RWMutex
	sinks                   []*flipSink     // one per subscribed controller, registered when its watch source starts.
	elected                 <-chan struct{} // closed once this replica is elected leader; nil means always elected.
}

// NewNamespaceMatcher returns a NamespaceMatcher. When sel is nil it acts as
// a no-op (Matches always returns true). Both the empty string (cluster-scoped
// resources) and operatorNS are pre-seeded in the short-circuit set so that
// cluster-scoped events and the operator's own namespace always match regardless
// of the configured selector.
func NewNamespaceMatcher(sel labels.Selector, operatorNS string) *NamespaceMatcher {
	return &NamespaceMatcher{
		selector: sel,
		alwaysManagedNamespaces: map[string]struct{}{
			"":         {},
			operatorNS: {},
		},
		matchedNamespaces: map[string]struct{}{},
	}
}

// SetElected provides the manager's election signal (manager.Elected()), a
// channel that is closed once this replica becomes leader (or immediately when
// leader election is disabled). The match-state map is maintained on every
// replica, but the controllers subscribed via Subscribe are leader-election
// runnables: on a non-leader they never consume from their channels, so
// Broadcast must not send. When set, Broadcast drops events until the channel
// is closed. When never set (tests), Broadcast always sends.
func (m *NamespaceMatcher) SetElected(elected <-chan struct{}) {
	m.elected = elected
}

// isElected reports whether this replica may broadcast to subscribers. True
// when no election signal was provided or when the election channel is closed.
//
// The select is deterministic: default only runs when no other case can
// proceed, and a receive from a closed channel always proceeds, so once
// elected is closed this never returns false. A select racing with the close
// itself may still return false; that at worst drops one broadcast at the
// moment of election, which the subscribers' initial sync covers anyway.
func (m *NamespaceMatcher) isElected() bool {
	if m.elected == nil {
		return true
	}
	select {
	case <-m.elected:
		return true
	default:
		return false
	}
}

// SelectorEnabled reports whether the matcher is actively filtering.
// Safe to call on a nil receiver; returns false in that case.
func (m *NamespaceMatcher) SelectorEnabled() bool {
	return m != nil && m.selector != nil
}

// Matches returns the last recorded match state for ns. It returns false for
// any namespace not yet observed by ObserveNamespace. When the selector is
// disabled, always returns true.
func (m *NamespaceMatcher) Matches(ns string) bool {
	if !m.SelectorEnabled() {
		return true
	}

	if _, ok := m.alwaysManagedNamespaces[ns]; ok {
		return true
	}

	m.matchedNamespacesMutex.Lock()
	defer m.matchedNamespacesMutex.Unlock()
	_, match := m.matchedNamespaces[ns]
	return match
}

func (m *NamespaceMatcher) MatchingNamespaces() []string {
	m.matchedNamespacesMutex.Lock()
	defer m.matchedNamespacesMutex.Unlock()

	names := make([]string, 0, len(m.matchedNamespaces)+len(m.alwaysManagedNamespaces))
	for ns := range m.matchedNamespaces {
		names = append(names, ns)
	}
	for ns := range m.alwaysManagedNamespaces {
		if ns == "" {
			continue
		}
		names = append(names, ns)
	}

	return names
}

// ObserveNamespace evaluates the selector against ns's current labels, records
// the result, and returns both the current and previous match states. Short-
// circuited namespaces (empty string and the operator's namespace) always
// return (true, true) without updating the internal state map. When the
// selector is disabled, always returns (true, true).
func (m *NamespaceMatcher) ObserveNamespace(ns *corev1.Namespace) (isMatching bool, wasMatching bool) {
	if !m.SelectorEnabled() {
		return true, true
	}

	if _, ok := m.alwaysManagedNamespaces[ns.Name]; ok {
		return true, true
	}

	isMatching = m.selector.Matches(labels.Set(ns.Labels))

	wasMatching = m.Swap(ns.Name, isMatching)
	return
}

// flipSink is one subscribed controller's connection to the matcher: the
// controller's own workqueue plus the mapper that turns a flipped namespace
// into the reconcile.Requests to enqueue on that queue.
type flipSink struct {
	queue workqueue.TypedRateLimitingInterface[reconcile.Request]
	mapFn func(context.Context, *corev1.Namespace) []reconcile.Request
}

// namespaceFlipSource is a controller-runtime source.Source. controller-runtime
// calls Start with the owning controller's workqueue when the controller starts,
// which is the only point at which that queue becomes available; the source
// registers a sink so Broadcast can enqueue directly onto it.
type namespaceFlipSource struct {
	m     *NamespaceMatcher
	mapFn func(context.Context, *corev1.Namespace) []reconcile.Request
}

// Start registers the sink and deregisters it when the controller's context is
// cancelled. It returns immediately: no goroutine drains anything, delivery is
// driven by Broadcast pushing onto the workqueue.
func (s *namespaceFlipSource) Start(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	sink := &flipSink{queue: q, mapFn: s.mapFn}
	s.m.registerSink(sink)
	go func() {
		<-ctx.Done()
		s.m.removeSink(sink)
	}()
	return nil
}

// FlipSource returns a watch source that, each time Broadcast fires, maps the
// flipped namespace through mapFn and enqueues the resulting requests onto the
// subscribing controller's workqueue. Intended to be passed to controller.Watch.
func (m *NamespaceMatcher) FlipSource(mapFn func(context.Context, *corev1.Namespace) []reconcile.Request) source.Source {
	return &namespaceFlipSource{m: m, mapFn: mapFn}
}

func (m *NamespaceMatcher) registerSink(s *flipSink) {
	m.sinksMutex.Lock()
	defer m.sinksMutex.Unlock()
	m.sinks = append(m.sinks, s)
}

func (m *NamespaceMatcher) removeSink(target *flipSink) {
	m.sinksMutex.Lock()
	defer m.sinksMutex.Unlock()
	for i, s := range m.sinks {
		if s == target {
			m.sinks = append(m.sinks[:i], m.sinks[i+1:]...)
			return
		}
	}
}

// Broadcast maps ns through every registered sink's mapper and enqueues the
// resulting requests onto that sink's controller workqueue. workqueue.Add is
// non-blocking and deduplicates by request key, so a slow reconciler grows its
// own queue depth without blocking Broadcast or any other subscriber, and a
// burst of flips for the same namespace collapses to a single pending item.
//
// On a replica that has not (yet) been elected leader, Broadcast is a no-op:
// subscriber controllers are leader-election runnables, so on a non-leader no
// sink is ever registered and there is nothing to enqueue onto. Skipping is
// safe because the match-state map is written before Broadcast is called and
// carries the state across the election; when this replica becomes leader, each
// subscriber controller's initial sync replays every CR against the already-warm
// map, subsuming any flip skipped here.
func (m *NamespaceMatcher) Broadcast(ctx context.Context, ns *corev1.Namespace) error {
	if !m.SelectorEnabled() || !m.isElected() {
		return nil
	}

	m.sinksMutex.RLock()
	sinks := append([]*flipSink(nil), m.sinks...)
	m.sinksMutex.RUnlock()

	for _, s := range sinks {
		for _, req := range s.mapFn(ctx, ns) {
			s.queue.Add(req)
		}
	}
	return nil
}

// ObserveAndBroadcast calls ObserveNamespace and broadcasts ns to all
// subscribers if the match state flipped. Returns whether the state changed
// and the current match result. When the selector is disabled, ObserveNamespace
// returns (true, true), so stateChanged is always false and no broadcast occurs.
func (m *NamespaceMatcher) ObserveAndBroadcast(ctx context.Context, ns *corev1.Namespace) (stateChanged, isMatching bool, err error) {
	var wasWatching bool
	isMatching, wasWatching = m.ObserveNamespace(ns)
	stateChanged = isMatching != wasWatching
	if stateChanged {
		err = m.Broadcast(ctx, ns)
	}
	return
}

// Swap records isMatching for ns and returns the previously recorded value
// (wasMatching). It is intended for internal use by ObserveNamespace and for
// external test usage; other callers should use ObserveNamespace and
// ForgetNamespace instead.
func (m *NamespaceMatcher) Swap(ns string, isMatching bool) (wasMatching bool) {
	m.matchedNamespacesMutex.Lock()
	defer m.matchedNamespacesMutex.Unlock()
	_, wasMatching = m.matchedNamespaces[ns]
	if isMatching {
		m.matchedNamespaces[ns] = struct{}{}
	} else {
		delete(m.matchedNamespaces, ns)
	}
	return
}

// ForgetNamespace clears any recorded match state for ns without broadcasting.
// Intended for use when ns has been deleted: its resources are being cleaned
// up by their own controllers, so there is nothing for subscribers to react to.
func (m *NamespaceMatcher) ForgetNamespace(ns string) {
	_ = m.Swap(ns, false)
}
