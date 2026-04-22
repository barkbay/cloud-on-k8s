// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package fixtures provides test-only builders and helpers for the stateless
// Elasticsearch driver. It is organised by concern:
//
//   - es.go:         Elasticsearch + NodeSet builders and options.
//   - driver.go:     stateless.Driver assembly with a fake client.
//   - deployment.go: Deployment fixtures used to seed observed state.
//   - query.go:      small accessors/assertions against the fake client.
//   - snapshot.go:   JSON projection of the cluster used for snapshot tests.
//
// Everything exported here is intended for stateless-driver tests; helpers
// that need access to package-private types stay inside the stateless test
// package itself.
package fixtures

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/bootstrap"
)

// DefaultTestVersion is the ES version used as the reconciliation target in
// tests when the specific version under reconciliation is not the focus of
// the test.
var DefaultTestVersion = version.MustParse("9.3.0")

// ESOption is a functional option for NewStatelessES.
type ESOption func(*esv1.Elasticsearch)

// WithObjectStore installs an ObjectStoreConfig on the cluster. Pass an
// empty esv1.ObjectStoreConfig{} (or a config that fails validation
// downstream) to exercise the missing-object-store path.
func WithObjectStore(cfg esv1.ObjectStoreConfig) ESOption {
	return func(es *esv1.Elasticsearch) { es.Spec.ObjectStore = &cfg }
}

// WithRevisionHistoryLimit overrides the top-level Elasticsearch
// RevisionHistoryLimit. Passing nil keeps the default (0).
func WithRevisionHistoryLimit(limit *int32) ESOption {
	return func(es *esv1.Elasticsearch) { es.Spec.RevisionHistoryLimit = limit }
}

// WithZoneAwareness enables cluster-wide zone awareness with the default
// topology key by attaching a ZoneAwareness block to every NodeSet.
func WithZoneAwareness() ESOption {
	return func(es *esv1.Elasticsearch) {
		for i := range es.Spec.NodeSets {
			es.Spec.NodeSets[i].ZoneAwareness = &esv1.ZoneAwareness{}
		}
	}
}

// WithBootstrapped annotates the ES resource as already bootstrapped, which
// enables tier-ordered grouped reconciliation.
func WithBootstrapped(uuid string) ESOption {
	return func(es *esv1.Elasticsearch) {
		if es.Annotations == nil {
			es.Annotations = map[string]string{}
		}
		es.Annotations[bootstrap.ClusterUUIDAnnotationName] = uuid
	}
}

// NewStatelessES builds a minimal stateless Elasticsearch fixture usable by
// buildNodeSetResources and reconcileNodeSets. A valid ObjectStore is
// installed by default so tests that do not care about that detail can stay
// concise; use WithObjectStore (with an empty config) to exercise the
// missing-object-store path.
func NewStatelessES(name string, nodeSets []esv1.NodeSet, opts ...ESOption) esv1.Elasticsearch {
	es := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: esv1.ElasticsearchSpec{
			Version:  DefaultTestVersion.String(),
			Mode:     esv1.ElasticsearchModeStateless,
			NodeSets: nodeSets,
			ObjectStore: &esv1.ObjectStoreConfig{
				Type:   esv1.ObjectStoreTypeS3,
				Bucket: "test-bucket",
			},
		},
	}
	for _, opt := range opts {
		opt(&es)
	}
	return es
}

// NodeSet builds a stateless NodeSet fixture with the given name, count, and
// tier. Name prefix + tier are intentionally redundant so tests can exercise
// either field without needing a separate constructor.
func NodeSet(name string, count int32, tier esv1.StatelessTier) esv1.NodeSet {
	return esv1.NodeSet{
		Name:  name,
		Count: count,
		Tier:  tier,
	}
}

// NodeSetWithConfig returns NodeSet with the given commonv1.Config applied
// as user config overrides.
func NodeSetWithConfig(name string, count int32, tier esv1.StatelessTier, cfg map[string]any) esv1.NodeSet {
	ns := NodeSet(name, count, tier)
	ns.Config = &commonv1.Config{Data: cfg}
	return ns
}
