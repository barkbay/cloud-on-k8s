// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package keystore

import (
	"context"
	"sync"

	"github.com/go-logr/logr"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

type pendingChangesPerCluster struct {
	pendingChangesPerCluster map[commonv1.KindNamespacedName]*pendingChanges
	mu                       sync.RWMutex
}

func NewProvider(c k8s.Client) *Provider {
	return &Provider{
		c: c,
		pendingChangesPerCluster: pendingChangesPerCluster{
			pendingChangesPerCluster: make(map[commonv1.KindNamespacedName]*pendingChanges),
		},
	}
}

type Provider struct {
	c                        k8s.Client
	pendingChangesPerCluster pendingChangesPerCluster
}

// ForgetCluster removes pending changes for a cluster identified by namespace, name and kind.
func (p *Provider) ForgetCluster(key commonv1.KindNamespacedName) {
	if p == nil {
		return
	}
	p.pendingChangesPerCluster.mu.Lock()
	defer p.pendingChangesPerCluster.mu.Unlock()
	delete(p.pendingChangesPerCluster.pendingChangesPerCluster, key)
}

func (p *Provider) ForCluster(ctx context.Context, log logr.Logger, owner escommon.ElasticsearchCluster) (*APIKeyStore, error) {
	if p == nil {
		return nil, nil
	}
	key := kindNamespacedNameFor(owner)
	pendingChanges := p.forCluster(key)
	if pendingChanges != nil {
		return loadAPIKeyStore(ctx, log, p.c, owner, pendingChanges)
	}
	return loadAPIKeyStore(ctx, log, p.c, owner, p.newForCluster(key))
}

// kindNamespacedNameFor creates a KindNamespacedName from an ElasticsearchCluster.
func kindNamespacedNameFor(cluster escommon.ElasticsearchCluster) commonv1.KindNamespacedName {
	kind := commonv1.ElasticsearchKind
	if cluster.IsStateless() {
		kind = commonv1.ElasticsearchStatelessKind
	}
	return commonv1.KindNamespacedName{
		Kind:      kind,
		Namespace: cluster.GetNamespace(),
		Name:      cluster.GetName(),
	}
}

func (p *Provider) forCluster(key commonv1.KindNamespacedName) *pendingChanges {
	if p == nil {
		return nil
	}
	p.pendingChangesPerCluster.mu.RLock()
	defer p.pendingChangesPerCluster.mu.RUnlock()
	return p.pendingChangesPerCluster.pendingChangesPerCluster[key]
}

func (p *Provider) newForCluster(key commonv1.KindNamespacedName) *pendingChanges {
	if p == nil {
		return nil
	}
	p.pendingChangesPerCluster.mu.Lock()
	defer p.pendingChangesPerCluster.mu.Unlock()
	// Check if another goroutine did not create the pending changes
	currentPendingChanges := p.pendingChangesPerCluster.pendingChangesPerCluster[key]
	if currentPendingChanges != nil {
		return currentPendingChanges
	}
	newPendingChanges := &pendingChanges{
		changes: make(map[string]pendingChange),
	}
	p.pendingChangesPerCluster.pendingChangesPerCluster[key] = newPendingChanges
	return newPendingChanges
}
