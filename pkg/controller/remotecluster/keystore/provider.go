// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package keystore

import (
	"context"
	"github.com/go-logr/logr"
	"sync"

	esv1 "github.com/elastic/cloud-on-k8s/v2/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v2/pkg/utils/k8s"
	"k8s.io/apimachinery/pkg/types"
)

type pendingChangesPerCluster struct {
	pendingChangesPerCluster map[types.NamespacedName]*pendingChanges
	mu                       sync.RWMutex
}

func NewKeystoreProvider(c k8s.Client) *KeystoreProvider {
	return &KeystoreProvider{
		c: c,
		pendingChangesPerCluster: pendingChangesPerCluster{
			pendingChangesPerCluster: make(map[types.NamespacedName]*pendingChanges),
		},
	}
}

type KeystoreProvider struct {
	c                        k8s.Client
	pendingChangesPerCluster pendingChangesPerCluster
}

func (kp *KeystoreProvider) ForgetCluster(name types.NamespacedName) {
	if kp == nil {
		return
	}
	kp.pendingChangesPerCluster.mu.Lock()
	defer kp.pendingChangesPerCluster.mu.Unlock()
	delete(kp.pendingChangesPerCluster.pendingChangesPerCluster, name)
}

func (kp *KeystoreProvider) ForCluster(ctx context.Context, log logr.Logger, owner *esv1.Elasticsearch) (*APIKeyStore, error) {
	if kp == nil {
		return nil, nil
	}
	name := types.NamespacedName{
		Namespace: owner.Namespace,
		Name:      owner.Name,
	}
	pendingChanges := kp.forCluster(name)
	if pendingChanges != nil {
		return loadAPIKeyStore(ctx, log, kp.c, owner, pendingChanges)
	}
	return loadAPIKeyStore(ctx, log, kp.c, owner, kp.newForCluster(name))
}

func (kp *KeystoreProvider) forCluster(name types.NamespacedName) *pendingChanges {
	if kp == nil {
		return nil
	}
	kp.pendingChangesPerCluster.mu.RLock()
	defer kp.pendingChangesPerCluster.mu.RUnlock()
	return kp.pendingChangesPerCluster.pendingChangesPerCluster[name]
}

func (kp *KeystoreProvider) newForCluster(name types.NamespacedName) *pendingChanges {
	if kp == nil {
		return nil
	}
	kp.pendingChangesPerCluster.mu.Lock()
	defer kp.pendingChangesPerCluster.mu.Unlock()
	// Check if another goroutine did not create the pending changes
	currentPendingChanges := kp.pendingChangesPerCluster.pendingChangesPerCluster[name]
	if currentPendingChanges != nil {
		return currentPendingChanges
	}
	newPendingChanges := &pendingChanges{
		changes: make(map[string]pendingChange),
	}
	kp.pendingChangesPerCluster.pendingChangesPerCluster[name] = newPendingChanges
	return newPendingChanges
}
