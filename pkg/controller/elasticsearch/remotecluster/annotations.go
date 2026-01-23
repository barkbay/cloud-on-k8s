// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package remotecluster

import (
	"context"
	"sort"
	"strings"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

const (
	// ManagedRemoteClustersAnnotationName holds the list of the remote clusters which have been created
	ManagedRemoteClustersAnnotationName = "elasticsearch.k8s.elastic.co/managed-remote-clusters"
)

// getRemoteClustersInAnnotation returns a set that contains a list of remote clusters that may have been declared in Elasticsearch.
// A map is returned here to quickly compare with the ones that are new or missing.
// If there's no remote clusters the map is empty but not nil.
func getRemoteClustersInAnnotation(es escommon.ElasticsearchCluster) map[string]struct{} {
	remoteClusters := make(map[string]struct{})
	annotations := es.GetAnnotations()
	if annotations == nil {
		return remoteClusters
	}
	serializedRemoteClusters, ok := annotations[ManagedRemoteClustersAnnotationName]
	if !ok || strings.TrimSpace(serializedRemoteClusters) == "" {
		return remoteClusters
	}
	for _, remoteClusterInAnnotation := range strings.Split(serializedRemoteClusters, ",") {
		remoteClusters[remoteClusterInAnnotation] = struct{}{}
	}
	return remoteClusters
}

func annotateWithCreatedRemoteClusters(ctx context.Context, c k8s.Client, es escommon.ElasticsearchCluster, remoteClusters map[string]struct{}) error {
	annotations := es.GetAnnotations()
	if len(remoteClusters) == 0 {
		// if there are no annotations, there's nothing to do
		if len(annotations) == 0 {
			return nil
		}

		// if the annotation exists, delete it
		if _, ok := annotations[ManagedRemoteClustersAnnotationName]; ok {
			delete(annotations, ManagedRemoteClustersAnnotationName)
			es.SetAnnotations(annotations)
			return c.Update(ctx, es)
		}

		return nil
	}

	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotation := make([]string, 0, len(remoteClusters))
	for remoteCluster := range remoteClusters {
		annotation = append(annotation, remoteCluster)
	}

	sort.Strings(annotation)
	expected := strings.Join(annotation, ",")
	current, ok := annotations[ManagedRemoteClustersAnnotationName]

	if !ok || current != expected {
		annotations[ManagedRemoteClustersAnnotationName] = expected
		es.SetAnnotations(annotations)
		return c.Update(ctx, es)
	}
	return nil
}
