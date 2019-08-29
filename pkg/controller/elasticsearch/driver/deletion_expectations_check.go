// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"time"

	"github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
)

type podCheck struct {
	k8s.Client
	ESState
	statefulSets sset.StatefulSetList
	*v1alpha1.Elasticsearch
}

func (p *podCheck) MayBeRemoved(pod *PodWithTTL, ttl time.Duration) (bool, error) {
	// Check the TTL
	if time.Now().After(pod.Time.Add(ttl)) {
		log.V(1).Info(
			"Pod has not been restarted in the expected time",
			"namespace", p.Namespace,
			"es_name", p.Name,
			"pod_name", pod.Name,
		)
		return true, nil
	}

	// Try to get the Pod
	var currentPod corev1.Pod
	err := p.Get(k8s.ExtractNamespacedName(pod), &currentPod)
	if err != nil {
		if errors.IsNotFound(err) {
			// TODO: extract this as a function
			statefulSetName, ordinal, err := sset.StatefulSetName(pod.Name)
			if err != nil {
				return false, err
			}
			statefulSet, found := p.statefulSets.GetByName(statefulSetName)
			if !found {
				// StatefulSet has been deleted
				return true, nil
			}
			if ordinal >= *statefulSet.Spec.Replicas {
				// StatefulSet has been scaled down, this pod is not expected anymore
				return true, nil
			}
		}
		return false, nil
	}

	if currentPod.UID == pod.UID {
		// not deleted
		return false, nil
	}

	// Check if current Pod is healthy
	if healthy, err := podIsHealthy(p.Client, p.ESState, currentPod); !healthy || err != nil {
		return false, err
	}

	// Make sure all nodes scheduled for upgrade are back into the cluster.
	nodesInCluster, err := p.NodeInCluster(pod.Name)
	if err != nil {
		return false, err
	}
	if !nodesInCluster {
		log.V(1).Info(
			"Some upgraded nodes are not back in the cluster yet, keeping shard allocations disabled",
			"namespace", p.Namespace,
			"es_name", p.Name,
		)
		return false, err
	}
	return true, nil
}
