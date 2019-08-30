// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package driver

import (
	"time"

	"github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
)

type podCheck struct {
	k8s.Client
	*v1alpha1.Elasticsearch
}

func (p *podCheck) canBeRemoved(pod *PodWithTTL, ttl time.Duration) (bool, error) {
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
			return true, nil
		}
		return false, nil
	}
	return currentPod.UID != pod.UID, nil
}
