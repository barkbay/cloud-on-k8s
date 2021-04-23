// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package autoscaler

import (
	"context"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/elasticsearch/status"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Context contains the required objects used by the autoscaler functions.
type Context struct {
	Log logr.Logger
	// AutoscalingSpec is the autoscaling specification as provided by the user.
	AutoscalingSpec esv1.AutoscalingPolicySpec
	// NodeSets is the list of the NodeSets managed by the autoscaling specification.
	NodeSets esv1.NodeSetList
	// CurrentAutoscalingStatus is the current resources status as stored in the Elasticsearch resource.
	CurrentAutoscalingStatus status.Status
	// AutoscalingPolicyResult contains the Elasticsearch Autoscaling API result.
	AutoscalingPolicyResult esclient.AutoscalingPolicyResult
	// StatusBuilder is used to track any event that should be surfaced to the user.
	StatusBuilder *status.AutoscalingStatusBuilder
	// PersistentVolumesStatus holds the capacity currently available.
	PersistentVolumesStatus PersistentVolumesStatus
}

// PersistentVolumeStatus holds the current PersistentVolumeClaim request and status.
// We need this status to take some decisions based on the actual storage capacity which may be larger then the request.
type PersistentVolumeStatus struct {
	Request  resource.Quantity
	Capacity resource.Quantity
}

func NewContext(
	k8sClient k8s.Client,
	es esv1.Elasticsearch,
	log logr.Logger,
	autoscalingSpec esv1.AutoscalingPolicySpec,
	nodeSets esv1.NodeSetList,
	currentAutoscalingStatus status.Status,
	autoscalingPolicyResult esclient.AutoscalingPolicyResult,
	statusBuilder *status.AutoscalingStatusBuilder,
) (*Context, error) {
	persistentVolumeStatus, err := getPersistentVolumesStatus(k8sClient, es, nodeSets)
	if err != nil {
		return nil, err
	}
	return &Context{
		Log:                      log,
		AutoscalingSpec:          autoscalingSpec,
		NodeSets:                 nodeSets,
		CurrentAutoscalingStatus: currentAutoscalingStatus,
		AutoscalingPolicyResult:  autoscalingPolicyResult,
		StatusBuilder:            statusBuilder,
		PersistentVolumesStatus:  persistentVolumeStatus,
	}, nil
}

type PersistentVolumesStatus []PersistentVolumeStatus

func getPersistentVolumesStatus(k8sClient k8s.Client, es esv1.Elasticsearch, nodeSets esv1.NodeSetList) (PersistentVolumesStatus, error) {
	var pvcs corev1.PersistentVolumeClaimList
	var persistentVolumesStatus PersistentVolumesStatus
	ns := client.InNamespace(es.Namespace)
	for _, nodeSet := range nodeSets {
		statefulSetName := esv1.StatefulSet(es.Name, nodeSet.Name)
		matchLabels := label.NewLabelSelectorForStatefulSetName(es.Name, statefulSetName)
		if err := k8sClient.List(context.Background(), &pvcs, ns, matchLabels); err != nil {
			return PersistentVolumesStatus{}, err
		}
		for _, _ = range pvcs.Items {
			/*persistentVolumesStatus = append(persistentVolumesStatus, PersistentVolumeStatus{
				Request:  pvc.Spec.Resources.Requests.Storage(),
				Capacity: resource.Quantity{},
			})*/
		}

	}
	return persistentVolumesStatus, nil
}
