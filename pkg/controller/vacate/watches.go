// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package vacate

import (
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
)

// AddWatches set watches on objects needed to manage the nodeSets to be added to implement grow-and-shrink.
func AddWatches(c controller.Controller) error {
	// Watch for changes on Elasticsearch clusters
	if err := c.Watch(&source.Kind{Type: &esv1.Elasticsearch{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// Watch PVC that contain remote certificate authorities managed by this controller
	return c.Watch(&source.Kind{Type: &v1.PersistentVolumeClaim{}}, &handler.EnqueueRequestForOwner{
		IsController: false,
		OwnerType:    &esv1.Elasticsearch{},
	})

}
