// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package sset

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/expectations"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/hash"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

// ReconcileStatefulSet creates or updates the expected StatefulSet.
func ReconcileStatefulSet(c k8s.Client, es esv1.Elasticsearch, expected appsv1.StatefulSet, expectations *expectations.Expectations) (appsv1.StatefulSet, error) {
	var reconciled appsv1.StatefulSet
	err := reconciler.ReconcileResource(reconciler.Params{
		Client:     c,
		Owner:      &es,
		Expected:   &expected,
		Reconciled: &reconciled,
		NeedsUpdate: func() bool {
			if len(reconciled.Labels) == 0 {
				return true
			}
			return !EqualTemplateHashLabels(expected, reconciled)
		},
		UpdateReconciled: func() {
			expected.DeepCopyInto(&reconciled)
		},
		PreCreate: validatePod(c, expected),
		PreUpdate: validatePod(c, expected),
		PostUpdate: func() {
			if expectations != nil {
				// expect the reconciled StatefulSet to be there in the cache for next reconciliations,
				// to prevent assumptions based on the wrong replica count
				expectations.ExpectGeneration(reconciled)
			}
		},
	})
	return reconciled, err
}

func validatePod(c k8s.Client, expected appsv1.StatefulSet) func() error {
	// Create a dummy Pod with the pod template
	return func() error {
		dummyPod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   expected.Namespace,
				Name:        expected.Name + "-dummy-" + rand.String(5),
				Labels:      expected.Spec.Template.Labels,
				Annotations: expected.Spec.Template.Annotations,
			},
			Spec: expected.Spec.Template.Spec,
		}
		// Dry run is beta and available since Kubernetes 1.13
		if err := c.Create(dummyPod, client.DryRunAll); err != nil {
			// Openshift 3.11 and K8S 1.12 don't support dryRun but gently returns "400 - BadRequest" in that case
			if errors.ReasonForError(err) == metav1.StatusReasonBadRequest {
				return nil

			}
			// If the Pod spec is invalid the expected error is 422 - UNPROCESSABLE ENTITY
			return fmt.Errorf("error while validating Pod for %s/%s: %v", expected.Namespace, expected.Name, err)
		}
		return nil
	}
}

// EqualTemplateHashLabels reports whether actual and expected StatefulSets have the same template hash label value.
func EqualTemplateHashLabels(expected, actual appsv1.StatefulSet) bool {
	return expected.Labels[hash.TemplateHashLabelName] == actual.Labels[hash.TemplateHashLabelName]
}
