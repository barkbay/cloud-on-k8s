// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package rbac

import (
	"fmt"
	"strings"
	"time"

	authorizationapi "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	ServiceAccountUsernamePrefix = "system:serviceaccount:"
)

var log = logf.Log.WithName("access-review")

type AccessReviewer interface {
	// AccessAllowed checks that the given ServiceAccount is allowed to update an other object.
	AccessAllowed(serviceAccount string, sourceNamespace string, object runtime.Object) (bool, error)
}

type subjectAccessReviewer struct {
	client kubernetes.Interface
}

var _ AccessReviewer = &subjectAccessReviewer{}

func NewSubjectAccessReviewer(client kubernetes.Interface) AccessReviewer {
	return &subjectAccessReviewer{
		client: client,
	}
}

func NewYesAccessReviewer() AccessReviewer {
	return &yesAccessReviewer{}
}

func (s *subjectAccessReviewer) AccessAllowed(serviceAccount string, sourceNamespace string, object runtime.Object) (bool, error) {
	if len(serviceAccount) == 0 {
		serviceAccount = "default"
	}

	allErrs := field.ErrorList{}
	for _, msg := range validation.IsDNS1123Subdomain(serviceAccount) { // TODO: should be done in a dedicated place but there's no validation for APM or Kibana yet.
		allErrs = append(allErrs, &field.Error{Type: field.ErrorTypeInvalid, Field: "serviceAccount", BadValue: serviceAccount, Detail: msg})
	}
	if len(allErrs) > 0 {
		return false, allErrs.ToAggregate()
	}

	metaObject, err := meta.Accessor(object)
	if err != nil {
		return false, nil
	}

	plural, err := toPlural(object.GetObjectKind().GroupVersionKind().Kind)
	if err != nil {
		return false, nil
	}

	sar := &authorizationapi.SubjectAccessReview{
		Spec: authorizationapi.SubjectAccessReviewSpec{
			ResourceAttributes: &authorizationapi.ResourceAttributes{
				Namespace: metaObject.GetNamespace(),
				Verb:      "get",
				Resource:  plural,
				//Subresource: "association",
				Group:   strings.ToLower(object.GetObjectKind().GroupVersionKind().Group),
				Version: strings.ToLower(object.GetObjectKind().GroupVersionKind().Version),
				Name:    metaObject.GetName(),
			},
			User: ServiceAccountUsernamePrefix + sourceNamespace + ":" + serviceAccount,
		},
	}

	sar, err = s.client.AuthorizationV1().SubjectAccessReviews().Create(sar)
	if err != nil {
		return false, err
	}
	log.V(1).Info("Access review", "result", sar.Status)
	if sar.Status.Denied {
		return false, nil
	}
	return sar.Status.Allowed, nil
}

// Lazy hack to get the plural form
func toPlural(singular string) (string, error) {
	switch singular {
	case "Elasticsearch":
		return "elasticsearches", nil
	case "Kibana":
		return "kibanas", nil
	case "ApmServer":
		return "apmservers", nil
	}
	return "", fmt.Errorf("unknown singular kind: %s", singular)
}

type yesAccessReviewer struct{}

var _ AccessReviewer = &yesAccessReviewer{}

func (s *yesAccessReviewer) AccessAllowed(_ string, _ string, _ runtime.Object) (bool, error) {
	return true, nil
}

// NextReconciliation returns a reconcile result depending on the implementation of the AccessReviewer.
// It is mostly used when using the subjectAccessReviewer implementation in which case a next reconcile loop should be
// triggered later to keep the association in sync with the RBAC roles and bindings.
// See https://github.com/elastic/cloud-on-k8s/issues/2468#issuecomment-579157063
func NextReconciliation(accessReviewer AccessReviewer) reconcile.Result {
	switch accessReviewer.(type) {
	case *subjectAccessReviewer:
		return reconcile.Result{RequeueAfter: 15 * time.Minute}
	default:
		return reconcile.Result{}
	}
}
