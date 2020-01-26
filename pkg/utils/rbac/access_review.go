package rbac

import (
	"fmt"
	"strings"

	authorizationapi "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
				Verb:      "update",
				Resource:  plural,
				Group:     strings.ToLower(object.GetObjectKind().GroupVersionKind().Group),
				Version:   strings.ToLower(object.GetObjectKind().GroupVersionKind().Version),
				Name:      metaObject.GetName(),
			},
			User: ServiceAccountUsernamePrefix + sourceNamespace + ":" + serviceAccount,
		},
	}

	sar, err = s.client.AuthorizationV1().SubjectAccessReviews().Create(sar)
	if err != nil {
		return false, err
	}
	log.Info("Access review", "result", sar.Status)
	if sar.Status.Denied {
		return false, nil
	}
	return sar.Status.Allowed, nil
}

// Dirty and lazy hack to get the plural form
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

func (s *yesAccessReviewer) AccessAllowed(serviceAccount string, sourceNamespace string, object runtime.Object) (bool, error) {
	return true, nil
}
