// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// AssociationStatus is the status of an association resource.
type AssociationStatus string

const (
	AssociationUnknown     AssociationStatus = ""
	AssociationPending     AssociationStatus = "Pending"
	AssociationEstablished AssociationStatus = "Established"
	AssociationFailed      AssociationStatus = "Failed"
)

// Associated interface represents a Elastic stack application that is associated with some other resources.
// An associated object needs some credentials to establish a connection to these resources and usually it
// offers a keystore which in ECK is represented with an underlying Secret.
// Kibana and the APM server are two examples of associated objects.
// +kubebuilder:object:generate=false
type Associated interface {
	metav1.Object
	runtime.Object
	AssociationRef(AssociationKind) (ObjectSelector, error)
	AssociationConf(AssociationKind) (*AssociationConf, error)
	ServiceAccountName() string
}

type AssociationKind string

const (
	ApmServerEs     AssociationKind = "apmserver-es"
	ApmServerKibana AssociationKind = "apmserver-kibana"
	KibanaEs        AssociationKind = "kibana-es"
)

// AssociationAnnotation returns the annotation used to store the config to access the remote resource.
func (a AssociationKind) ConfigurationAnnotation() (string, error) {
	switch a {
	case ApmServerEs, KibanaEs:
		return "association.k8s.elastic.co/es-conf", nil
	case ApmServerKibana:
		return "association.k8s.elastic.co/kibana-conf", nil
	}
	return "", fmt.Errorf("unknown association kind: %s", a)
}

func (a AssociationKind) UnmanagedError(object runtime.Object) error {
	return fmt.Errorf("%s does not manage an association kind %s", object.GetObjectKind().GroupVersionKind().Kind, a)
}

// Associator describes an object that allows its association to be set.
// +kubebuilder:object:generate=false
type Associator interface {
	metav1.Object
	runtime.Object
	SetAssociationConf(AssociationKind, *AssociationConf) error
}

// AssociationConf holds the association configuration of an Elasticsearch cluster.
type AssociationConf struct {
	AuthSecretName string `json:"authSecretName"`
	AuthSecretKey  string `json:"authSecretKey"`
	CACertProvided bool   `json:"caCertProvided"`
	CASecretName   string `json:"caSecretName"`
	URL            string `json:"url"`
}

// IsConfigured returns true if all the fields are set.
func (ac *AssociationConf) IsConfigured() bool {
	return ac.AuthIsConfigured() && ac.CAIsConfigured() && ac.URLIsConfigured()
}

// AuthIsConfigured returns true if all the auth fields are set.
func (ac *AssociationConf) AuthIsConfigured() bool {
	if ac == nil {
		return false
	}
	return ac.AuthSecretName != "" && ac.AuthSecretKey != ""
}

// CAIsConfigured returns true if the CA field is set.
func (ac *AssociationConf) CAIsConfigured() bool {
	if ac == nil {
		return false
	}
	return ac.CASecretName != ""
}

// URLIsConfigured returns true if the URL field is set.
func (ac *AssociationConf) URLIsConfigured() bool {
	if ac == nil {
		return false
	}
	return ac.URL != ""
}

func (ac *AssociationConf) GetAuthSecretName() string {
	if ac == nil {
		return ""
	}
	return ac.AuthSecretName
}

func (ac *AssociationConf) GetAuthSecretKey() string {
	if ac == nil {
		return ""
	}
	return ac.AuthSecretKey
}

func (ac *AssociationConf) GetCACertProvided() bool {
	if ac == nil {
		return false
	}
	return ac.CACertProvided
}

func (ac *AssociationConf) GetCASecretName() string {
	if ac == nil {
		return ""
	}
	return ac.CASecretName
}

func (ac *AssociationConf) GetURL() string {
	if ac == nil {
		return ""
	}
	return ac.URL
}
