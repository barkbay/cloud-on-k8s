// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1alpha1

import (
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LinkSpec defines the desired state of Link
type LinkSpec struct {
	// +optional
	DeploymentRefs []commonv1.ObjectSelector `json:"deploymentRefs"`

	// +optional
	DaemonSetRefs []commonv1.ObjectSelector `json:"daemonSetRefs"`

	// ElasticsearchName is the name of the Elasticsearch resource in this namespace for which the user data and certs should be copied
	// +optional
	ElasticsearchName string `json:"elasticsearchName,omitempty"`

	// KibanaName is the name of the Kibana resource in this namespace for which the user data and certs should be copied
	// +optional
	KibanaName string `json:"kibanaName,omitempty"`

	// KibanaName is the name of the Kibana resource in this namespace for which the user data and certs should be copied
	// +optional
	ApmServerName string `json:"apmServerName,omitempty"`

	// User to inject.
	// Optional, if not set the superuser "elastic" is used.
	// +optional
	User commonv1.SecretRef `json:"user,omitempty"`
}

type User struct {
	Role string `json:"role,omitempty"`
}

type Certificate struct {
	// If empty ElasticsearchRef.Namespace-ElasticsearchRef.Name-certs
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// Mount path for the volume that contains the Elasticsearch cert.
	// If empty /mnt/elastic/
	// +optional
	MountPath string `json:"mountPath,omitempty"`

	// If empty es-certs is used
	// +optional
	VolumeName string `json:"volumeName,omitempty"`
}

// LinkStatus defines the observed state of Link
type LinkStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Link is the Schema for the links API
// +k8s:openapi-gen=true
type Link struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LinkSpec   `json:"spec,omitempty"`
	Status LinkStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LinkList contains a list of Link
type LinkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Link `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Link{}, &LinkList{})
}
