// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package controller

import (
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/association"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/name"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	eslabel "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/rbac"
)

const (
	// EssAssociationLabelName marks resources created for an association originating from ElasticsearchStateless with the
	// ElasticsearchStateless name.
	EssAssociationLabelName = "essassociation.k8s.elastic.co/name"
	// EssAssociationLabelNamespace marks resources created for an association originating from ElasticsearchStateless with the
	// ElasticsearchStateless namespace.
	EssAssociationLabelNamespace = "essassociation.k8s.elastic.co/namespace"
	// EssAssociationLabelType marks resources created for an association originating from ElasticsearchStateless
	// with the target resource type (e.g. "elasticsearch").
	EssAssociationLabelType = "essassociation.k8s.elastic.co/type"
)

// AddEssMonitoring reconciles an association between an ElasticsearchStateless cluster and Elasticsearch clusters for Stack Monitoring.
// Beats are configured to collect monitoring metrics and logs data of the associated ElasticsearchStateless and send
// them to the Elasticsearch referenced in the association.
func AddEssMonitoring(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) error {
	return association.AddAssociationController(mgr, accessReviewer, params, essMonitoringAssociationInfo())
}

func essMonitoringAssociationInfo() association.AssociationInfo {
	return association.AssociationInfo{
		AssociatedObjTemplate: func() commonv1.Associated { return &essv1alpha1.ElasticsearchStateless{} },
		ReferencedObjTemplate: func(kind string) client.Object {
			return elasticsearchObjTemplate(kind)
		},
		ReferencedResourceVersion: referencedElasticsearchStatusVersion,
		ExternalServiceURL:        getElasticsearchExternalURL,
		AssociationType:           commonv1.EsMonitoringAssociationType,
		ReferencedResourceNamer: func(kind string) name.Namer {
			return elasticsearchNamer(kind)
		},
		ReferencedKinds: func() []string {
			return []string{commonv1.ElasticsearchKind, commonv1.ElasticsearchStatelessKind}
		},
		AssociationName:     "ess-monitoring",
		AssociatedShortName: "ess-mon",
		Labels: func(associated types.NamespacedName) map[string]string {
			return map[string]string{
				EssAssociationLabelName:      associated.Name,
				EssAssociationLabelNamespace: associated.Namespace,
				EssAssociationLabelType:      commonv1.EsMonitoringAssociationType,
			}
		},
		AssociationConfAnnotationNameBase:     commonv1.ElasticsearchConfigAnnotationNameBase,
		AssociationResourceNameLabelName:      eslabel.ClusterNameLabelNameForKind,
		AssociationResourceNamespaceLabelName: func(_ string) string { return eslabel.ClusterNamespaceLabelName },

		ElasticsearchUserCreation: &association.ElasticsearchUserCreation{
			ElasticsearchRef: func(c k8s.Client, association commonv1.Association) (bool, commonv1.ObjectSelector, string, error) {
				return true, association.AssociationRef(), association.AssociationRefKind(), nil
			},
			UserSecretSuffix: "beat-ess-mon-user",
			ESUserRole: func(associated commonv1.Associated) (string, error) {
				return user.StackMonitoringUserRole, nil
			},
		},
	}
}
