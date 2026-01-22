// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	kbv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/association"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/name"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/operator"
	ver "github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	eslabel "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/rbac"
)

const (
	// KibanaAssociationLabelName marks resources created for an association originating from Kibana with the
	// Kibana name.
	KibanaAssociationLabelName = "kibanaassociation.k8s.elastic.co/name"
	// KibanaAssociationLabelNamespace marks resources created for an association originating from Kibana with the
	// Kibana namespace.
	KibanaAssociationLabelNamespace = "kibanaassociation.k8s.elastic.co/namespace"
	// KibanaAssociationLabelType marks resources created for an association originating from Kibana
	// with the target resource type (e.g. "elasticsearch" or "ent).
	KibanaAssociationLabelType = "kibanaassociation.k8s.elastic.co/type"

	// KibanaSystemUserBuiltinRole is the name of the built-in role for the Kibana system user.
	KibanaSystemUserBuiltinRole = "kibana_system"

	// serverlessBuildFlavor is the string returned in the version.build_flavor field when running on serverless.
	serverlessBuildFlavor = "serverless"
)

func AddKibanaES(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) error {
	return association.AddAssociationController(mgr, accessReviewer, params, association.AssociationInfo{
		AssociatedObjTemplate: func() commonv1.Associated { return &kbv1.Kibana{} },
		ReferencedObjTemplate: func(kind string) client.Object {
			return elasticsearchObjTemplate(kind)
		},
		ReferencedResourceVersion: referencedElasticsearchStatusVersion,
		ExternalServiceURL:        getElasticsearchExternalURL,
		AssociationType:           commonv1.ElasticsearchAssociationType,
		ReferencedResourceNamer: func(kind string) name.Namer {
			return elasticsearchNamer(kind)
		},
		ReferencedKinds: func() []string {
			return []string{commonv1.ElasticsearchKind, commonv1.ElasticsearchStatelessKind}
		},
		AssociationName:     "kb-es",
		AssociatedShortName: "kb",
		Labels: func(associated types.NamespacedName) map[string]string {
			return map[string]string{
				KibanaAssociationLabelName:      associated.Name,
				KibanaAssociationLabelNamespace: associated.Namespace,
				KibanaAssociationLabelType:      commonv1.ElasticsearchAssociationType,
			}
		},
		AssociationConfAnnotationNameBase:     commonv1.ElasticsearchConfigAnnotationNameBase,
		AssociationResourceNameLabelName:      eslabel.ClusterNameLabelName,
		AssociationResourceNamespaceLabelName: eslabel.ClusterNamespaceLabelName,

		ElasticsearchUserCreation: &association.ElasticsearchUserCreation{
			ElasticsearchRef: func(c k8s.Client, association commonv1.Association) (bool, commonv1.ObjectSelector, error) {
				return true, association.AssociationRef(), nil
			},
			UserSecretSuffix: "kibana-user",
			ESUserRole: func(associated commonv1.Associated) (string, error) {
				return KibanaSystemUserBuiltinRole, nil
			},
		},
	})
}

// elasticsearchObjTemplate returns the appropriate Elasticsearch object template based on the kind.
func elasticsearchObjTemplate(kind string) client.Object {
	if kind == commonv1.ElasticsearchStatelessKind {
		return &essv1alpha1.ElasticsearchStateless{}
	}
	return &esv1.Elasticsearch{}
}

// elasticsearchNamer returns the appropriate namer based on the Elasticsearch kind.
func elasticsearchNamer(kind string) name.Namer {
	if kind == commonv1.ElasticsearchStatelessKind {
		return essv1alpha1.ESSNamer
	}
	return esv1.ESNamer
}

type elasticsearchVersionResponse struct {
	Version struct {
		Number      string `json:"number"`
		BuildFlavor string `json:"build_flavor"`
	} `json:"version"`
}

func (evr elasticsearchVersionResponse) IsServerless() bool {
	return evr.Version.BuildFlavor == serverlessBuildFlavor
}

func (evr elasticsearchVersionResponse) GetVersion() (string, error) {
	if _, err := ver.Parse(evr.Version.Number); err != nil {
		return "", err
	}
	return evr.Version.Number, nil
}

// referencedElasticsearchStatusVersion returns the currently running version of Elasticsearch
// reported in its status.
func referencedElasticsearchStatusVersion(c k8s.Client, esAssociation commonv1.Association) (string, bool, error) {
	esRef := esAssociation.AssociationRef()
	if esRef.IsExternal() {
		info, err := association.GetUnmanagedAssociationConnectionInfoFromSecret(c, esAssociation)
		if err != nil {
			return "", false, err
		}
		esVersionResponse := &elasticsearchVersionResponse{}
		ver, isServerless, err := info.Version("/", esVersionResponse)
		if err != nil {
			return "", false, err
		}
		return ver, isServerless, nil
	}

	// Handle both stateful and stateless Elasticsearch kinds
	if commonv1.AssociationRefIsStateless(esAssociation) {
		var ess essv1alpha1.ElasticsearchStateless
		err := c.Get(context.Background(), esRef.NamespacedName(), &ess)
		if err != nil {
			return "", false, err
		}
		return ess.Status.Version, false, nil
	}

	var es esv1.Elasticsearch
	err := c.Get(context.Background(), esRef.NamespacedName(), &es)
	if err != nil {
		return "", false, err
	}
	return es.Status.Version, false, nil
}
