package apmserverelasticsearchassociation

import (
	"context"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	kbv1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/labels"
	apmlabels "github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/labels"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates/http"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/controller/kibana"
	kbconfig "github.com/elastic/cloud-on-k8s/pkg/controller/kibana/config"
	kbname "github.com/elastic/cloud-on-k8s/pkg/controller/kibana/name"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"go.elastic.co/apm"
	"k8s.io/apimachinery/pkg/types"
)

const (
	apmKbUserSuffix = "apm-kb-user"
)

func (r *ReconcileApmServerElasticsearchAssociation) reconcileKibanaAssociation(
	ctx context.Context,
	apmServer commonv1.Associated,
	kbCfgHelper association.ConfigurationHelper,
) (commonv1.AssociationStatus, error) {
	kibanaRef := kbCfgHelper.AssociationRef()
	if !kibanaRef.IsDefined() {
		// no auto-association nothing to do
		return commonv1.AssociationUnknown, nil
	}
	if kibanaRef.Namespace == "" {
		// no namespace provided: default to the APM server namespace
		kibanaRef.Namespace = apmServer.GetNamespace()
	}
	assocKey := k8s.ExtractNamespacedName(apmServer)
	err := r.watches.Kibanas.AddHandler(watches.NamedWatch{
		Name:    kibanaWatchName(assocKey),
		Watched: []types.NamespacedName{kibanaRef.NamespacedName()},
		Watcher: assocKey,
	})
	if err != nil {
		return commonv1.AssociationFailed, err
	}

	var kb kbv1.Kibana
	associationStatus, err := r.getElasticsearch(ctx, apmServer, kibanaRef, &kb, kbCfgHelper.ConfigurationAnnotation())
	if associationStatus != "" || err != nil {
		return associationStatus, err
	}

	// Check if reference to Elasticsearch is allowed to be established
	if allowed, err := association.CheckAndUnbind(
		r.accessReviewer,
		apmServer,
		&kb,
		kbCfgHelper.ConfigurationAnnotation(),
		r,
		r.recorder,
	); err != nil || !allowed {
		return commonv1.AssociationPending, err
	}

	// We can only create a user if Kibana is connected to an Elasticsearch cluster managed by the operator
	kbEsCfgHelper := kbconfig.ConfigurationHelper(r.Client, &kb)
	kbEsAssociation := kbEsCfgHelper.AssociationRef()
	if kbEsAssociation.IsDefined() {
		var es esv1.Elasticsearch
		associationStatus, err := r.getElasticsearch(ctx, apmServer, kbEsCfgHelper.AssociationRef().WithDefaultNamespace(apmServer.GetNamespace()), &es, kbCfgHelper.ConfigurationAnnotation())
		if associationStatus != "" || err != nil {
			return associationStatus, err
		}

		if err := association.ReconcileEsUser(
			ctx,
			r.Client,
			r.scheme,
			apmServer,
			kbEsCfgHelper.AssociationRef().Namespace,
			map[string]string{
				AssociationLabelName:      apmServer.GetName(),
				AssociationLabelType:      apmlabels.KibanaAssociationLabelValue,
				AssociationLabelNamespace: apmServer.GetNamespace(),
			},
			"superuser",
			apmKbUserSuffix,
			es,
		); err != nil { // TODO distinguish conflicts and non-recoverable errors here
			return commonv1.AssociationPending, err
		}
	}

	caSecret, err := r.reconcileKibanaCA(ctx, apmServer, kibanaRef.NamespacedName())
	if err != nil {
		return commonv1.AssociationPending, err // maybe not created yet
	}

	// construct the expected ES output configuration
	authSecretRef := association.ClearTextSecretKeySelector(apmServer, apmKbUserSuffix)
	expectedAssocConf := &commonv1.AssociationConf{
		AuthSecretName: authSecretRef.Name,
		AuthSecretKey:  authSecretRef.Key,
		CACertProvided: caSecret.CACertProvided,
		CASecretName:   caSecret.Name,
		URL:            kibana.ExternalServiceURL(kb),
	}

	var status commonv1.AssociationStatus
	status, err = r.updateEsAssocConf(ctx, expectedAssocConf, apmServer, kbCfgHelper)
	if err != nil || status != "" {
		return status, err
	}

	return commonv1.AssociationEstablished, nil
}

func kibanaWatchName(assocKey types.NamespacedName) string {
	return assocKey.Namespace + "-" + assocKey.Name + "-kb-watch"
}

func (r *ReconcileApmServerElasticsearchAssociation) reconcileKibanaCA(ctx context.Context, as commonv1.Associated, kb types.NamespacedName) (association.CASecret, error) {
	span, _ := apm.StartSpan(ctx, "reconcile_kibana_ca", tracing.SpanTypeApp)
	defer span.End()

	apmKey := k8s.ExtractNamespacedName(as)
	// watch ES CA secret to reconcile on any change
	if err := r.watches.Secrets.AddHandler(watches.NamedWatch{
		Name:    kbCAWatchName(apmKey),
		Watched: []types.NamespacedName{http.PublicCertsSecretRef(kbname.KBNamer, kb)},
		Watcher: apmKey,
	}); err != nil {
		return association.CASecret{}, err
	}
	// Build the labels applied on the secret
	labels := labels.NewLabels(as.GetName())
	labels[AssociationLabelName] = as.GetName()
	labels[AssociationLabelType] = apmlabels.KibanaAssociationLabelValue
	return association.ReconcileCASecret(
		r.Client,
		r.scheme,
		as,
		kb,
		kbname.KBNamer,
		labels,
		kibanaCASecretSuffix,
	)
}

func kbCAWatchName(apm types.NamespacedName) string {
	return apm.Namespace + "-" + apm.Name + "-kb-ca-watch"
}
