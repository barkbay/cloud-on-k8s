package apmserverelasticsearchassociation

import (
	"context"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/labels"
	apmlabels "github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/labels"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates/http"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"go.elastic.co/apm"
	"k8s.io/apimachinery/pkg/types"
)

const (
	apmUserSuffix = "apm-user"
)

func (r *ReconcileApmServerElasticsearchAssociation) reconcileEsAssociation(
	ctx context.Context,
	apmServer commonv1.Associated,
	cfgHelper association.ConfigurationHelper,
) (commonv1.AssociationStatus, error) {
	// no auto-association nothing to do
	elasticsearchRef := cfgHelper.AssociationRef()
	if !elasticsearchRef.IsDefined() {
		return commonv1.AssociationUnknown, nil
	}
	if elasticsearchRef.Namespace == "" {
		// no namespace provided: default to the APM server namespace
		elasticsearchRef.Namespace = apmServer.GetNamespace()
	}
	assocKey := k8s.ExtractNamespacedName(apmServer)
	// Make sure we see events from Elasticsearch using a dynamic watch
	// will become more relevant once we refactor user handling to CRDs and implement
	// syncing of user credentials across namespaces
	err := r.watches.ElasticsearchClusters.AddHandler(watches.NamedWatch{
		Name:    elasticsearchWatchName(assocKey),
		Watched: []types.NamespacedName{elasticsearchRef.NamespacedName()},
		Watcher: assocKey,
	})
	if err != nil {
		return commonv1.AssociationFailed, err
	}

	var es esv1.Elasticsearch
	associationStatus, err := r.getElasticsearch(ctx, apmServer, elasticsearchRef, &es, cfgHelper.ConfigurationAnnotation())
	if associationStatus != "" || err != nil {
		return associationStatus, err
	}

	// Check if reference to Elasticsearch is allowed to be established
	if allowed, err := association.CheckAndUnbind(
		r.accessReviewer,
		apmServer,
		&es,
		cfgHelper.ConfigurationAnnotation(),
		r,
		r.recorder,
	); err != nil || !allowed {
		return commonv1.AssociationPending, err
	}

	if err := association.ReconcileEsUser(
		ctx,
		r.Client,
		r.scheme,
		apmServer,
		cfgHelper.AssociationRef().Namespace,
		map[string]string{
			AssociationLabelName:      apmServer.GetName(),
			AssociationLabelNamespace: apmServer.GetNamespace(),
			AssociationLabelType:      apmlabels.ElasticsearchAssociationLabelValue,
		},
		"superuser",
		apmUserSuffix,
		es,
	); err != nil { // TODO distinguish conflicts and non-recoverable errors here
		return commonv1.AssociationPending, err
	}

	caSecret, err := r.reconcileElasticsearchCA(ctx, apmServer, elasticsearchRef.NamespacedName())
	if err != nil {
		return commonv1.AssociationPending, err // maybe not created yet
	}

	// construct the expected ES output configuration
	authSecretRef := association.ClearTextSecretKeySelector(apmServer, apmUserSuffix)
	expectedAssocConf := &commonv1.AssociationConf{
		AuthSecretName: authSecretRef.Name,
		AuthSecretKey:  authSecretRef.Key,
		CACertProvided: caSecret.CACertProvided,
		CASecretName:   caSecret.Name,
		URL:            services.ExternalServiceURL(es),
	}

	var status commonv1.AssociationStatus
	status, err = r.updateEsAssocConf(ctx, expectedAssocConf, apmServer, cfgHelper)
	if err != nil || status != "" {
		return status, err
	}

	return commonv1.AssociationEstablished, nil
}

func elasticsearchWatchName(assocKey types.NamespacedName) string {
	return assocKey.Namespace + "-" + assocKey.Name + "-es-watch"
}

func (r *ReconcileApmServerElasticsearchAssociation) reconcileElasticsearchCA(ctx context.Context, as commonv1.Associated, es types.NamespacedName) (association.CASecret, error) {
	span, _ := apm.StartSpan(ctx, "reconcile_es_ca", tracing.SpanTypeApp)
	defer span.End()

	apmKey := k8s.ExtractNamespacedName(as)
	// watch ES CA secret to reconcile on any change
	if err := r.watches.Secrets.AddHandler(watches.NamedWatch{
		Name:    esCAWatchName(apmKey),
		Watched: []types.NamespacedName{http.PublicCertsSecretRef(esv1.ESNamer, es)},
		Watcher: apmKey,
	}); err != nil {
		return association.CASecret{}, err
	}
	// Build the labels applied on the secret
	labels := labels.NewLabels(as.GetName())
	labels[AssociationLabelName] = as.GetName()
	labels[AssociationLabelType] = apmlabels.ElasticsearchAssociationLabelValue
	return association.ReconcileCASecret(
		r.Client,
		r.scheme,
		as,
		es,
		esv1.ESNamer,
		labels,
		elasticsearchCASecretSuffix,
	)
}

// esCAWatchName returns the name of the watch setup on the secret that
// contains the HTTP certificate chain of Elasticsearch.
func esCAWatchName(apm types.NamespacedName) string {
	return apm.Namespace + "-" + apm.Name + "-ca-watch"
}
