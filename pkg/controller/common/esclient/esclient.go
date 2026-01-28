// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package esclient

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	esclient "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/v3/pkg/dev"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/net"
)

type Provider func(ctx context.Context, c k8s.Client, dialer net.Dialer, es escommon.ElasticsearchCluster) (esclient.Client, error)

func NewClient(
	ctx context.Context,
	c k8s.Client,
	dialer net.Dialer,
	es escommon.ElasticsearchCluster,
) (esclient.Client, error) {
	defer tracing.Span(&ctx)()
	v, err := version.Parse(es.GetVersion())
	if err != nil {
		return nil, err
	}
	// Get user Secret
	var controllerUserSecret corev1.Secret
	key := types.NamespacedName{
		Namespace: es.GetNamespace(),
		Name:      escommon.InternalUsersSecret(es),
	}
	if err := c.Get(ctx, key, &controllerUserSecret); err != nil {
		return nil, err
	}
	password, ok := controllerUserSecret.Data[user.ControllerUserName]
	if !ok {
		return nil, fmt.Errorf("controller user %s not found in Secret %s/%s", user.ControllerUserName, key.Namespace, key.Name)
	}

	// Get public certs - use the appropriate namer based on cluster type
	namer := escommon.StatefulNamer
	if es.IsStateless() {
		namer = escommon.StatelessNamer
	}
	var caSecret corev1.Secret
	key = types.NamespacedName{
		Namespace: es.GetNamespace(),
		Name:      certificates.PublicCertsSecretName(namer, es.GetName()),
	}
	if err := c.Get(ctx, key, &caSecret); err != nil {
		return nil, err
	}
	trustedCerts, ok := caSecret.Data[certificates.CertFileName]
	if !ok {
		return nil, fmt.Errorf("%s not found in Secret %s/%s", certificates.CertFileName, key.Namespace, key.Name)
	}
	caCerts, err := certificates.ParsePEMCerts(trustedCerts)
	if err != nil {
		return nil, err
	}

	return esclient.NewElasticsearchClient(
		dialer,
		k8s.ExtractNamespacedName(es),
		newURLProvider(es, c),
		esclient.BasicAuth{
			Name:     user.ControllerUserName,
			Password: string(password),
		},
		v,
		caCerts,
		esclient.Timeout(ctx, es),
		dev.Enabled,
	), nil
}

// newURLProvider creates a URLProvider for the given Elasticsearch cluster.
func newURLProvider(es escommon.ElasticsearchCluster, c k8s.Client) esclient.URLProvider {
	// For stateful clusters, use the existing URL provider
	if !es.IsStateless() {
		return services.NewElasticsearchURLProvider(es, c)
	}
	// For stateless clusters, return a service-based URL provider
	return &simpleURLProvider{
		url: services.InternalServiceURL(es),
	}
}

// simpleURLProvider is a basic URL provider that returns a fixed URL.
type simpleURLProvider struct {
	url string
}

func (s *simpleURLProvider) URL() (string, error) {
	return s.url, nil
}

func (s *simpleURLProvider) HasEndpoints() bool {
	return true
}

func (s *simpleURLProvider) Equals(other esclient.URLProvider) bool {
	otherSimple, ok := other.(*simpleURLProvider)
	if !ok {
		return false
	}
	return s.url == otherSimple.url
}
