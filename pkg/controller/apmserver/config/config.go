// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package config

import (
	"fmt"
	"path"
	"path/filepath"

	apmv1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates/http"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/settings"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

const (
	// DefaultHTTPPort is the (default) port used by ApmServer
	DefaultHTTPPort = 8200

	// Certificates
	EsCertificatesDir     = "config/elasticsearch-certs"
	KibanaCertificatesDir = "config/kibana-certs"

	APMServerHost        = "apm-server.host"
	APMServerSecretToken = "apm-server.secret_token"

	APMServerSSLEnabled     = "apm-server.ssl.enabled"
	APMServerSSLKey         = "apm-server.ssl.key"
	APMServerSSLCertificate = "apm-server.ssl.certificate"
)

func NewConfigFromSpec(c k8s.Client, as *apmv1.ApmServer) (*settings.CanonicalConfig, error) {
	specConfig := as.Spec.Config
	if specConfig == nil {
		specConfig = &commonv1.Config{}
	}

	userSettings, err := settings.NewCanonicalConfigFrom(specConfig.Data)
	if err != nil {
		return nil, err
	}

	outputCfg := settings.NewCanonicalConfig()
	for _, associationResolver := range as.AssociationResolvers() {
		associationConf := associationResolver.AssociationConf()
		if associationConf.IsConfigured() {
			tmpOutputCfg, err := conf(c, as, associationResolver)
			if err != nil {
				return nil, err
			}
			err = outputCfg.MergeWith(settings.MustCanonicalConfig(tmpOutputCfg))
			if err != nil {
				return nil, err
			}
		}
	}

	// Create a base configuration.
	cfg := settings.MustCanonicalConfig(map[string]interface{}{
		APMServerHost:        fmt.Sprintf(":%d", DefaultHTTPPort),
		APMServerSecretToken: "${SECRET_TOKEN}",
	})

	// Merge the configuration with userSettings last so they take precedence.
	err = cfg.MergeWith(
		outputCfg,
		settings.MustCanonicalConfig(tlsSettings(as)),
		userSettings,
	)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func conf(c k8s.Client, as *apmv1.ApmServer, resolver commonv1.AssociationResolver) (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	switch _ := resolver.(type) {
	case *apmv1.ApmEsAssociationResolver:
		username, password, err := association.ElasticsearchAuthSettings(c, as, resolver.AssociationConf())
		if err != nil {
			return cfg, err
		}
		cfg["output.elasticsearch.hosts"] = []string{resolver.AssociationConf().GetURL()}
		cfg["output.elasticsearch.username"] = username
		cfg["output.elasticsearch.password"] = password
		if resolver.AssociationConf().GetCACertProvided() {
			cfg["output.elasticsearch.ssl.certificate_authorities"] = []string{filepath.Join(EsCertificatesDir, certificates.CAFileName)}
		}
	case *apmv1.ApmKibanaAssociationResolver:

	}
	return cfg, fmt.Errorf("association %v is not supported", resolver)
}

func tlsSettings(as *apmv1.ApmServer) map[string]interface{} {
	if !as.Spec.HTTP.TLS.Enabled() {
		return nil
	}
	return map[string]interface{}{
		APMServerSSLEnabled:     true,
		APMServerSSLCertificate: path.Join(http.HTTPCertificatesSecretVolumeMountPath, certificates.CertFileName),
		APMServerSSLKey:         path.Join(http.HTTPCertificatesSecretVolumeMountPath, certificates.KeyFileName),
	}

}
