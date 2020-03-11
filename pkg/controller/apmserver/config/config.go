// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package config

import (
	"fmt"
	"path"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"

	apmv1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates/http"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/settings"
)

const (
	// DefaultHTTPPort is the (default) port used by ApmServer
	DefaultHTTPPort = 8200

	// ApmBaseDir is the base directory of the APM server
	ApmBaseDir = "/usr/share/apm-server"

	APMServerHost        = "apm-server.host"
	APMServerSecretToken = "apm-server.secret_token"

	APMServerSSLEnabled     = "apm-server.ssl.enabled"
	APMServerSSLKey         = "apm-server.ssl.key"
	APMServerSSLCertificate = "apm-server.ssl.certificate"
)

func NewConfigFromSpec(as *apmv1.ApmServer, configurationHelpers []association.ConfigurationHelper) (*settings.CanonicalConfig, error) {
	specConfig := as.Spec.Config
	if specConfig == nil {
		specConfig = &commonv1.Config{}
	}

	userSettings, err := settings.NewCanonicalConfigFrom(specConfig.Data)
	if err != nil {
		return nil, err
	}

	outputCfg := settings.NewCanonicalConfig()
	for _, configurationHelper := range configurationHelpers {
		tmpOutputCfg, err := configurationHelper.Configuration()
		if err != nil {
			return nil, err
		}
		err = outputCfg.MergeWith(settings.MustCanonicalConfig(tmpOutputCfg))
		if err != nil {
			return nil, err
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
