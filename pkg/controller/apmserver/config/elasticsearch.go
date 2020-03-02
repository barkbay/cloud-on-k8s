package config

import (
	"path/filepath"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"

	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/volume"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

const (
	// Where Elasticsearch CA certificates are stored
	EsCertificatesDir = "config/elasticsearch-certs"
)

type EsAssociationConfigurationHelper struct {
	k8s.Client
	*v1.ApmEsAssociationResolver
}

func (*EsAssociationConfigurationHelper) ConfigurationAnnotation() string {
	return "association.k8s.elastic.co/es-conf"
}

func (e *EsAssociationConfigurationHelper) Configuration() (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	if !e.AssociationConf().IsConfigured() {
		return cfg, nil
	}
	username, password, err := association.ElasticsearchAuthSettings(e)
	if err != nil {
		return cfg, err
	}
	cfg["output.elasticsearch.hosts"] = []string{e.AssociationConf().GetURL()}
	cfg["output.elasticsearch.username"] = username
	cfg["output.elasticsearch.password"] = password
	if e.AssociationConf().GetCACertProvided() {
		cfg["output.elasticsearch.ssl.certificate_authorities"] = []string{filepath.Join(EsCertificatesDir, certificates.CAFileName)}
	}
	return cfg, nil
}

func (e *EsAssociationConfigurationHelper) SslVolume() volume.SecretVolume {
	return volume.NewSecretVolumeWithMountPath(
		e.AssociationConf().GetCASecretName(),
		"elasticsearch-certs",
		filepath.Join(ApmBaseDir, EsCertificatesDir),
	)
}
