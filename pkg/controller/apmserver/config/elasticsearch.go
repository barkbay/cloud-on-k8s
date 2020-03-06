package config

import (
	"path/filepath"

	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/labels"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
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
	return annotation.ElasticsearchAssociationConf
}

func (*EsAssociationConfigurationHelper) AssociationTypeValue() string {
	return labels.ElasticsearchAssociationLabelValue
}

func (e *EsAssociationConfigurationHelper) Configuration() (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	if !e.AssociationConf().IsConfigured() {
		return cfg, nil
	}
	username, password, err := association.ElasticsearchAuthSettings(e.Client, e.AssociationConf(), e.Namespace)
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
