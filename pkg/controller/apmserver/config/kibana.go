package config

import (
	"path/filepath"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"

	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"

	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/volume"
)

const (
	// Where Kibana CA certificates are stored
	KibanaCertificatesDir = "config/kibana-certs"
)

type KibanaAssociationConfigurationHelper struct {
	k8s.Client
	*v1.ApmKibanaAssociationResolver
}

func (*KibanaAssociationConfigurationHelper) ConfigurationAnnotation() string {
	return "association.k8s.elastic.co/kibana-conf"
}

func (k *KibanaAssociationConfigurationHelper) Configuration() (map[string]interface{}, error) {
	cfg := make(map[string]interface{})
	if !k.AssociationConf().IsConfigured() {
		return cfg, nil
	}
	username, password, err := association.ElasticsearchAuthSettings(k)
	if err != nil {
		return cfg, err
	}
	cfg["apm-server.kibana.enabled"] = true
	cfg["apm-server.kibana.host"] = k.AssociationConf().GetURL()
	cfg["apm-server.kibana.username"] = username
	cfg["apm-server.kibana.password"] = password
	if k.AssociationConf().GetCACertProvided() {
		cfg["apm-server.kibana.ssl.certificate_authorities"] = []string{filepath.Join(KibanaCertificatesDir, certificates.CAFileName)}
	}
	return cfg, nil
}

func (k *KibanaAssociationConfigurationHelper) SslVolume() volume.SecretVolume {
	return volume.NewSecretVolumeWithMountPath(
		k.AssociationConf().GetCASecretName(),
		"kibana-certs",
		filepath.Join(ApmBaseDir, KibanaCertificatesDir),
	)
}
