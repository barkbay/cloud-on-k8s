package config

import (
	"path"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"

	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"

	v1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/volume"
)

const (
	// Where Elasticsearch CA certificates are stored
	eSCertsVolumeMountPath = "/usr/share/kibana/config/elasticsearch-certs"
)

func ConfigurationHelper(client k8s.Client, kb *v1.Kibana) association.ConfigurationHelper {
	return &EsAssociationConfigurationHelper{
		Client:              client,
		KibanaEsAssociation: &v1.KibanaEsAssociation{Kibana: kb},
	}
}

type EsAssociationConfigurationHelper struct {
	k8s.Client
	*v1.KibanaEsAssociation
}

func (*EsAssociationConfigurationHelper) ConfigurationAnnotation() string {
	return annotation.ElasticsearchAssociationConf
}

func (*EsAssociationConfigurationHelper) AssociationTypeValue() string {
	return "" // not used for Kibana since there is only 1 association
}

func (e *EsAssociationConfigurationHelper) Configuration() (map[string]interface{}, error) {
	cfg := make(map[string]interface{})

	if e.AssociationConf().GetCACertProvided() {
		esCertsVolumeMountPath := e.SslVolume().VolumeMount().MountPath
		cfg[ElasticsearchSslCertificateAuthorities] = path.Join(esCertsVolumeMountPath, certificates.CAFileName)
		cfg[ElasticsearchSslVerificationMode] = "certificate"
	}

	if e.RequiresAssociation() {
		username, password, err := association.ElasticsearchAuthSettings(e.Client, e.AssociationConf(), e.Namespace)
		if err != nil {
			return cfg, err
		}
		cfg["elasticsearch.hosts"] = []string{e.AssociationConf().GetURL()}
		cfg[ElasticsearchUsername] = username
		cfg[ElasticsearchPassword] = password
	}

	return cfg, nil
}

func (e *EsAssociationConfigurationHelper) SslVolume() volume.SecretVolume {
	return volume.NewSecretVolumeWithMountPath(
		e.AssociationConf().GetCASecretName(),
		"elasticsearch-certs",
		eSCertsVolumeMountPath,
	)
}
