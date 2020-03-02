package config

import (
	apmv1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

func ConfigurationHelpers(client k8s.Client, as *apmv1.ApmServer) []association.ConfigurationHelper {
	return []association.ConfigurationHelper{
		&EsAssociationConfigurationHelper{
			Client:                   client,
			ApmEsAssociationResolver: &apmv1.ApmEsAssociationResolver{ApmServer: as},
		},
		&KibanaAssociationConfigurationHelper{
			Client:                       client,
			ApmKibanaAssociationResolver: &apmv1.ApmKibanaAssociationResolver{ApmServer: as},
		},
	}
}
