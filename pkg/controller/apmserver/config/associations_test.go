package config

import (
	"context"
	"testing"

	apmv1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"
	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestFetchAPMServer(t *testing.T) {
	testCases := []struct {
		name                string
		apmServer           *apmv1.ApmServer
		request             reconcile.Request
		wantErr             bool
		wantEsAssocConf     *commonv1.AssociationConf
		wantKibanaAssocConf *commonv1.AssociationConf
	}{
		{
			name:      "with Elasticsearch association annotation",
			apmServer: mkAPMServerWithAnnotations(true, false),
			request:   reconcile.Request{NamespacedName: types.NamespacedName{Name: "apm-server-test", Namespace: "apm-ns"}},
			wantEsAssocConf: &commonv1.AssociationConf{
				AuthSecretName: "auth-secret",
				AuthSecretKey:  "apm-user",
				CASecretName:   "ca-secret",
				URL:            "https://es.svc:9300",
			},
		},
		{
			name:      "without any association annotation",
			apmServer: mkAPMServerWithAnnotations(false, false),
			request:   reconcile.Request{NamespacedName: types.NamespacedName{Name: "apm-server-test", Namespace: "apm-ns"}},
		},
		{
			name:      "non existent",
			apmServer: mkAPMServerWithAnnotations(true, true),
			request:   reconcile.Request{NamespacedName: types.NamespacedName{Name: "some-other-apm", Namespace: "apm-ns"}},
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := k8s.WrappedFakeClient(tc.apmServer)

			var got apmv1.ApmServer
			err := association.FetchWithAssociations(context.Background(), client, tc.request, &got, ConfigurationHelpers(client, &got)...)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.Equal(t, "apm-server-test", got.Name)
			require.Equal(t, "apm-ns", got.Namespace)
			require.Equal(t, "test-image", got.Spec.Image)
			require.EqualValues(t, 1, got.Spec.Count)
			apmEsAssociationManager := apmv1.ApmEsAssociationResolver{ApmServer: &got}
			require.Equal(t, tc.wantEsAssocConf, apmEsAssociationManager.AssociationConf())
			apmKibanaAssociationManager := apmv1.ApmKibanaAssociationResolver{ApmServer: &got}
			require.Equal(t, tc.wantKibanaAssocConf, apmKibanaAssociationManager.AssociationConf())
		})
	}
}

func mkAPMServerWithAnnotations(withEsAnnotations, withKbAnnotations bool) *apmv1.ApmServer {
	apmServer := &apmv1.ApmServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "apm-server-test",
			Namespace: "apm-ns",
		},
		Spec: apmv1.ApmServerSpec{
			Image: "test-image",
			Count: 1,
		},
	}

	if withEsAnnotations {
		apmServer.ObjectMeta.Annotations = map[string]string{
			annotation.ElasticsearchAssociationConf: `{"authSecretName":"auth-secret", "authSecretKey":"apm-user", "caSecretName": "ca-secret", "url":"https://es.svc:9300"}`,
		}
	}

	if withKbAnnotations {
		apmServer.ObjectMeta.Annotations = map[string]string{
			annotation.KibanaAssociationConf: `{"authSecretName":"auth-secret", "authSecretKey":"apm-user", "caSecretName": "kb-ca-secret", "url":"https://kb.svc:9300"}`,
		}
	}

	return apmServer
}
