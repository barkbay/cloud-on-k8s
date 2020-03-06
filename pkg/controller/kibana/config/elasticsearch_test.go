package config

import (
	"context"
	"testing"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/association"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	kbv1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/annotation"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestFetchKibana(t *testing.T) {
	testCases := []struct {
		name          string
		kibana        *kbv1.Kibana
		request       reconcile.Request
		wantErr       bool
		wantAssocConf *commonv1.AssociationConf
	}{
		{
			name:    "with association annotation",
			kibana:  mkKibanaWithAnnotations(true),
			request: reconcile.Request{NamespacedName: types.NamespacedName{Name: "kb-test", Namespace: "kb-ns"}},
			wantAssocConf: &commonv1.AssociationConf{
				AuthSecretName: "auth-secret",
				AuthSecretKey:  "kb-user",
				CASecretName:   "ca-secret",
				URL:            "https://es.svc:9300",
			},
		},
		{
			name:    "without association annotation",
			kibana:  mkKibanaWithAnnotations(false),
			request: reconcile.Request{NamespacedName: types.NamespacedName{Name: "kb-test", Namespace: "kb-ns"}},
		},
		{
			name:    "non existent",
			kibana:  mkKibanaWithAnnotations(true),
			request: reconcile.Request{NamespacedName: types.NamespacedName{Name: "some-other-kb", Namespace: "kb-ns"}},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := k8s.WrappedFakeClient(tc.kibana)

			var got kbv1.Kibana
			err := association.FetchWithAssociations(context.Background(), client, tc.request, &got, ConfigurationHelper(client, &got))

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.Equal(t, "kb-test", got.Name)
			require.Equal(t, "kb-ns", got.Namespace)
			require.Equal(t, "test-image", got.Spec.Image)
			require.EqualValues(t, 1, got.Spec.Count)
			kibanaEsAssociationManager := kbv1.KibanaEsAssociationResolver{Kibana: &got}
			require.Equal(t, tc.wantAssocConf, kibanaEsAssociationManager.AssociationConf())
		})
	}
}

func mkKibanaWithAnnotations(withAnnotations bool) *kbv1.Kibana {
	kb := &kbv1.Kibana{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kb-test",
			Namespace: "kb-ns",
		},
		Spec: kbv1.KibanaSpec{
			Image: "test-image",
			Count: 1,
		},
	}

	if withAnnotations {
		kb.ObjectMeta.Annotations = map[string]string{
			annotation.ElasticsearchAssociationConf: `{"authSecretName":"auth-secret", "authSecretKey":"kb-user", "caSecretName": "ca-secret", "url":"https://es.svc:9300"}`,
		}
	}

	return kb
}

func TestKibanaUpdateAssociationConf(t *testing.T) {
	kb := mkKibanaWithAnnotations(true)
	request := reconcile.Request{NamespacedName: types.NamespacedName{Name: "kb-test", Namespace: "kb-ns"}}
	client := k8s.WrappedFakeClient(kb)

	assocConf := &commonv1.AssociationConf{
		AuthSecretName: "auth-secret",
		AuthSecretKey:  "kb-user",
		CASecretName:   "ca-secret",
		URL:            "https://es.svc:9300",
	}

	// check the existing values
	var got kbv1.Kibana
	cfgHelper := ConfigurationHelper(client, &got)
	err := association.FetchWithAssociations(context.Background(), client, request, &got, cfgHelper)
	require.NoError(t, err)
	require.Equal(t, "kb-test", got.Name)
	require.Equal(t, "kb-ns", got.Namespace)
	require.Equal(t, "test-image", got.Spec.Image)
	require.EqualValues(t, 1, got.Spec.Count)
	require.Equal(t, assocConf, cfgHelper.AssociationConf())

	// update and check the new values
	newAssocConf := &commonv1.AssociationConf{
		AuthSecretName: "new-auth-secret",
		AuthSecretKey:  "new-kb-user",
		CASecretName:   "new-ca-secret",
		URL:            "https://new-es.svc:9300",
	}

	err = association.UpdateAssociationConf(client, cfgHelper.ConfigurationAnnotation(), &got, newAssocConf)
	require.NoError(t, err)

	err = association.FetchWithAssociations(context.Background(), client, request, &got)
	require.NoError(t, err)
	require.Equal(t, "kb-test", got.Name)
	require.Equal(t, "kb-ns", got.Namespace)
	require.Equal(t, "test-image", got.Spec.Image)
	require.EqualValues(t, 1, got.Spec.Count)
	require.Equal(t, newAssocConf, cfgHelper.AssociationConf())
}

func TestRemoveKibanaAssociationConf(t *testing.T) {
	kb := mkKibanaWithAnnotations(true)
	request := reconcile.Request{NamespacedName: types.NamespacedName{Name: "kb-test", Namespace: "kb-ns"}}
	client := k8s.WrappedFakeClient(kb)

	assocConf := &commonv1.AssociationConf{
		AuthSecretName: "auth-secret",
		AuthSecretKey:  "kb-user",
		CASecretName:   "ca-secret",
		URL:            "https://es.svc:9300",
	}

	// check the existing values
	var got kbv1.Kibana
	cfgHelper := ConfigurationHelper(client, &got)
	err := association.FetchWithAssociations(context.Background(), client, request, &got, cfgHelper)
	require.NoError(t, err)
	require.Equal(t, "kb-test", got.Name)
	require.Equal(t, "kb-ns", got.Namespace)
	require.Equal(t, "test-image", got.Spec.Image)
	require.EqualValues(t, 1, got.Spec.Count)
	require.Equal(t, assocConf, cfgHelper.AssociationConf())

	// remove and check the new values
	err = association.RemoveAssociationConf(client, cfgHelper.ConfigurationAnnotation(), &got)
	require.NoError(t, err)

	err = association.FetchWithAssociations(context.Background(), client, request, &got)
	require.NoError(t, err)
	require.Equal(t, "kb-test", got.Name)
	require.Equal(t, "kb-ns", got.Namespace)
	require.Equal(t, "test-image", got.Spec.Image)
	require.EqualValues(t, 1, got.Spec.Count)
	require.Nil(t, cfgHelper.AssociationConf())
}
