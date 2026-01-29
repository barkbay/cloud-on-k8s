// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package association

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	kbv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/password/fixtures"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	esuser "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

const (
	userName                  = "default-kibana-foo-kibana-user"
	userSecretName            = "kibana-foo-kibana-user"
	associationLabelName      = "association.k8s.elastic.co/name"
	associationLabelNamespace = "association.k8s.elastic.co/namespace"
)

func Test_reconcileEsUser(t *testing.T) {
	esFixture := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "es-foo",
			Namespace: "default",
			UID:       "f8d564d9-885e-11e9-896d-08002703f062",
		},
	}

	var kibanaFixtureUID types.UID = "82257b19-8862-11e9-896d-08002703f062"

	kibanaFixtureObjectMeta := metav1.ObjectMeta{
		Name:      "kibana-foo",
		Namespace: "default",
		UID:       kibanaFixtureUID,
	}

	kibanaFixture := kbv1.Kibana{
		ObjectMeta: kibanaFixtureObjectMeta,
		Spec: kbv1.KibanaSpec{
			ElasticsearchRef: commonv1.ElasticsearchRef{
				ObjectSelector: commonv1.ObjectSelector{
					Name:      esFixture.Name,
					Namespace: esFixture.Namespace,
				},
			},
		},
	}

	type args struct {
		initialObjects []client.Object
		kibana         kbv1.Kibana
		es             esv1.Elasticsearch
	}
	tests := []struct {
		name          string
		args          args
		wantErr       bool
		postCondition func(client k8s.Client)
	}{
		{
			name: "Reconcile updates existing labels",
			args: args{
				initialObjects: []client.Object{&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      userName,
						Namespace: "default",
						Labels: map[string]string{
							associationLabelName: kibanaFixture.Name,
						},
					},
				}},
				kibana: kibanaFixture,
				es:     esFixture,
			},
			wantErr: false,
			postCondition: func(c k8s.Client) {
				var esUser corev1.Secret
				assert.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: userName, Namespace: "default"}, &esUser))
				expectedLabels := map[string]string{
					associationLabelName:       kibanaFixture.Name,
					commonv1.TypeLabelName:     esuser.AssociatedUserType,
					label.ClusterNameLabelName: "es-foo",
				}
				for k, v := range expectedLabels {
					assert.Equal(t, v, esUser.Labels[k])
				}
			},
		},
		{
			name: "Happy path: should create two secrets",
			args: args{
				initialObjects: nil,
				kibana:         kibanaFixture,
				es:             esFixture,
			},
			postCondition: func(c k8s.Client) {
				userKey := types.NamespacedName{
					Name:      userName,
					Namespace: "default",
				}
				assert.NoError(t, c.Get(context.Background(), userKey, &corev1.Secret{}))
				secretKey := types.NamespacedName{
					Name:      userSecretName,
					Namespace: "default",
				}
				assert.NoError(t, c.Get(context.Background(), secretKey, &corev1.Secret{}))
			},
			wantErr: false,
		},
		{
			name: "Existing secret but different namespace: create new",
			args: args{
				initialObjects: []client.Object{&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      userSecretName,
						Namespace: "other",
					},
				}},
				kibana: kibanaFixture,
				es:     esFixture,
			},
			wantErr: false,
			postCondition: func(c k8s.Client) {
				list := corev1.SecretList{}
				assert.NoError(t, c.List(context.Background(), &list))
				assert.Equal(t, 3, len(list.Items))
				s := GetSecret(list, types.NamespacedName{Namespace: "other", Name: userSecretName})
				assert.NotNil(t, s)
				s = GetSecret(list, types.NamespacedName{Namespace: esFixture.Namespace, Name: userSecretName})
				assert.NotNil(t, s)
				password, passwordIsSet := s.Data[userName]
				assert.True(t, passwordIsSet)
				assert.NotEmpty(t, password)
				s = GetSecret(list, types.NamespacedName{Namespace: esFixture.Namespace, Name: userName}) // secret on the ES side
				ChecksUser(t, s, userName, []string{"kibana_system"})
			},
		},
		{
			name: "Reconcile updates existing resources",
			args: args{
				initialObjects: []client.Object{&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      userSecretName,
						Namespace: "default",
					},
				}},
				kibana: kibanaFixture,
				es:     esFixture,
			},
			wantErr: false,
			postCondition: func(c k8s.Client) {
				var s corev1.Secret
				assert.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: userSecretName, Namespace: "default"}, &s))
				password, ok := s.Data[userName]
				assert.True(t, ok)
				assert.NotEmpty(t, password)
			},
		},
		{
			name: "Reconcile avoids unnecessary updates",
			args: args{
				initialObjects: []client.Object{
					&corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "default",
							Name:      userSecretName,
							Labels: map[string]string{
								associationLabelName:      kibanaFixture.Name,
								associationLabelNamespace: kibanaFixture.Namespace,
							},
						},
						Data: map[string][]byte{
							userName: []byte("my-secret-pw"),
						},
					},
					&corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      userName,
							Namespace: "default",
							Labels: map[string]string{
								associationLabelName:       kibanaFixture.Name,
								associationLabelNamespace:  kibanaFixture.Namespace,
								commonv1.TypeLabelName:     esuser.AssociatedUserType,
								label.ClusterNameLabelName: esFixture.Name,
							},
						},
						Data: map[string][]byte{
							esuser.UserNameField:     []byte(userName),
							esuser.PasswordHashField: []byte("$2a$10$mE3yo/AkZgR4eVW9kbA1TeIQ40Jv6WaWU494rx4C6EhLvuY0BSg4e"),
							esuser.UserRolesField:    []byte("kibana_system"),
						},
					},
				},
				kibana: kibanaFixture,
				es:     esFixture,
			},
			wantErr: false,
			postCondition: func(c k8s.Client) {
				var userSecret corev1.Secret
				assert.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: userName, Namespace: "default"}, &userSecret))
				require.Equal(t, "$2a$10$mE3yo/AkZgR4eVW9kbA1TeIQ40Jv6WaWU494rx4C6EhLvuY0BSg4e", string(userSecret.Data[esuser.PasswordHashField]))
			},
		},
		{
			name: "Reconcile is namespace aware",
			args: args{
				kibana: kbv1.Kibana{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "kibana-foo",
						Namespace: "ns-2",
					},
					Spec: kbv1.KibanaSpec{
						ElasticsearchRef: commonv1.ElasticsearchRef{
							ObjectSelector: commonv1.ObjectSelector{
								Name:      esFixture.Name,
								Namespace: esFixture.Namespace,
							},
						},
					},
				},
				es: esFixture,
			},
			wantErr: false,
			postCondition: func(c k8s.Client) {
				// user should be in ES namespace
				assert.NoError(t, c.Get(context.Background(), types.NamespacedName{
					Namespace: "default",
					// name should include kibana namespace
					Name: "ns-2-kibana-foo-kibana-user",
				}, &corev1.Secret{}))
				// secret should be in Kibana namespace
				assert.NoError(t, c.Get(context.Background(), types.NamespacedName{
					Namespace: "ns-2",
					Name:      userSecretName,
				}, &corev1.Secret{}))
			},
		},
	}
	for _, tt := range tests {
		c := k8s.NewFakeClient(tt.args.initialObjects...)
		t.Run(tt.name, func(t *testing.T) {
			if err := reconcileEsUserSecret(
				context.Background(),
				c,
				tt.args.kibana.EsAssociation(),
				metadata.Metadata{
					Labels: map[string]string{
						associationLabelName:      tt.args.kibana.Name,
						associationLabelNamespace: tt.args.kibana.Namespace,
					},
				},
				"kibana_system",
				"kibana-user",
				&tt.args.es,
				fixtures.MustTestRandomGenerator(24),
			); (err != nil) != tt.wantErr {
				t.Errorf("reconcileEsUser() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.postCondition(c)
		})
	}
}

// ChecksUser checks that a secret contains the required fields expected by the user reconciler.
func ChecksUser(t *testing.T, secret *corev1.Secret, expectedUsername string, expectedRoles []string) {
	t.Helper()
	assert.NotNil(t, secret)
	currentUsername, ok := secret.Data["name"]
	assert.True(t, ok)
	assert.Equal(t, expectedUsername, string(currentUsername))
	passwordHash, ok := secret.Data["passwordHash"]
	assert.True(t, ok)
	assert.NotEmpty(t, passwordHash)
	currentRoles, ok := secret.Data["userRoles"]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedRoles, strings.Split(string(currentRoles), ","))
}

// GetSecret gets the first secret in a list that matches the namespace and the name.
func GetSecret(list corev1.SecretList, namespacedName types.NamespacedName) *corev1.Secret {
	for _, secret := range list.Items {
		if secret.Namespace == namespacedName.Namespace && secret.Name == namespacedName.Name {
			return &secret
		}
	}
	return nil
}

func Test_reconcileEsUser_statelessES(t *testing.T) {
	// Test that user secrets for stateless ES use StatelessClusterNameLabelName
	essFixture := essv1alpha1.ElasticsearchStateless{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ess-foo",
			Namespace: "default",
			UID:       "f8d564d9-885e-11e9-896d-08002703f062",
		},
	}

	var kibanaFixtureUID types.UID = "82257b19-8862-11e9-896d-08002703f062"

	kibanaFixture := kbv1.Kibana{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kibana-foo",
			Namespace: "default",
			UID:       kibanaFixtureUID,
		},
		Spec: kbv1.KibanaSpec{
			ElasticsearchRef: commonv1.ElasticsearchRef{
				ObjectSelector: commonv1.ObjectSelector{
					Name:      essFixture.Name,
					Namespace: essFixture.Namespace,
				},
			},
		},
	}

	c := k8s.NewFakeClient()
	err := reconcileEsUserSecret(
		context.Background(),
		c,
		kibanaFixture.EsAssociation(),
		metadata.Metadata{
			Labels: map[string]string{
				associationLabelName:      kibanaFixture.Name,
				associationLabelNamespace: kibanaFixture.Namespace,
			},
		},
		"kibana_system",
		"kibana-user",
		&essFixture,
		fixtures.MustTestRandomGenerator(24),
	)
	require.NoError(t, err)

	// Check that the ES user secret was created with the correct stateless label
	userName := "default-kibana-foo-kibana-user-ess" // -ess suffix for stateless
	var esUser corev1.Secret
	err = c.Get(context.Background(), types.NamespacedName{Name: userName, Namespace: "default"}, &esUser)
	require.NoError(t, err)

	// Verify StatelessClusterNameLabelName is used instead of ClusterNameLabelName
	assert.Equal(t, "ess-foo", esUser.Labels[label.StatelessClusterNameLabelName])
	assert.Empty(t, esUser.Labels[label.ClusterNameLabelName])
	assert.Equal(t, esuser.AssociatedUserType, esUser.Labels[commonv1.TypeLabelName])
}

func Test_reconcileEsUser_statefulAndStatelessIsolation(t *testing.T) {
	// Test that secrets for stateful and stateless ES with the same name are properly isolated
	esFixture := esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "same-name",
			Namespace: "default",
			UID:       "es-uid",
		},
	}
	essFixture := essv1alpha1.ElasticsearchStateless{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "same-name",
			Namespace: "default",
			UID:       "ess-uid",
		},
	}

	kibanaForStateful := kbv1.Kibana{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kb-stateful",
			Namespace: "default",
			UID:       "kb-stateful-uid",
		},
		Spec: kbv1.KibanaSpec{
			ElasticsearchRef: commonv1.ElasticsearchRef{
				ObjectSelector: commonv1.ObjectSelector{
					Name:      esFixture.Name,
					Namespace: esFixture.Namespace,
				},
			},
		},
	}
	kibanaForStateless := kbv1.Kibana{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kb-stateless",
			Namespace: "default",
			UID:       "kb-stateless-uid",
		},
		Spec: kbv1.KibanaSpec{
			ElasticsearchRef: commonv1.ElasticsearchRef{
				ObjectSelector: commonv1.ObjectSelector{
					Name:      essFixture.Name,
					Namespace: essFixture.Namespace,
				},
			},
		},
	}

	c := k8s.NewFakeClient()

	// Create user for stateful ES
	err := reconcileEsUserSecret(
		context.Background(),
		c,
		kibanaForStateful.EsAssociation(),
		metadata.Metadata{Labels: map[string]string{
			associationLabelName:      kibanaForStateful.Name,
			associationLabelNamespace: kibanaForStateful.Namespace,
		}},
		"kibana_system",
		"kibana-user",
		&esFixture,
		fixtures.MustTestRandomGenerator(24),
	)
	require.NoError(t, err)

	// Create user for stateless ES
	err = reconcileEsUserSecret(
		context.Background(),
		c,
		kibanaForStateless.EsAssociation(),
		metadata.Metadata{Labels: map[string]string{
			associationLabelName:      kibanaForStateless.Name,
			associationLabelNamespace: kibanaForStateless.Namespace,
		}},
		"kibana_system",
		"kibana-user",
		&essFixture,
		fixtures.MustTestRandomGenerator(24),
	)
	require.NoError(t, err)

	// List all secrets
	var secrets corev1.SecretList
	err = c.List(context.Background(), &secrets)
	require.NoError(t, err)

	// Find secrets by their labels
	var statefulUserSecret, statelessUserSecret *corev1.Secret
	for i := range secrets.Items {
		s := &secrets.Items[i]
		if s.Labels[label.ClusterNameLabelName] == "same-name" && s.Labels[commonv1.TypeLabelName] == esuser.AssociatedUserType {
			statefulUserSecret = s
		}
		if s.Labels[label.StatelessClusterNameLabelName] == "same-name" && s.Labels[commonv1.TypeLabelName] == esuser.AssociatedUserType {
			statelessUserSecret = s
		}
	}

	// Both secrets should exist and be different
	require.NotNil(t, statefulUserSecret, "stateful user secret should exist")
	require.NotNil(t, statelessUserSecret, "stateless user secret should exist")
	require.NotEqual(t, statefulUserSecret.Name, statelessUserSecret.Name, "secrets should have different names")

	// Verify labels are correct
	assert.Empty(t, statefulUserSecret.Labels[label.StatelessClusterNameLabelName])
	assert.Empty(t, statelessUserSecret.Labels[label.ClusterNameLabelName])
}
