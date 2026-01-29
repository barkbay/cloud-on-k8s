// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package user

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateful/v1"
	essv1alpha1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/stateless/v1alpha1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

func TestAssociatedUserLabels(t *testing.T) {
	tests := []struct {
		name string
		es   func() interface {
			GetName() string
			IsStateless() bool
		}
		wantLabels map[string]string
	}{
		{
			name: "stateful Elasticsearch uses ClusterNameLabelName",
			es: func() interface {
				GetName() string
				IsStateless() bool
			} { return &esv1.Elasticsearch{
				ObjectMeta: metav1.ObjectMeta{Name: "my-es", Namespace: "ns"},
			} },
			wantLabels: map[string]string{
				label.ClusterNameLabelName: "my-es",
				commonv1.TypeLabelName:     AssociatedUserType,
			},
		},
		{
			name: "stateless ElasticsearchStateless uses StatelessClusterNameLabelName",
			es: func() interface {
				GetName() string
				IsStateless() bool
			} { return &essv1alpha1.ElasticsearchStateless{
				ObjectMeta: metav1.ObjectMeta{Name: "my-ess", Namespace: "ns"},
			} },
			wantLabels: map[string]string{
				label.StatelessClusterNameLabelName: "my-ess",
				commonv1.TypeLabelName:              AssociatedUserType,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We need to use the interface that AssociatedUserLabels expects
			es := tt.es()
			switch e := es.(type) {
			case *esv1.Elasticsearch:
				got := AssociatedUserLabels(e)
				require.Equal(t, tt.wantLabels, got)
			case *essv1alpha1.ElasticsearchStateless:
				got := AssociatedUserLabels(e)
				require.Equal(t, tt.wantLabels, got)
			}
		})
	}
}

func Test_retrieveAssociatedUsers(t *testing.T) {
	es := &esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{Name: "es", Namespace: "ns"},
	}
	tests := []struct {
		name    string
		secrets []client.Object
		want    users
	}{
		{
			name:    "no associated user secret",
			secrets: nil,
			want:    users{},
		},
		{
			name: "some associated users secrets",
			secrets: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: es.Namespace,
						Name:      "user1",
						Labels:    AssociatedUserLabels(es),
					},
					Data: map[string][]byte{
						UserNameField:     []byte("user1"),
						PasswordHashField: []byte("passwordHash1"),
						UserRolesField:    []byte("role1,role2"),
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: es.Namespace,
						Name:      "user2",
						Labels:    AssociatedUserLabels(es),
					},
					Data: map[string][]byte{
						UserNameField:     []byte("user2"),
						PasswordHashField: []byte("passwordHash2"),
						UserRolesField:    []byte("role1,role2,role3"),
					},
				},
			},
			want: users{
				{Name: "user1", PasswordHash: []byte("passwordHash1"), Roles: []string{"role1", "role2"}},
				{Name: "user2", PasswordHash: []byte("passwordHash2"), Roles: []string{"role1", "role2", "role3"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := k8s.NewFakeClient(tt.secrets...)
			users, err := retrieveAssociatedUsers(c, es)
			require.NoError(t, err)
			require.Equal(t, tt.want, users)
		})
	}
}

func Test_retrieveAssociatedUsers_stateless(t *testing.T) {
	ess := &essv1alpha1.ElasticsearchStateless{
		ObjectMeta: metav1.ObjectMeta{Name: "ess", Namespace: "ns"},
	}
	// Also create a stateful ES with same name/namespace to verify isolation
	es := &esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{Name: "ess", Namespace: "ns"},
	}

	tests := []struct {
		name    string
		secrets []client.Object
		want    users
	}{
		{
			name:    "no associated user secret for stateless",
			secrets: nil,
			want:    users{},
		},
		{
			name: "stateless ES only retrieves secrets with StatelessClusterNameLabelName",
			secrets: []client.Object{
				// Secret for stateless ES - should be found
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: ess.Namespace,
						Name:      "stateless-user",
						Labels:    AssociatedUserLabels(ess),
					},
					Data: map[string][]byte{
						UserNameField:     []byte("stateless-user"),
						PasswordHashField: []byte("passwordHash1"),
						UserRolesField:    []byte("kibana_system"),
					},
				},
				// Secret for stateful ES with same name - should NOT be found
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: es.Namespace,
						Name:      "stateful-user",
						Labels:    AssociatedUserLabels(es),
					},
					Data: map[string][]byte{
						UserNameField:     []byte("stateful-user"),
						PasswordHashField: []byte("passwordHash2"),
						UserRolesField:    []byte("kibana_system"),
					},
				},
			},
			want: users{
				{Name: "stateless-user", PasswordHash: []byte("passwordHash1"), Roles: []string{"kibana_system"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := k8s.NewFakeClient(tt.secrets...)
			users, err := retrieveAssociatedUsers(c, ess)
			require.NoError(t, err)
			require.Equal(t, tt.want, users)
		})
	}
}

func Test_retrieveAssociatedUsers_isolation(t *testing.T) {
	// Test that stateful and stateless ES with same name/namespace are properly isolated
	ess := &essv1alpha1.ElasticsearchStateless{
		ObjectMeta: metav1.ObjectMeta{Name: "same-name", Namespace: "ns"},
	}
	es := &esv1.Elasticsearch{
		ObjectMeta: metav1.ObjectMeta{Name: "same-name", Namespace: "ns"},
	}

	secrets := []client.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "stateless-user",
				Labels:    AssociatedUserLabels(ess),
			},
			Data: map[string][]byte{
				UserNameField:     []byte("stateless-user"),
				PasswordHashField: []byte("hash1"),
				UserRolesField:    []byte("role1"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "stateful-user",
				Labels:    AssociatedUserLabels(es),
			},
			Data: map[string][]byte{
				UserNameField:     []byte("stateful-user"),
				PasswordHashField: []byte("hash2"),
				UserRolesField:    []byte("role2"),
			},
		},
	}

	c := k8s.NewFakeClient(secrets...)

	// Stateless ES should only find stateless user
	statelessUsers, err := retrieveAssociatedUsers(c, ess)
	require.NoError(t, err)
	require.Len(t, statelessUsers, 1)
	require.Equal(t, "stateless-user", statelessUsers[0].Name)

	// Stateful ES should only find stateful user
	statefulUsers, err := retrieveAssociatedUsers(c, es)
	require.NoError(t, err)
	require.Len(t, statefulUsers, 1)
	require.Equal(t, "stateful-user", statefulUsers[0].Name)
}

func Test_parseAssociatedUserSecret(t *testing.T) {
	type args struct {
		secret corev1.Secret
	}
	tests := []struct {
		name    string
		args    args
		want    AssociatedUser
		wantErr bool
	}{
		{
			name:    "Simple kibana example",
			wantErr: false,
			args: args{
				secret: corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns2-kibana-sample-kibana-user",
						Namespace: "default",
					},
					Data: map[string][]byte{
						UserNameField:     []byte("ns2-kibana-sample-kibana-user"),
						PasswordHashField: []byte("$2a$10$D6q/zdYfGJsJxipsZ4Jioul8tWIcL.o.Mhx/as1nlNdOX6EgqRRRS"),
						UserRolesField:    []byte("kibana_system"),
					},
				},
			},
			want: AssociatedUser{
				Name:         "ns2-kibana-sample-kibana-user",
				PasswordHash: []byte("$2a$10$D6q/zdYfGJsJxipsZ4Jioul8tWIcL.o.Mhx/as1nlNdOX6EgqRRRS"),
				Roles:        []string{"kibana_system"},
			},
		},
		{
			name:    "Multi-roles example",
			wantErr: false,
			args: args{
				secret: corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns2-kibana-sample-kibana-user",
						Namespace: "default",
					},
					Data: map[string][]byte{
						UserNameField:     []byte("ns2-kibana-sample-kibana-user"),
						PasswordHashField: []byte("$2a$10$D6q/zdYfGJsJxipsZ4Jioul8tWIcL.o.Mhx/as1nlNdOX6EgqRRRS"),
						UserRolesField:    []byte("kibana_system1,kibana_system2,kibana_system3"),
					},
				},
			},
			want: AssociatedUser{
				Name:         "ns2-kibana-sample-kibana-user",
				PasswordHash: []byte("$2a$10$D6q/zdYfGJsJxipsZ4Jioul8tWIcL.o.Mhx/as1nlNdOX6EgqRRRS"),
				Roles:        []string{"kibana_system1", "kibana_system2", "kibana_system3"},
			},
		},
		{
			name:    "User name is missing",
			wantErr: true,
			args: args{
				secret: corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns2-kibana-sample-kibana-user",
						Namespace: "default",
					},
					Data: map[string][]byte{
						PasswordHashField: []byte("$2a$10$D6q/zdYfGJsJxipsZ4Jioul8tWIcL.o.Mhx/as1nlNdOX6EgqRRRS"),
						UserRolesField:    []byte("kibana_system"),
					},
				},
			},
			want: AssociatedUser{},
		},
		{
			name:    "Password is missing",
			wantErr: true,
			args: args{
				secret: corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ns2-kibana-sample-kibana-user",
						Namespace: "default",
					},
					Data: map[string][]byte{
						UserNameField:  []byte("ns2-kibana-sample-kibana-user"),
						UserRolesField: []byte("kibana_system"),
					},
				},
			},
			want: AssociatedUser{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAssociatedUserSecret(tt.args.secret)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseAssociatedUserSecret() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseAssociatedUserSecret() = %v, want %v", got, tt.want)
			}
		})
	}
}
