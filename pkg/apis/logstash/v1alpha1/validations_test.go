// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	commonv1 "github.com/elastic/cloud-on-k8s/v2/pkg/apis/common/v1"
)

func TestCheckNameLength(t *testing.T) {
	testCases := []struct {
		name         string
		logstashName string
		wantErr      bool
		wantErrMsg   string
	}{
		{
			name:         "valid configuration",
			logstashName: "test-logstash",
			wantErr:      false,
		},
		{
			name:         "long Logstash name",
			logstashName: "extremely-long-winded-and-unnecessary-name-for-logstash",
			wantErr:      true,
			wantErrMsg:   "name exceeds maximum allowed length",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls := Logstash{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.logstashName,
					Namespace: "test",
				},
				Spec: LogstashSpec{},
			}

			errList := checkNameLength(&ls)
			assert.Equal(t, tc.wantErr, len(errList) > 0)
		})
	}
}

func TestCheckNoUnknownFields(t *testing.T) {
	type args struct {
		prev *Logstash
		curr *Logstash
	}
	tests := []struct {
		name string
		args args
		want field.ErrorList
	}{
		{
			name: "No downgrade",
			args: args{
				prev: &Logstash{Spec: LogstashSpec{Version: "7.17.0"}},
				curr: &Logstash{Spec: LogstashSpec{Version: "8.6.1"}},
			},
			want: nil,
		},
		{
			name: "Downgrade NOK",
			args: args{
				prev: &Logstash{Spec: LogstashSpec{Version: "8.6.1"}},
				curr: &Logstash{Spec: LogstashSpec{Version: "8.5.0"}},
			},
			want: field.ErrorList{&field.Error{Type: field.ErrorTypeForbidden, Field: "spec.version", BadValue: "", Detail: "Version downgrades are not supported"}},
		},
		{
			name: "Downgrade with override OK",
			args: args{
				prev: &Logstash{Spec: LogstashSpec{Version: "8.6.1"}},
				curr: &Logstash{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					commonv1.DisableDowngradeValidationAnnotation: "true",
				}}, Spec: LogstashSpec{Version: "8.5.0"}},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, checkNoDowngrade(tt.args.prev, tt.args.curr), "checkNoDowngrade(%v, %v)", tt.args.prev, tt.args.curr)
		})
	}
}

func Test_checkSingleConfigSource(t *testing.T) {
	tests := []struct {
		name     string
		logstash Logstash
		wantErr  bool
	}{
		{
			name: "configRef absent, config present",
			logstash: Logstash{
				Spec: LogstashSpec{
					Config: &commonv1.Config{},
				},
			},
			wantErr: false,
		},
		{
			name: "config absent, configRef present",
			logstash: Logstash{
				Spec: LogstashSpec{
					ConfigRef: &commonv1.ConfigSource{},
				},
			},
			wantErr: false,
		},
		{
			name: "neither present",
			logstash: Logstash{
				Spec: LogstashSpec{},
			},
			wantErr: false,
		},
		{
			name: "both present",
			logstash: Logstash{
				Spec: LogstashSpec{
					Config:    &commonv1.Config{},
					ConfigRef: &commonv1.ConfigSource{},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkSingleConfigSource(&tc.logstash)
			assert.Equal(t, tc.wantErr, len(got) > 0)
		})
	}
}

func Test_checkSupportedVersion(t *testing.T) {
	for _, tt := range []struct {
		name    string
		version string
		wantErr bool
	}{
		{
			name:    "below min supported",
			version: "8.5.0",
			wantErr: true,
		},
		{
			name:    "above max supported",
			version: "9.0.0",
			wantErr: true,
		},
		{
			name:    "above min supported",
			version: "8.7.1",
			wantErr: false,
		},
		{
			name:    "at min supported",
			version: "8.7.0",
			wantErr: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := Logstash{
				Spec: LogstashSpec{
					Version: tt.version,
				},
			}
			got := checkSupportedVersion(&a)
			assert.Equal(t, tt.wantErr, len(got) > 0)
		})
	}
}
