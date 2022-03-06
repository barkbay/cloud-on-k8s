// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/types"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
)

type Name string

const (
	Namespace string = "elastic"

	Kibana      Name = "kibana"
	FleetServer Name = "fleet-server"
)

var (
	MinSupportedVersion = version.MustParse("8.0.0")
)

type TokenReference struct {
	SecretRef types.NamespacedName
	TokenName string
}

type ElasticsearchStore interface {
	EnsureElasticsearchStoreExists(tokens ...Token) error
}

type ApplicationStore interface {
	EnsureTokenExists(applicationName string, applicationUID types.UID, serviceAccount Name) (*TokenReference, error)

	DeleteToken() error
}

// Token stores all the required data for a given service account token.
type Token struct {
	ServiceAccountName string       `json:"serviceAccountName"`
	TokenName          string       `json:"tokenName"`
	Token              SecureString `json:"token"`
	Hash               SecureString `json:"hash"`
}

func (u Token) UnsecureMarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ServiceAccountName string       `json:"serviceAccountName"`
		TokenName          string       `json:"tokenName"`
		Token              SecureString `json:"token"`
		Hash               SecureString `json:"hash"`
	}{
		ServiceAccountName: u.ServiceAccountName,
		TokenName:          u.TokenName,
		Token:              u.Token,
		Hash:               u.Hash,
	})
}

func (u Token) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ServiceAccountName string       `json:"serviceAccountName"`
		TokenName          string       `json:"tokenName"`
		Token              SecureString `json:"token"`
		Hash               SecureString `json:"hash"`
	}{
		ServiceAccountName: u.ServiceAccountName,
		TokenName:          u.TokenName,
		Token:              "REDACTED",
		Hash:               "REDACTED",
	})
}
