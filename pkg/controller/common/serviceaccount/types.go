// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/types"
)

type Name string

const (
	Namespace string = "elastic"

	Kibana      Name = "kibana"
	FleetServer Name = "fleet-server"
)

type TokenReference struct {
	SecretRef types.NamespacedName
	TokenName string
}

type ElasticsearchStore interface {
	EnsureElasticsearchStoreExists(tokens ...Token) error
}

type SecretsManager interface {
	EnsureTokenExists(applicationName string, applicationUID types.UID, serviceAccount Name) (*TokenReference, error)
}

// Token stores all the required data for a given service account token.
type Token struct {
	ServiceAccountName string       `json:"serviceAccountName"`
	TokenName          string       `json:"tokenName"`
	Token              SecureString `json:"token"`
	Hash               SecureString `json:"hash"`
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
