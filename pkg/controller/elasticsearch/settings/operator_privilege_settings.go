// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package settings

import (
	"fmt"
)

type OperatorRealmType string

const (
	OperatorRealmTypeFile           OperatorRealmType = "file"
	OperatorRealmTypeServiceAccount OperatorRealmType = "_service_account"
	OperatorRealmTypeJWT            OperatorRealmType = "jwt"

	OperatorUsersSettingsFileName = "operator_users.yml"
	TokenAuthType                 = "token"
	RealmAuthType                 = "realm"
)

type OperatorPrivilegesSetting struct {
	Usernames []string          `yaml:"usernames"`
	RealmType OperatorRealmType `yaml:"realm_type"`
	RealmName string            `yaml:"realm_name,omitempty"`
	AuthType  string            `yaml:"auth_type,omitempty"`
	// Token auth type specific fields
	TokenSource string   `yaml:"token_source,omitempty"`
	TokenNames  []string `yaml:"token_names,omitempty"`
}

type OperatorPrivilegesSettings struct {
	Operator []OperatorPrivilegesSetting `yaml:"operator"`
}

type OperatorAccount struct {
	Names     []string          `mapstructure:"names" validate:"required"`
	RealmType OperatorRealmType `mapstructure:"realm_type" validate:"required"`
}

func NewOperatorPrivilegesSettings(operatorUsernames []OperatorAccount) (OperatorPrivilegesSettings, error) {
	var operatorPrivilegesSettings OperatorPrivilegesSettings
	for _, operatorUsername := range operatorUsernames {
		switch operatorUsername.RealmType {
		case OperatorRealmTypeFile:
			operatorPrivilegesSettings.Operator = append(operatorPrivilegesSettings.Operator, OperatorPrivilegesSetting{
				Usernames: operatorUsername.Names,
				RealmType: operatorUsername.RealmType,
				AuthType:  RealmAuthType,
			})
		default:
			return OperatorPrivilegesSettings{}, fmt.Errorf("unknown realm type %s, known realm types are %s and %s",
				operatorUsername.RealmType, OperatorRealmTypeServiceAccount, OperatorRealmTypeFile)
		}
	}
	return operatorPrivilegesSettings, nil
}
