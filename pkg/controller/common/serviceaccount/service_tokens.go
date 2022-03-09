// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"sort"
	"strings"
)

type ServiceToken struct {
	FullyQualifiedServiceAccountName string
	HashedSecret                     SecureString
}

type ServiceTokens []ServiceToken

func (s *ServiceTokens) Add(fullyQualifiedServiceAccountName string, hashedSecret SecureString) *ServiceTokens {
	var newServiceTokens ServiceTokens
	newServiceToken := ServiceToken{
		FullyQualifiedServiceAccountName: fullyQualifiedServiceAccountName,
		HashedSecret:                     hashedSecret,
	}
	if s == nil {
		newServiceTokens = ServiceTokens{newServiceToken}
		return &newServiceTokens
	}
	// Remove the service token if it already exists
	for _, existingServiceToken := range *s {
		if existingServiceToken.FullyQualifiedServiceAccountName == fullyQualifiedServiceAccountName {
			continue
		}
		newServiceTokens = append(newServiceTokens, existingServiceToken)
	}
	newServiceTokens = append(newServiceTokens, newServiceToken)
	return &newServiceTokens
}

func (s *ServiceTokens) ToBytes() []byte {
	if s == nil {
		return []byte{}
	}
	// Ensure that the file is sorted
	sort.SliceStable(*s, func(i, j int) bool {
		return (*s)[i].FullyQualifiedServiceAccountName < (*s)[j].FullyQualifiedServiceAccountName
	})
	var result strings.Builder
	for _, serviceToken := range *s {
		result.WriteString(serviceToken.FullyQualifiedServiceAccountName)
		result.WriteString(":")
		result.WriteString(serviceToken.HashedSecret.Clear())
		result.WriteString("\n")
	}
	return []byte(result.String())
}
