// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package user

import (
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"

	"golang.org/x/crypto/pbkdf2"
	corev1 "k8s.io/api/core/v1"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
)

type ServiceAccountToken struct {
	FullyQualifiedServiceAccountName string
	HashedSecret                     string
}

type ServiceAccountTokens []ServiceAccountToken

const (
	ServiceTokensFileName = "service_tokens"

	Namespace string = "elastic"

	ServiceAccountNameField       = "serviceAccount"
	ServiceAccountTokenValueField = "token"
	ServiceAccountTokenNameField  = "name"
	ServiceAccountHashField       = "hash"
)

func GetOrCreateToken(
	es *esv1.Elasticsearch,
	secretName string,
	secretData map[string][]byte,
	serviceAccountName commonv1.ServiceAccountName,
	tokenName string,
) (*Token, error) {
	token := getCurrentApplicationToken(es, secretName, secretData)
	if token == nil {
		// We need to create a new token
		return NewApplicationToken(serviceAccountName, tokenName)
	}
	return token, nil
}

// getCurrentApplicationToken returns the current token from the application Secret, or nil if the content of the Secret is not valid.
func getCurrentApplicationToken(es *esv1.Elasticsearch, secretName string, secretData map[string][]byte) *Token {
	if len(secretData) == 0 {
		log.V(1).Info("secret is empty", "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	}
	result := &Token{}
	if value := getFieldOrNil(es, secretName, secretData, ServiceAccountTokenNameField); value != nil && len(*value) > 0 {
		result.TokenName = *value
	} else {
		return nil
	}

	if value := getFieldOrNil(es, secretName, secretData, ServiceAccountTokenValueField); value != nil && len(*value) > 0 {
		result.Token = *value
	} else {
		return nil
	}

	if value := getFieldOrNil(es, secretName, secretData, ServiceAccountHashField); value != nil && len(*value) > 0 {
		result.Hash = *value
	} else {
		return nil
	}

	if value := getFieldOrNil(es, secretName, secretData, ServiceAccountNameField); value != nil && len(*value) > 0 {
		result.ServiceAccountName = *value
	} else {
		return nil
	}

	return result
}

func getFieldOrNil(es *esv1.Elasticsearch, secretName string, secretData map[string][]byte, fieldName string) *string {
	data, exists := secretData[fieldName]
	if !exists {
		log.V(1).Info(fmt.Sprintf("%s field is missing in service account token Secret", fieldName), "es_name", es.Name, "namespace", es.Namespace, "secret", secretName)
		return nil
	}
	fieldValue := string(data)
	return &fieldValue
}

var prefix = [...]byte{0x0, 0x1, 0x0, 0x1}

// NewApplicationToken generates a new token for a given service account.
func NewApplicationToken(serviceAccountName commonv1.ServiceAccountName, tokenName string) (*Token, error) {
	secret := common.RandomBytes(64)
	hash, err := pbkdf2Key(secret)
	if err != nil {
		return nil, err
	}

	fullyQualifiedName := fmt.Sprintf("%s/%s", Namespace, serviceAccountName)
	suffix := []byte(fmt.Sprintf("%s/%s:%s", fullyQualifiedName, tokenName, secret))
	token := base64.StdEncoding.EncodeToString(append(prefix[:], suffix...))

	return &Token{
		ServiceAccountName: fullyQualifiedName,
		TokenName:          tokenName,
		Token:              token,
		Hash:               hash,
	}, nil
}

// Token stores all the required data for a given service account token.
type Token struct {
	ServiceAccountName string
	TokenName          string
	Token              string
	Hash               string
}

func (u Token) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ServiceAccountName string `json:"serviceAccountName"`
		TokenName          string `json:"tokenName"`
		Token              string `json:"token"`
		Hash               string `json:"hash"`
	}{
		ServiceAccountName: u.ServiceAccountName,
		TokenName:          u.TokenName,
		Token:              "REDACTED",
		Hash:               "REDACTED",
	})
}

// -- crypto

const (
	pbkdf2StretchPrefix     = "{PBKDF2_STRETCH}"
	pbkdf2DefaultCost       = 10000
	pbkdf2KeyLength         = 32
	pbkdf2DefaultSaltLength = 32
)

// pbkdf2Key derives a key from the provided secret, as expected by the service tokens file store in Elasticsearch.
func pbkdf2Key(secret []byte) (string, error) {
	var result strings.Builder
	result.WriteString(pbkdf2StretchPrefix)
	result.WriteString(strconv.Itoa(pbkdf2DefaultCost))
	result.WriteString("$")

	salt := make([]byte, pbkdf2DefaultSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	result.WriteString(base64.StdEncoding.EncodeToString(salt))
	result.WriteString("$")

	hashedSecret := sha512.Sum512(secret)
	hashedSecretAsString := hex.EncodeToString(hashedSecret[:])

	dk := pbkdf2.Key([]byte(hashedSecretAsString), salt, pbkdf2DefaultCost, pbkdf2KeyLength, sha512.New)
	result.WriteString(base64.StdEncoding.EncodeToString(dk))
	return result.String(), nil
}

// getServiceAccountToken reads a service account token from a secret.
func getServiceAccountToken(secret corev1.Secret) (ServiceAccountToken, error) {
	token := ServiceAccountToken{}
	if len(secret.Data) == 0 {
		return token, fmt.Errorf("service account token secret %s/%s is empty", secret.Namespace, secret.Name)
	}

	if serviceAccountName, ok := secret.Data[ServiceAccountTokenNameField]; ok && len(serviceAccountName) > 0 {
		token.FullyQualifiedServiceAccountName = string(serviceAccountName)
	} else {
		return token, fmt.Errorf(fieldNotFound, ServiceAccountTokenNameField, secret.Namespace, secret.Name)
	}

	if hash, ok := secret.Data[ServiceAccountHashField]; ok && len(hash) > 0 {
		token.HashedSecret = string(hash)
	} else {
		return token, fmt.Errorf(fieldNotFound, ServiceAccountHashField, secret.Namespace, secret.Name)
	}

	return token, nil
}

func (s ServiceAccountTokens) Add(serviceAccountToken ServiceAccountToken) ServiceAccountTokens {
	return append(s, serviceAccountToken)
}

func (s *ServiceAccountTokens) ToBytes() []byte {
	if s == nil {
		return []byte{}
	}
	// Ensure that the file is sorted for stable comparison.
	sort.SliceStable(*s, func(i, j int) bool {
		return (*s)[i].FullyQualifiedServiceAccountName < (*s)[j].FullyQualifiedServiceAccountName
	})
	var result strings.Builder
	for _, serviceToken := range *s {
		result.WriteString(serviceToken.FullyQualifiedServiceAccountName)
		result.WriteString(":")
		result.WriteString(serviceToken.HashedSecret)
		result.WriteString("\n")
	}
	return []byte(result.String())
}
