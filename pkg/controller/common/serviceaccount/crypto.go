// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"crypto/rand"
	sha512 "crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

type SecureString string

func (s *SecureString) Clear() string {
	if s == nil {
		return ""
	}
	return string(*s)
}

func (s *SecureString) String() string {
	if s == nil {
		return ""
	}
	return "REDACTED"
}

const (
	pbkdf2StretchPrefix = "{PBKDF2_STRETCH}"

	pbkdf2DefaultCost       = 10000
	pbkdf2KeyLength         = 32
	pbkdf2DefaultSaltLength = 32
)

func hash(secret []byte) (*string, error) {
	var result strings.Builder
	result.WriteString(pbkdf2StretchPrefix)
	result.WriteString(strconv.Itoa(pbkdf2DefaultCost))
	result.WriteString("$")

	salt := make([]byte, pbkdf2DefaultSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	result.WriteString(base64.StdEncoding.EncodeToString(salt))
	result.WriteString("$")

	hashedSecret := sha512.Sum512(secret)
	hashedSecretAsString := hex.EncodeToString(hashedSecret[:])

	dk := pbkdf2.Key([]byte(hashedSecretAsString), salt, pbkdf2DefaultCost, pbkdf2KeyLength, sha512.New)
	result.WriteString(base64.StdEncoding.EncodeToString(dk))
	hash := result.String()
	return &hash, nil
}
