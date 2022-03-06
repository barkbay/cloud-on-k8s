// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package serviceaccount

import (
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"

	"golang.org/x/crypto/pbkdf2"

	assert "github.com/stretchr/testify/assert"
)

func TestSecureString(t *testing.T) {
	tests := []struct {
		name  string
		token Token
		want  string
	}{
		{
			name: "Secret data should not be serialized",
			token: Token{
				ServiceAccountName: "service_account",
				TokenName:          "token_name",
				Token:              SecureString("secret"),
				Hash:               SecureString("secret"),
			},
			want: `{"serviceAccountName":"service_account","tokenName":"token_name","token":"REDACTED","hash":"REDACTED"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ser, err := json.Marshal(tt.token)
			assert.Nil(t, err)
			assert.Equal(t, string(ser), tt.want)
		})
	}
}

func Test_hash(t *testing.T) {
	type args struct {
		secret []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Simple hash test",
			args: args{
				secret: []byte("asecret"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := hash(tt.args.secret)
			if (err != nil) != tt.wantErr {
				t.Errorf("hash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				verify(t, tt.args.secret, *got)
			}
		})
	}
}

func verify(t *testing.T, password []byte, hash string) {
	t.Helper()

	// password is hashed using sha512 and converted to a hex string
	hashedSecret := sha512.Sum512([]byte(password))
	hashedSecretAsString := hex.EncodeToString(hashedSecret[:])

	// Base64 string length : (4*(n/3)) rounded up to the next multiple of 4 because of padding.
	// n is 32 (PBKDF2_KEY_LENGTH in bytes), so tokenLength is 44
	tokenLength := 44
	hashChars := hash[len(hash)-tokenLength : len(hash)]
	saltChars := hash[len(hash)-(2*tokenLength+1) : len(hash)-(tokenLength+1)]
	salt, err := base64.StdEncoding.DecodeString(saltChars)
	assert.NoError(t, err)

	costChars := hash[len(pbkdf2StretchPrefix) : len(hash)-(2*tokenLength+2)]
	cost, err := strconv.Atoi(costChars)
	assert.NoError(t, err)

	dk := pbkdf2.Key([]byte(hashedSecretAsString), salt, cost, pbkdf2KeyLength, sha512.New)
	computedPwdHash := base64.StdEncoding.EncodeToString(dk)

	assert.Equal(t, hashChars, computedPwdHash)
}
