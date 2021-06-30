// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package certificates

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"github.com/pkg/errors"
)

var ErrEncryptedPrivateKey = errors.New("encrypted private key")

// ParsePEMCerts returns a list of certificates from the given PEM certs data
// Based on the code of x509.CertPool.AppendCertsFromPEM (https://golang.org/src/crypto/x509/cert_pool.go)
// We don't rely on x509.CertPool.AppendCertsFromPEM directly here since it returns an interface from which
// we cannot extract the actual certificates if we need to compare them.
func ParsePEMCerts(pemData []byte) ([]*x509.Certificate, error) {
	certs := []*x509.Certificate{}
	for len(pemData) > 0 {
		var block *pem.Block
		block, pemData = pem.Decode(pemData)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		certs = append(certs, cert)
	}
	return certs, nil
}

// EncodePEMCert encodes the given certificate blocks as a PEM certificate
func EncodePEMCert(certBlocks ...[]byte) []byte {
	var buf bytes.Buffer
	for _, block := range certBlocks {
		_, _ = buf.Write(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block}))
	}
	return buf.Bytes()
}

// EncodePEMPrivateKey encodes the given private key in the PEM format
func EncodePEMPrivateKey(privateKey crypto.Signer) ([]byte, error) {
	pemBlock, err := pemBlockForKey(privateKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(pemBlock), nil
}

func pemBlockForKey(privateKey interface{}) (*pem.Block, error) {
	switch k := privateKey.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}, nil
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, err
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}, nil
	default:
		// attempt PKCS#8 format
		b, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			return nil, err
		}
		return &pem.Block{Type: "PRIVATE KEY", Bytes: b}, nil
	}
}

// ParsePEMPrivateKey parses the given private key in the PEM format
// ErrEncryptedPrivateKey is returned as an error if the private key is encrypted.
func ParsePEMPrivateKey(pemData []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing private key")
	}

	switch {
	case x509.IsEncryptedPEMBlock(block): //nolint:staticcheck
		// Private key is encrypted, do not attempt to parse it
		return nil, ErrEncryptedPrivateKey
	case block.Type == "PRIVATE KEY":
		return parsePKCS8PrivateKey(block.Bytes)
	case block.Type == "RSA PRIVATE KEY" && len(block.Headers) == 0:
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	case block.Type == "EC PRIVATE KEY":
		return parseECPrivateKey(block.Bytes)
	default:
		return nil, errors.New("expected PEM block to contain an RSA private key")
	}
}

func parseECPrivateKey(block []byte) (*ecdsa.PrivateKey, error) {
	return x509.ParseECPrivateKey(block)
}

func parsePKCS8PrivateKey(block []byte) (*rsa.PrivateKey, error) {
	key, err := x509.ParsePKCS8PrivateKey(block)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse private key")
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.Errorf("expected an RSA private key but got %t", key)
	}

	return rsaKey, nil
}

// GetPrimaryCertificate returns the primary certificate (i.e. the actual subject, not a CA or intermediate) from a PEM certificate chain
func GetPrimaryCertificate(pemBytes []byte) (*x509.Certificate, error) {
	parsedCerts, err := ParsePEMCerts(pemBytes)
	if err != nil {
		return nil, err
	}
	// the primary certificate should always come first, see:
	// http://tools.ietf.org/html/rfc4346#section-7.4.2
	if len(parsedCerts) < 1 {
		return nil, errors.New("Expected at least one certificate")
	}
	return parsedCerts[0], nil
}

// PrivateMatchesPublicKey returns true if the public and private keys correspond to each other.
func PrivateMatchesPublicKey(publicKey crypto.PublicKey, privateKey crypto.Signer) bool {
	switch k := publicKey.(type) {
	case *rsa.PublicKey:
		return k.Equal(privateKey.Public())
	case *ecdsa.PublicKey:
		return k.Equal(privateKey.Public())
	default:
		log.Error(fmt.Errorf("unsupported public key type: %T", publicKey), "")
		return false
	}
}
