// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

const (
	RSAKeyMinBits     = 2048
	RSAKeyDefaultBits = RSAKeyMinBits

	// algorithm-agnostic PKCS#8 / PKIX PEM block labels
	// (shared with other key types, e.g. Ed25519)
	PEMTypePrivateKey = "PRIVATE KEY"
	PEMTypePublicKey  = "PUBLIC KEY"
)

func GenerateRSAKey(bits int) (*rsa.PrivateKey, error) {
	if bits == 0 {
		bits = RSAKeyDefaultBits
	}
	if bits < RSAKeyMinBits {
		return nil, fmt.Errorf("invalid RSA key size %d (must be >= %d)", bits, RSAKeyMinBits)
	}
	// NOTE: rsa.GenerateKey output is valid by construction - no Validate() here
	return rsa.GenerateKey(rand.Reader, bits)
}

func EncodeRSAPrivateKeyPEM(key *rsa.PrivateKey) ([]byte, error) {
	if key == nil {
		return nil, errors.New("nil RSA private key")
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal RSA private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: PEMTypePrivateKey, Bytes: der}), nil
}

func ParseRSAPrivateKeyPEM(b []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("decoding RSA private key PEM block failed")
	}
	if block.Type != PEMTypePrivateKey {
		return nil, fmt.Errorf("unexpected RSA private key PEM type %q", block.Type)
	}

	keyAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS8 private key: %w", err)
	}
	key, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected RSA private key, got %T", keyAny)
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("validate RSA private key: %w", err)
	}
	return key, nil
}

func EncodeRSAPublicKeyPEM(pub *rsa.PublicKey) ([]byte, error) {
	if pub == nil {
		return nil, errors.New("nil RSA public key")
	}

	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("marshal RSA public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: PEMTypePublicKey, Bytes: der}), nil
}
