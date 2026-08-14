// Package tok provides AuthN token (structure and methods)
// for validation by AIS gateways
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tok

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"

	"github.com/golang-jwt/jwt/v5"
)

type (
	KeyProvider interface {
		// ResolveKey returns a key that should be used to validate the given token
		ResolveKey(ctx context.Context, tok *jwt.Token) (any, error)
	}

	StaticKeyProvider struct {
		hmacSecret   cmn.Censored
		rsaPublicKey *rsa.PublicKey
	}
)

var (
	errRSAParse    = errors.New("failed to parse RSA public key")
	ErrNoStaticKey = errors.New("no static key in config")
)

func NewStaticKeyProvider(conf *cmn.AuthConf) (*StaticKeyProvider, error) {
	// First check for env vars as they take precedence
	if pubKeyEnvStr := os.Getenv(env.AisAuthPublicKey); pubKeyEnvStr != "" {
		pubKey, err := parsePubKey(pubKeyEnvStr)
		if err != nil {
			return nil, fmt.Errorf("%v: %v", errRSAParse, err)
		}
		return &StaticKeyProvider{rsaPublicKey: pubKey}, nil
	}
	if hmacEnvStr := os.Getenv(env.AisAuthSecretKey); hmacEnvStr != "" {
		return &StaticKeyProvider{hmacSecret: cmn.Censored(hmacEnvStr)}, nil
	}
	// If no env vars and no config, exit with no error - not configured for static validation
	if conf.Signature == nil {
		return nil, ErrNoStaticKey
	}
	// Finally check config -- parse according to provided method
	m := strings.ToUpper(conf.Signature.Method)
	switch {
	case conf.Signature.IsHMAC():
		return &StaticKeyProvider{hmacSecret: conf.Signature.Key}, nil
	case conf.Signature.IsRSA():
		pubKey, err := parsePubKey(string(conf.Signature.Key))
		if err != nil {
			return nil, fmt.Errorf("failed to parse RSA public key: %v", err)
		}
		return &StaticKeyProvider{rsaPublicKey: pubKey}, nil
	default:
		return nil, fmt.Errorf("auth enabled with invalid key signature: %q. Supported values are: %s", m, conf.Signature.ValidMethods())
	}
}

func parsePubKey(str string) (*rsa.PublicKey, error) {
	if str == "" {
		return nil, errors.New("empty public key string")
	}
	var derBytes []byte
	var err error

	// Try PEM format first
	if block, _ := pem.Decode([]byte(str)); block != nil {
		derBytes = block.Bytes
	} else {
		// Fall back to raw base64 DER
		derBytes, err = base64.StdEncoding.DecodeString(str)
		if err != nil {
			return nil, fmt.Errorf("invalid public key format: %w", err)
		}
	}
	pub, err := x509.ParsePKIXPublicKey(derBytes)
	if err != nil {
		return nil, err
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("not an RSA public key")
	}
	return rsaPub, nil
}

// ResolveKey for static provider resolves key directly from config
func (s *StaticKeyProvider) ResolveKey(_ context.Context, t *jwt.Token) (any, error) {
	switch t.Method.(type) {
	case *jwt.SigningMethodHMAC:
		if s.hmacSecret == "" {
			return nil, errors.New("HMAC secret not configured")
		}
		return []byte(s.hmacSecret), nil
	case *jwt.SigningMethodRSA:
		if s.rsaPublicKey == nil {
			return nil, errors.New("RSA public key not configured")
		}
		return s.rsaPublicKey, nil
	default:
		return nil, fmt.Errorf("unsupported signing method %v, header specified %s", t.Method, t.Header["alg"])
	}
}
