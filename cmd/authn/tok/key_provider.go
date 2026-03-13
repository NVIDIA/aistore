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
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/golang-jwt/jwt/v5"
)

type (
	KeyProvider interface {
		// ResolveKey returns a key that should be used to validate the given token
		ResolveKey(ctx context.Context, tok *jwt.Token) (any, error)
	}

	// ServerKeyProvider extends KeyProvider with functionality only used on the AIS server side (not authN service)
	ServerKeyProvider interface {
		KeyProvider
		// ValidateKey checks a given public key or secret checksum and returns an error iff it is invalid
		ValidateKey(ctx context.Context, conf *authn.ServerConf) (int, error)
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
		return nil, nil
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
		return []byte(s.hmacSecret), nil
	case *jwt.SigningMethodRSA:
		return s.rsaPublicKey, nil
	default:
		return nil, fmt.Errorf("unsupported signing method %v, header specified %s", t.Method, t.Header["alg"])
	}
}

// ValidateKey checks if the request struct contains a key or checksum consistent with our current config
func (s *StaticKeyProvider) ValidateKey(_ context.Context, reqConf *authn.ServerConf) (int, error) {
	// If RSA public key is provided
	if reqConf.PubKey != nil {
		if s.rsaPublicKey == nil {
			return http.StatusBadRequest, errors.New("cannot validate public key: AIS not configured with RSA")
		}
		reqKey, err := parsePubKey(*reqConf.PubKey)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("invalid public key (%q)", cos.SHead(*reqConf.PubKey))
		}
		if !reqKey.Equal(s.rsaPublicKey) {
			return http.StatusForbidden, fmt.Errorf("provided public key (%q) does not match cluster's public key", cos.SHead(*reqConf.PubKey))
		}
	}

	// If HMAC secret checksum is provided
	if reqConf.Secret != "" {
		secStr := string(s.hmacSecret)
		if secStr == "" {
			return http.StatusBadRequest, errors.New("cannot validate secret checksum: AIS not configured with HMAC secret")
		}
		currentHash := cos.ChecksumB2S(cos.UnsafeB(secStr), cos.ChecksumSHA256)
		if currentHash != reqConf.Secret {
			return http.StatusForbidden, fmt.Errorf("invalid secret sha256(%q)", cos.SHead(reqConf.Secret))
		}
	}
	return 0, nil
}
