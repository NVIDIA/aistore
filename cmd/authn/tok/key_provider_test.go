// Package tok_test includes tests for tok pkg
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tok_test

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestValidateToken_InvalidKey(t *testing.T) {
	tokenStr, err := hmacSigner.SignToken(newAdminClaims())
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	authConf := &cmn.AuthConf{Signature: &cmn.AuthSignatureConf{Method: "hmac", Key: "invalid-secret"}}
	invalidKeyProvider, err := tok.NewStaticKeyProvider(authConf)
	tassert.CheckFatal(t, err)
	invalidParser := tok.NewTokenParser(invalidKeyProvider, nil)
	_, err = invalidParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with wrong key to fail")
}

func newHMACProvider(t *testing.T, secret string) *tok.StaticKeyProvider {
	authConf := &cmn.AuthConf{Signature: &cmn.AuthSignatureConf{Method: "hmac", Key: cmn.Censored(secret)}}
	prov, err := tok.NewStaticKeyProvider(authConf)
	tassert.CheckFatal(t, err)
	return prov
}

func newRSAProvider(t *testing.T, key *rsa.PrivateKey) *tok.StaticKeyProvider {
	pemStr := rsaPubKeyPEM(t, key)
	authConf := &cmn.AuthConf{Signature: &cmn.AuthSignatureConf{Method: "rsa", Key: cmn.Censored(pemStr)}}
	prov, err := tok.NewStaticKeyProvider(authConf)
	tassert.CheckFatal(t, err)
	return prov
}

func TestStaticKeyProvider_ValidateKey_HMAC(t *testing.T) {
	secret := testHMACSigningSecret
	prov := newHMACProvider(t, secret)
	correctHash := cos.ChecksumB2S(cos.UnsafeB(secret), cos.ChecksumSHA256)

	t.Run("MatchingSecret", func(t *testing.T) {
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{Secret: correctHash})
		tassert.Errorf(t, err == nil, "Expected matching secret to validate, got: %v", err)
		tassert.Errorf(t, code == 0, "Expected status 0, got %d", code)
	})

	t.Run("NonMatchingSecret", func(t *testing.T) {
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{Secret: "wrong-checksum"})
		tassert.Error(t, err != nil, "Expected non-matching secret to be rejected")
		tassert.Errorf(t, code == http.StatusForbidden, "Expected 403, got %d", code)
	})

	t.Run("RSAPubKeyNotConfigured", func(t *testing.T) {
		pemStr := "some-key"
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &pemStr})
		tassert.Error(t, err != nil, "Expected RSA key to be rejected when configured for HMAC")
		tassert.Errorf(t, code == http.StatusBadRequest, "Expected 400, got %d", code)
	})
}

func TestStaticKeyProvider_ValidateKey_RSA(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	tassert.CheckFatal(t, err)
	prov := newRSAProvider(t, rsaKey)

	t.Run("MatchingKey", func(t *testing.T) {
		pemStr := rsaPubKeyPEM(t, rsaKey)
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &pemStr})
		tassert.Errorf(t, err == nil, "Expected matching key to validate, got: %v", err)
		tassert.Errorf(t, code == 0, "Expected status 0, got %d", code)
	})

	t.Run("NonMatchingKey", func(t *testing.T) {
		otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
		tassert.CheckFatal(t, err)
		pemStr := rsaPubKeyPEM(t, otherKey)
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &pemStr})
		tassert.Error(t, err != nil, "Expected non-matching key to be rejected")
		tassert.Errorf(t, code == http.StatusForbidden, "Expected 403, got %d", code)
	})

	t.Run("InvalidPEM", func(t *testing.T) {
		bad := "not-a-valid-pem"
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &bad})
		tassert.Error(t, err != nil, "Expected invalid PEM to be rejected")
		tassert.Errorf(t, code == http.StatusBadRequest, "Expected 400, got %d", code)
	})

	t.Run("HMACSecretNotConfigured", func(t *testing.T) {
		code, err := prov.ValidateKey(t.Context(), &authn.ServerConf{Secret: "some-checksum"})
		tassert.Error(t, err != nil, "Expected HMAC secret to be rejected when configured for RSA")
		tassert.Errorf(t, code == http.StatusBadRequest, "Expected 400, got %d", code)
	})
}
