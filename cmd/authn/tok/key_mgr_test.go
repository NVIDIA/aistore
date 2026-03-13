// Package tok_test includes tests for tok pkg
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tok_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

var (
	cacheConfNoRetry = &tok.CacheConfig{
		DiscoveryConf: &tok.DiscoveryConf{
			Retries:   0,
			BaseDelay: time.Duration(0),
		},
		MinCacheRefreshInterval: nil,
	}
	invalidIssuerURLs = []string{
		"http://not-https.com/jwks",  // not https
		"not-absolute",               // not absolute
		"://missing-scheme",          // malformed URL
		"https://",                   // missing host
		"/relative/path",             // relative URL
		"https://?query=missinghost", // missing host, query only
		"https://host:invalidport",   // invalid port
	}
)

func generateTestJWKS(t *testing.T, privKey *rsa.PrivateKey, keyID string) string {
	pubJWK, err := jwk.FromRaw(privKey.Public())
	tassert.CheckFatal(t, err)
	_ = pubJWK.Set(jwk.KeyIDKey, keyID)
	jwks := jwk.NewSet()
	err = jwks.AddKey(pubJWK)
	tassert.CheckFatal(t, err)
	raw, err := json.Marshal(jwks)
	tassert.CheckFatal(t, err)
	return string(raw)
}

func createMockJWKSServer(jwksJSON string) *httptest.Server {
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, jwksJSON)
	}))
}

func createMockOIDCServer(jwksURL string) *httptest.Server {
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			resp := map[string]string{
				"jwks_uri": jwksURL,
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		http.NotFound(w, r)
	}))
}

func createTokenWithKeyID(t *testing.T, claims jwt.Claims, rsaKey *rsa.PrivateKey, keyID string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = keyID
	res, err := token.SignedString(rsaKey)
	tassert.CheckFatal(t, err)
	return res
}

// For testing tokens with public key lookup by token issuer

type testTokenEnv struct {
	keyID    string
	rsaKey   *rsa.PrivateKey
	jwksJSON string
	jwksSrv  *httptest.Server
	oidcSrv  *httptest.Server
	keyCache *tok.KeyCacheManager
	tkParser *tok.TokenParser
}

func setupTokenTestEnv(t *testing.T, init bool) testTokenEnv {
	keyID := "123"
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	tassert.Errorf(t, err == nil, "Failed to create rsa signing key: %v", err)
	// Get the JWKS we'd expect the issuer to give to match this key info
	jwksJSON := generateTestJWKS(t, rsaKey, keyID)
	// Mock JWKS server will return a JWKS including an entry for this key ID
	jwksS := createMockJWKSServer(jwksJSON)
	// Mock OIDC discovery server will return the URL to the JWKS endpoint
	oidcS := createMockOIDCServer(jwksS.URL)
	// The URL of the mock server is our issuer
	oidcConf := &cmn.OIDCConf{AllowedIssuers: []string{oidcS.URL}}
	// Set up the TLS config and get the config for our client
	authConf := &cmn.AuthConf{OIDC: oidcConf}
	client := getKeyCacheClient(t, oidcS.Certificate())
	keyCacheManager := tok.NewKeyCacheManager(authConf.OIDC, client, nil, nil)
	tkParser := tok.NewTokenParser(keyCacheManager, authConf)
	tassert.Fatalf(t, err == nil, "Failed to create token parser: %v", err)
	if init {
		initKeyCacheManager(t, keyCacheManager)
	}
	t.Cleanup(func() {
		jwksS.Close()
		oidcS.Close()
	})

	return testTokenEnv{
		keyID, rsaKey, jwksJSON, jwksS, oidcS, keyCacheManager, tkParser,
	}
}

func initKeyCacheManager(t *testing.T, kcm *tok.KeyCacheManager) {
	kcm.Init(t.Context())
	jwksCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := kcm.PopulateJWKSCache(jwksCtx)
	tassert.Fatalf(t, err == nil, "Failed to populate key manager cache: %v", err)
}

func getKeyCacheClient(t *testing.T, cert *x509.Certificate) *http.Client {
	tls := cmn.TLSArgs{
		SkipVerify: false,
	}
	if cert != nil {
		// Extract the certificate from the test server and encode it as PEM
		caCertPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		})

		// Write to a temp file
		caFile, err := os.CreateTemp(t.TempDir(), "test-ca-*.pem")
		tassert.Fatalf(t, err == nil, "Failed to create temp CA file: %v", err)
		t.Cleanup(func() {
			os.Remove(caFile.Name())
		})

		_, err = caFile.Write(caCertPEM)
		tassert.Fatalf(t, err == nil, "Failed to write CA cert: %v", err)
		caFile.Close()

		tls = cmn.TLSArgs{
			ClientCA:   caFile.Name(), // Path to the CA cert file
			SkipVerify: false,
		}
	}
	transport := cmn.TransportArgs{
		DialTimeout:      1 * time.Second,
		Timeout:          2 * time.Second,
		IdleConnsPerHost: 0,
		MaxIdleConns:     1,
	}
	return cmn.NewClientTLS(transport, tls, false)
}

func TestKeyCacheManager_Population_InvalidDiscovery(t *testing.T) {
	// Test that an invalid discovery URL raises an error when initializing a KeyCacheManager
	for _, url := range invalidIssuerURLs {
		authConf := &cmn.AuthConf{OIDC: &cmn.OIDCConf{AllowedIssuers: []string{url}}}
		kcm := tok.NewKeyCacheManager(authConf.OIDC, getKeyCacheClient(t, nil), nil, nil)
		kcm.Init(t.Context())
		err := kcm.PopulateJWKSCache(t.Context())
		tassert.Errorf(
			t, err != nil,
			"Key Cache Manager population should raise error for invalid discovery URL %s", url,
		)
	}
}

func TestKeyCacheManager_Population_UnresponsiveDiscovery(t *testing.T) {
	// Test that a valid but unresponsive discovery URL does NOT raise an error when initializing a KeyCacheManager
	validURL := "https://missing-host:1234"
	authConf := &cmn.AuthConf{OIDC: &cmn.OIDCConf{AllowedIssuers: []string{validURL}}}
	kcm := tok.NewKeyCacheManager(authConf.OIDC, getKeyCacheClient(t, nil), cacheConfNoRetry, nil)
	kcm.Init(t.Context())
	err := kcm.PopulateJWKSCache(t.Context())
	tassert.Errorf(
		t, err == nil,
		"Key Cache Manager population should not raise error for valid unresponsive discovery URL, got %v", err,
	)
}

func rsaPubKeyPEM(t *testing.T, key *rsa.PrivateKey) string {
	pubBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	tassert.CheckFatal(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes}))
}

func TestKeyCacheManager_ValidateKey(t *testing.T) {
	env := setupTokenTestEnv(t, true)

	t.Run("MatchingKey", func(t *testing.T) {
		pemStr := rsaPubKeyPEM(t, env.rsaKey)
		code, err := env.keyCache.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &pemStr})
		tassert.Errorf(t, err == nil, "Expected matching key to validate, got: %v", err)
		tassert.Errorf(t, code == 0, "Expected status 0, got %d", code)
	})

	t.Run("NonMatchingKey", func(t *testing.T) {
		otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
		tassert.CheckFatal(t, err)
		pemStr := rsaPubKeyPEM(t, otherKey)
		code, err := env.keyCache.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &pemStr})
		tassert.Error(t, err != nil, "Expected non-matching key to be rejected")
		tassert.Errorf(t, code == http.StatusForbidden, "Expected 403, got %d", code)
	})

	t.Run("HMACSecretRejected", func(t *testing.T) {
		code, err := env.keyCache.ValidateKey(t.Context(), &authn.ServerConf{Secret: "some-checksum"})
		tassert.Error(t, err != nil, "Expected HMAC secret to be rejected by OIDC-based provider")
		tassert.Errorf(t, code == http.StatusBadRequest, "Expected 400, got %d", code)
	})

	t.Run("NilPubKey", func(t *testing.T) {
		code, err := env.keyCache.ValidateKey(t.Context(), &authn.ServerConf{})
		tassert.Error(t, err != nil, "Expected nil pubkey to be rejected")
		tassert.Errorf(t, code == http.StatusBadRequest, "Expected 400, got %d", code)
	})

	t.Run("InvalidPEM", func(t *testing.T) {
		bad := "not-a-valid-pem"
		code, err := env.keyCache.ValidateKey(t.Context(), &authn.ServerConf{PubKey: &bad})
		tassert.Error(t, err != nil, "Expected invalid PEM to be rejected")
		tassert.Errorf(t, code == http.StatusBadRequest, "Expected 400, got %d", code)
	})

	t.Run("UninitializedCache", func(t *testing.T) {
		authConf := &cmn.AuthConf{OIDC: &cmn.OIDCConf{AllowedIssuers: []string{"https://example.com"}}}
		kcm := tok.NewKeyCacheManager(authConf.OIDC, nil, nil, nil)
		pemStr := rsaPubKeyPEM(t, env.rsaKey)
		code, err := kcm.ValidateKey(t.Context(), &authn.ServerConf{PubKey: apc.Ptr(pemStr)})
		tassert.Error(t, err != nil, "Expected validation to fail with uninitialized cache")
		tassert.Errorf(t, code == http.StatusInternalServerError, "Expected 500, got %d", code)
	})
}
