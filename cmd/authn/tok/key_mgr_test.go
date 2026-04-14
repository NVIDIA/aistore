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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

var (
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

func genRSAKey(t *testing.T) *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	tassert.CheckFatal(t, err)
	return key
}

func generateMultiKeyJWKS(t *testing.T, keys map[string]*rsa.PrivateKey) string {
	jwks := jwk.NewSet()
	for kid, key := range keys {
		pubJWK, err := jwk.FromRaw(key.Public())
		tassert.CheckFatal(t, err)
		_ = pubJWK.Set(jwk.KeyIDKey, kid)
		err = jwks.AddKey(pubJWK)
		tassert.CheckFatal(t, err)
	}
	raw, err := json.Marshal(jwks)
	tassert.CheckFatal(t, err)
	return string(raw)
}

func generateTestJWKS(t *testing.T, privKey *rsa.PrivateKey, keyID string) string {
	return generateMultiKeyJWKS(t, map[string]*rsa.PrivateKey{keyID: privKey})
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
	rsaKey := genRSAKey(t)
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
	keyCacheManager := tok.NewKeyCacheManager(kcmConf(authConf.OIDC), client, nil)
	tkParser := tok.NewTokenParser(keyCacheManager, authConf)
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

func kcmConf(oidc *cmn.OIDCConf) *tok.KCMConfig {
	if oidc == nil {
		return nil
	}
	return &tok.KCMConfig{OIDCConf: *oidc}
}

func kcmConfNoRetry(oidc *cmn.OIDCConf) *tok.KCMConfig {
	conf := kcmConf(oidc)
	if conf != nil {
		conf.Discovery = &tok.DiscoveryConf{Retries: apc.Ptr(0), BaseDelay: apc.Ptr(time.Duration(0))}
	}
	return conf
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
		kcm := tok.NewKeyCacheManager(kcmConf(authConf.OIDC), getKeyCacheClient(t, nil), nil)
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
	kcm := tok.NewKeyCacheManager(kcmConfNoRetry(authConf.OIDC), getKeyCacheClient(t, nil), nil)
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
		pemStr := rsaPubKeyPEM(t, genRSAKey(t))
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
		kcm := tok.NewKeyCacheManager(kcmConf(authConf.OIDC), nil, nil)
		pemStr := rsaPubKeyPEM(t, env.rsaKey)
		code, err := kcm.ValidateKey(t.Context(), &authn.ServerConf{PubKey: apc.Ptr(pemStr)})
		tassert.Error(t, err != nil, "Expected validation to fail with uninitialized cache")
		tassert.Errorf(t, code == http.StatusInternalServerError, "Expected 500, got %d", code)
	})
}

//
// Dynamic JWKS server + key rotation / refresh tests
//

type dynamicJWKSHandler struct {
	mu       sync.RWMutex
	jwksJSON string
	reqCount atomic.Int64
}

func (h *dynamicJWKSHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.reqCount.Add(1)
	h.mu.RLock()
	defer h.mu.RUnlock()
	fmt.Fprint(w, h.jwksJSON)
}

type dynamicTokenEnv struct {
	keys        map[string]*rsa.PrivateKey
	jwksHandler *dynamicJWKSHandler
	oidcSrv     *httptest.Server
	tkParser    *tok.TokenParser
}

// addKey generates a new RSA key, adds it to the JWKS served by the mock server
func (env *dynamicTokenEnv) addKey(t *testing.T, kid string) {
	key := genRSAKey(t)
	env.keys[kid] = key
	env.jwksHandler.mu.Lock()
	env.jwksHandler.jwksJSON = generateMultiKeyJWKS(t, env.keys)
	env.jwksHandler.mu.Unlock()
}

func (env *dynamicTokenEnv) signToken(t *testing.T, kid string) string {
	key, ok := env.keys[kid]
	tassert.Fatalf(t, ok, "no key registered for kid %q", kid)
	return createTokenWithKeyID(t, newAdminClaimsWithIssuer(env.oidcSrv.URL), key, kid)
}

func setupDynamicTokenEnv(t *testing.T, rotationRefresh time.Duration) *dynamicTokenEnv {
	initKey := genRSAKey(t)
	keys := map[string]*rsa.PrivateKey{"key-1": initKey}

	handler := &dynamicJWKSHandler{
		jwksJSON: generateMultiKeyJWKS(t, keys),
	}
	jwksSrv := httptest.NewTLSServer(handler)
	oidcSrv := createMockOIDCServer(jwksSrv.URL)

	oidcConf := &cmn.OIDCConf{AllowedIssuers: []string{oidcSrv.URL}}
	if rotationRefresh > 0 {
		oidcConf.JWKSCacheConf = &cmn.JWKSCacheConf{
			MinRotationRefresh: cos.Duration(rotationRefresh),
		}
	}
	authConf := &cmn.AuthConf{OIDC: oidcConf}
	client := getKeyCacheClient(t, oidcSrv.Certificate())
	kcm := tok.NewKeyCacheManager(kcmConf(authConf.OIDC), client, nil)
	initKeyCacheManager(t, kcm)

	t.Cleanup(func() {
		jwksSrv.Close()
		oidcSrv.Close()
	})

	return &dynamicTokenEnv{
		keys:        keys,
		jwksHandler: handler,
		oidcSrv:     oidcSrv,
		tkParser:    tok.NewTokenParser(kcm, authConf),
	}
}

func TestKeyCacheManager_KeyRotation(t *testing.T) {
	env := setupDynamicTokenEnv(t, 0)

	// Original key validates from cache
	_, err := env.tkParser.ValidateToken(t.Context(), env.signToken(t, "key-1"))
	tassert.Errorf(t, err == nil, "original key: %v", err)

	// Rotate: add key-2, keeping key-1
	env.addKey(t, "key-2")

	// New key triggers cache miss → refresh → success
	_, err = env.tkParser.ValidateToken(t.Context(), env.signToken(t, "key-2"))
	tassert.Errorf(t, err == nil, "rotated key: %v", err)

	// Original key still works
	_, err = env.tkParser.ValidateToken(t.Context(), env.signToken(t, "key-1"))
	tassert.Errorf(t, err == nil, "original key after rotation: %v", err)
}

func TestKeyCacheManager_UnknownKidAfterRefresh(t *testing.T) {
	env := setupDynamicTokenEnv(t, 0)

	// Sign with a key the JWKS server doesn't know about (not added via addKey)
	unknownKey := genRSAKey(t)
	tk := createTokenWithKeyID(t, newAdminClaimsWithIssuer(env.oidcSrv.URL), unknownKey, "unknown-kid")
	_, err := env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation to fail for unknown kid even after refresh")
}

func TestKeyCacheManager_RefreshThrottle_CacheFallback(t *testing.T) {
	env := setupDynamicTokenEnv(t, 10*time.Minute)

	// Add key-2 and key-3 to the JWKS server
	env.addKey(t, "key-2")
	env.addKey(t, "key-3")

	// key-2: triggers refresh, cache now has all three keys
	_, err := env.tkParser.ValidateToken(t.Context(), env.signToken(t, "key-2"))
	tassert.Errorf(t, err == nil, "key-2 after refresh: %v", err)

	// key-3: cache.Get already returns the refreshed set, so it's found without a new refresh
	_, err = env.tkParser.ValidateToken(t.Context(), env.signToken(t, "key-3"))
	tassert.Errorf(t, err == nil, "key-3 from updated cache: %v", err)
}

func TestKeyCacheManager_ConcurrentRefresh(t *testing.T) {
	env := setupDynamicTokenEnv(t, 10*time.Minute)
	env.addKey(t, "key-2")

	countBefore := env.jwksHandler.reqCount.Load()
	tk := env.signToken(t, "key-2")

	const numRoutines = 20
	var wg sync.WaitGroup
	errs := make([]error, numRoutines)
	for i := range numRoutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = env.tkParser.ValidateToken(t.Context(), tk)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		tassert.Errorf(t, e == nil, "goroutine %d: %v", i, e)
	}

	refreshCount := env.jwksHandler.reqCount.Load() - countBefore
	tassert.Errorf(t, refreshCount == 1, "Expected exactly 1 JWKS fetch, got %d", refreshCount)
}
