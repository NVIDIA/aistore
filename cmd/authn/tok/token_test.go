// Package tok_test includes tests for tok pkg
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package tok_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
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

const (
	testUser              = "testUserName"
	testAudience          = "testAudience"
	testHMACSigningSecret = "tokenSigningValue"
)

var (
	previousTime = time.Now().Add(-time.Second)
	futureTime   = time.Now().Add(1 * time.Hour)
	reqAud       = []string{testAudience}
	reqClaims    = &cmn.RequiredClaimsConf{
		Aud: reqAud,
	}
	hmacSigConfig = &cmn.AuthSignatureConf{
		Key:    testHMACSigningSecret,
		Method: cmn.SigMethodHMAC,
	}
	hmacParser       = tok.NewTokenParser(&cmn.AuthConf{RequiredClaims: reqClaims, Signature: hmacSigConfig}, nil, nil)
	basicAdminClaims = tok.AdminClaims(futureTime, testUser, testAudience)
	cacheConfNoRetry = &tok.CacheConfig{
		DiscoveryConf: &tok.DiscoveryConf{
			Retries:   0,
			BaseDelay: time.Duration(0),
		},
		MinCacheRefreshInterval: nil,
	}
	// Invalid for both disovery and JWKS
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

// Helper for dummy permission structs
func makeBckACL(access apc.AccessAttrs, clusterID, name string) *authn.BckACL {
	return &authn.BckACL{
		Bck:    cmn.Bck{Name: name, Provider: "ais", Ns: cmn.Ns{UUID: clusterID}},
		Access: access,
	}
}

func makeCluACL(access apc.AccessAttrs, clusterID string) *authn.CluACL {
	return &authn.CluACL{
		ID:     clusterID,
		Access: access,
	}
}

// Common assertions for specific token types

func assertCommon(t *testing.T, c *tok.AISClaims, user string, admin bool) {
	if admin {
		tassert.Error(t, c.IsAdmin, "Expected IsAdmin to be true")
	} else {
		tassert.Error(t, !c.IsAdmin, "Expected IsAdmin to be false")
	}
	sub, err := c.GetSubject()
	tassert.Error(t, err == nil, "Expected token GetSubject to succeed")
	tassert.Errorf(t, sub == user, "Expected username %q, got %q", user, sub)
}

func assertAdminClaims(t *testing.T, c *tok.AISClaims, user string) {
	assertCommon(t, c, user, true)
	// Bucket and cluster don't matter if token has full admin access
	err := c.CheckPermissions("any", &cmn.Bck{Name: "b1", Provider: "ais"}, apc.ClusterAccessRW&apc.AccessRW)
	tassert.Errorf(t, err == nil, "Failed to get full cluster RW access, error: %v", err)
}

func assertBucketClaims(t *testing.T, c *tok.AISClaims, user, cid string, bck1, bck2 *cmn.Bck) {
	assertCommon(t, c, user, false)
	err := c.CheckPermissions(cid, bck1, apc.AccessRO)
	tassert.Errorf(t, err == nil, "Expected RO access on %s, got:: %v", bck1.Name, err)
	err = c.CheckPermissions(cid, bck1, apc.AccessRW)
	tassert.Errorf(t, err != nil, "Expected validation error requesting RW access on %s", bck1.Name)
	err = c.CheckPermissions("another-cluster", bck1, apc.AccessRO)
	tassert.Error(t, err != nil, "Expected validation error when accessing unauthorized cluster")
	err = c.CheckPermissions(cid, bck2, apc.AccessRO)
	tassert.Errorf(t, err != nil, "Expected all access to %s to be denied", bck2.Name)
}

func assertClusterClaims(t *testing.T, c *tok.AISClaims, user, cid string) {
	assertCommon(t, c, user, false)
	err := c.CheckPermissions(cid, nil, apc.ClusterAccessRO)
	tassert.Errorf(t, err == nil, "Expected RO access on %s, got: %v", cid, err)
	err = c.CheckPermissions(cid, nil, apc.ClusterAccessRW)
	tassert.Errorf(t, err != nil, "Expected validation error when requesting RW access on %s", cid)
	err = c.CheckPermissions("another-cluster", nil, apc.ClusterAccessRO)
	tassert.Error(t, err != nil, "Expected validation error when requesting RO access on unauthorized cluster")
}

// Other helper functions

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

func TestCreateHMACTokenStr(t *testing.T) {
	tk, err := tok.CreateHMACTokenStr(basicAdminClaims, testHMACSigningSecret)
	tassert.Errorf(t, err == nil, "Failed to create hmac-signed token string: %v", err)
	_, err = hmacParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Failed to validate hmac-siagned token: %v", err)
}

func TestCreateRSATokenStr(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	tassert.Errorf(t, err == nil, "Failed to create rsa signing key: %v", err)
	derBytes, err := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	tassert.Errorf(t, err == nil, "Failed to marshal rsa public key: %v", err)
	sigConf := &cmn.AuthSignatureConf{Key: base64.StdEncoding.EncodeToString(derBytes), Method: cmn.SigMethodRSA}
	tkParser := tok.NewTokenParser(&cmn.AuthConf{Signature: sigConf}, nil, nil)
	tk, err := tok.CreateRSATokenStr(basicAdminClaims, rsaKey)
	tassert.Errorf(t, err == nil, "Failed to create RSA-signed token string: %v", err)
	_, err = tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Failed to validate RSA-signed token: %v", err)
}

func TestAdminClaims(t *testing.T) {
	assertAdminClaims(t, basicAdminClaims, testUser)
}

func TestStandardClaimsBucket(t *testing.T) {
	cluster := "cid1"

	bckACL := makeBckACL(apc.AccessRO, cluster, "b1")

	claims := tok.StandardClaims(futureTime, testUser, testAudience, []*authn.BckACL{bckACL}, nil)

	// When comparing against bucket, don't include the cluster id in the namespace
	bck1 := &cmn.Bck{Name: "b1", Provider: "ais"}
	bck2 := &cmn.Bck{Name: "b2", Provider: "ais"}

	assertBucketClaims(t, claims, testUser, cluster, bck1, bck2)
}

func TestStandardClaimsCluster(t *testing.T) {
	cluster := "cid1"

	cluACL := makeCluACL(apc.ClusterAccessRO, cluster)

	claims := tok.StandardClaims(futureTime, testUser, testAudience, nil, []*authn.CluACL{cluACL})
	assertClusterClaims(t, claims, testUser, cluster)
}

func TestExtractToken(t *testing.T) {
	// Test bearer token extraction (s3CompatEnabled=false)
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer sometoken")
	token, err := tok.ExtractToken(hdr)
	tassert.Fatalf(t, err == nil, "ExtractToken failed: %v", err)
	tassert.Errorf(t, token.Token == "sometoken", "Expected 'sometoken', got %q", token.Token)
	tassert.Errorf(t, token.Header == "Authorization", "Expected 'Authorization', got %q", token.Header)

	// Test missing Bearer prefix
	hdr.Set("Authorization", "sometoken")
	_, err = tok.ExtractToken(hdr)
	tassert.Error(t, err != nil, "Expected failure due to missing 'Bearer' prefix")
}

func TestExtractTokenS3Compat(t *testing.T) {
	// Create a dummy JWT
	testJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0dXNlciIsImV4cCI6OTk5OTk5OTk5OX0.dummy"

	// Test 1: JWT in X-Amz-Security-Token (with SigV4 headers present)
	hdr := http.Header{}
	hdr.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request...")
	hdr.Set("X-Amz-Security-Token", testJWT)
	hdr.Set("X-Amz-Date", "20130524T000000Z")

	token, err := tok.ExtractToken(hdr)
	tassert.Fatalf(t, err == nil, "ExtractToken failed: %v", err)
	tassert.Errorf(t, token.Token == testJWT, "Expected test JWT, got %q", token.Token)
	tassert.Errorf(t, token.Header == "X-Amz-Security-Token", "Expected JWT from X-Amz-Security-Token, got %q", token.Header)

	// Test 2: Bearer token takes precedence
	hdr.Set("Authorization", "Bearer priority-token")
	token, err = tok.ExtractToken(hdr)
	tassert.Fatalf(t, err == nil, "ExtractToken failed: %v", err)
	tassert.Errorf(t, token.Token == "priority-token", "Expected Bearer token to take precedence, got %q", token)

	// Test 3: X-Amz-Security-Token with non-JWT data extracts successfully
	// (validation happens later in ValidateToken, not during extraction)
	hdr = http.Header{}
	hdr.Set("Authorization", "AWS4-HMAC-SHA256 Credential=...")
	hdr.Set("X-Amz-Security-Token", "not-a-jwt-no-dots")
	token, err = tok.ExtractToken(hdr)
	tassert.Fatalf(t, err == nil, "ExtractToken should succeed, got: %v", err)
	tassert.Errorf(t, token.Token == "not-a-jwt-no-dots", "Expected extracted token, got %q", token)

	// Test 4: No token at all
	hdr = http.Header{}
	hdr.Set("Authorization", "AWS4-HMAC-SHA256 Credential=...")
	_, err = tok.ExtractToken(hdr)
	tassert.Fatalf(t, errors.Is(err, tok.ErrNoToken), "Expected ErrNoToken, got: %v", err)
}

func TestClaims_IsExpired(t *testing.T) {
	c := &tok.AISClaims{Expires: previousTime}
	tassert.Error(t, c.IsExpired(), "AISClaims should be expired")
	c = &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(previousTime),
		},
	}
	tassert.Error(t, c.IsExpired(), "AISClaims should be expired")
	c = &tok.AISClaims{Expires: futureTime}
	tassert.Error(t, !c.IsExpired(), "AISClaims should not be expired")
	c = &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(futureTime),
		},
	}
	tassert.Error(t, !c.IsExpired(), "AISClaims should not be expired")
}

func TestClaims_IsUser(t *testing.T) {
	c := &tok.AISClaims{UserID: testUser}
	tassert.Error(t, c.IsUser(testUser), "Claims should equal user")
	tassert.Error(t, !c.IsUser("someOtherUser"), "Claims should not equal user")
	c = &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: testUser,
		},
	}
	tassert.Error(t, c.IsUser(testUser), "Claims should equal user")
	tassert.Error(t, !c.IsUser("someOtherUser"), "Claims should not equal user")
}

// Create claims containing both old and new fields and validate the standard fields take precedence
func TestClaims_BackwardsCompat(t *testing.T) {
	expectedExp := jwt.NewNumericDate(time.Now().Add(30 * time.Minute))
	expectedSub := "sub claim"
	oldExp := jwt.NewNumericDate(time.Now().Add(2 * time.Hour))
	userID := "username field"
	combinedClaims := &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: expectedExp,
			Subject:   expectedSub,
		},
		UserID:  userID,
		Expires: oldExp.UTC(),
	}
	sub, err := combinedClaims.GetSubject()
	tassert.Errorf(t, err == nil, "Error getting subject from token: %v", err)
	tassert.Error(t, expectedSub == sub, "Expected token subject to be equal to the OIDC sub claim")
	exp, err := combinedClaims.GetExpirationTime()
	tassert.Errorf(t, err == nil, "Error getting expiration time from token: %v", err)
	tassert.Error(t, expectedExp.UTC().Equal(exp.UTC()), "Expected token expiration time to be equal to the OIDC exp claim")
	// The old fields are still set, just not used by the overridden Claims methods
	tassert.Error(t, oldExp.UTC().Equal(combinedClaims.Expires), "Expected token to contain old expiry field")
	tassert.Error(t, userID == combinedClaims.UserID, "Expected user id to match old userID field")
}

// Test validating a token successfully
func TestValidateToken_Success(t *testing.T) {
	tokenStr, err := tok.CreateHMACTokenStr(basicAdminClaims, testHMACSigningSecret)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	claims, err := hmacParser.ValidateToken(t.Context(), tokenStr)
	tassert.Errorf(t, err == nil, "Expected successful validation but got %v", err)
	assertAdminClaims(t, claims, testUser)
}

// Test validating token with invalid claims
func TestValidateToken_InvalidKey(t *testing.T) {
	tokenStr, err := tok.CreateHMACTokenStr(basicAdminClaims, testHMACSigningSecret)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	invalidSig := &cmn.AuthSignatureConf{Key: "invalid-secret", Method: cmn.SigMethodHMAC}
	invalidParser := tok.NewTokenParser(&cmn.AuthConf{Signature: invalidSig}, nil, nil)
	_, err = invalidParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with wrong key to fail")
}

// Test validating an expired token with an unsupported signing method
func TestValidateToken_Unsupported(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tassert.Fatalf(t, err == nil, "Failed to generate ecdsa key: %v", err)
	tk := jwt.NewWithClaims(jwt.SigningMethodES256, basicAdminClaims)
	tokenStr, err := tk.SignedString(key)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	_, err = hmacParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with invalid signing method to fail")
}

// Test that validating an expired token raises the appropriate exception
func TestValidateToken_Expired(t *testing.T) {
	c := tok.AdminClaims(previousTime, testUser, testAudience)
	tokenStr, err := tok.CreateHMACTokenStr(c, testHMACSigningSecret)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	_, err = hmacParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrTokenExpired), "Expected an error when token is expired")
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "An expired token is invalid")
}

func TestValidateToken_AudienceMismatch(t *testing.T) {
	c := &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(futureTime),
			Audience:  []string{"wrong-audience"},
			Subject:   "subject1",
		},
	}
	tokenStr, err := tok.CreateHMACTokenStr(c, testHMACSigningSecret)
	tassert.Fatalf(t, err == nil, "Token generation failed: %v", err)
	_, err = hmacParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, jwt.ErrTokenInvalidClaims), "Error raised from JWT parse from invalid claims")
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with wrong audience to fail")
}

func TestValidateToken_NoAudience(t *testing.T) {
	c := &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(futureTime),
		},
	}
	tokenStr, err := tok.CreateHMACTokenStr(c, testHMACSigningSecret)
	tassert.Fatalf(t, err == nil, "Token generation failed: %v", err)
	_, err = hmacParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, jwt.ErrTokenInvalidClaims), "Error raised from JWT parse from invalid claims")
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with missing audience to fail")
}

func TestValidateToken_NoSubject(t *testing.T) {
	c := &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(futureTime),
			Audience:  reqAud,
		},
	}
	tokenStr, err := tok.CreateHMACTokenStr(c, testHMACSigningSecret)
	tassert.Fatalf(t, err == nil, "Token generation failed: %v", err)
	_, err = hmacParser.ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with missing subject and username to fail")
	tassert.Fatal(t, errors.Is(err, tok.ErrNoSubject), "Expected validating token with missing subject and username to fail")
}

// For testing tokens with public key lookup by token issuer

type testTokenEnv struct {
	keyID    string
	rsaKey   *rsa.PrivateKey
	jwksJSON string
	jwksSrv  *httptest.Server
	oidcSrv  *httptest.Server
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
	clientConf := getClientConf(t, oidcS.Certificate())
	tkParser := tok.NewTokenParser(&cmn.AuthConf{OIDC: oidcConf}, clientConf, nil)
	tassert.Fatalf(t, err == nil, "Failed to create token parser: %v", err)
	if init {
		err = tkParser.InitKeyCache(t.Context())
		tassert.Fatalf(t, err == nil, "Failed to initialize token parser key cache: %v", err)
	}
	t.Cleanup(func() {
		jwksS.Close()
		oidcS.Close()
	})

	return testTokenEnv{
		keyID, rsaKey, jwksJSON, jwksS, oidcS, tkParser,
	}
}

func getClientConf(t *testing.T, cert *x509.Certificate) *tok.JWKSClientConf {
	// Extract the certificate from the test server and encode it as PEM
	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})

	// Write to a temp file if your ClientCA expects a file path
	caFile, err := os.CreateTemp(t.TempDir(), "test-ca-*.pem")
	tassert.Fatalf(t, err == nil, "Failed to create temp CA file: %v", err)
	t.Cleanup(func() {
		os.Remove(caFile.Name())
	})

	_, err = caFile.Write(caCertPEM)
	tassert.Fatalf(t, err == nil, "Failed to write CA cert: %v", err)
	caFile.Close()

	// The URL of the mock server is our issuer
	return &tok.JWKSClientConf{
		TLSArgs: &cmn.TLSArgs{
			ClientCA:   caFile.Name(), // Path to the CA cert file
			SkipVerify: false,
		},
	}
}

func TestValidateToken_IssLookup(t *testing.T) {
	env := setupTokenTestEnv(t, true)
	// Test with claims issued by the allowed issuer
	basicAdminClaims.Issuer = env.oidcSrv.URL
	tk := createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, env.keyID)
	_, err := env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Failed to validate RSA-signed token: %v", err)
}

func TestValidateToken_IssLookupNoInit(t *testing.T) {
	env := setupTokenTestEnv(t, false)

	// Test with claims issued by the allowed issuer
	basicAdminClaims.Issuer = env.oidcSrv.URL
	tk := createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, env.keyID)
	_, err := env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err != nil, "Expected token validation to fail with uninitialized cache")

	// Init and repeat
	err = env.tkParser.InitKeyCache(t.Context())
	tassert.Errorf(t, err == nil, "Failed to initialize token key cache: %v", err)
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Failed to validate RSA-signed token: %v", err)
}

func TestValidateToken_IssLookupNoKey(t *testing.T) {
	env := setupTokenTestEnv(t, true)

	emptyKeyToken := createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, "")
	_, err := env.tkParser.ValidateToken(t.Context(), emptyKeyToken)
	tassert.Errorf(t, err != nil, "Expected validation failure with no key id in token header")

	badKeyToken := createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, "999")
	_, err = env.tkParser.ValidateToken(t.Context(), badKeyToken)
	tassert.Errorf(t, err != nil, "Expected validation failure with no matching key id in token header")
}

func TestValidateToken_IssLookupNotAllowed(t *testing.T) {
	env := setupTokenTestEnv(t, true)
	oidcConf := &cmn.OIDCConf{
		// Empty allowed issuer fails validation
		AllowedIssuers: []string{},
	}
	emptyTkParser := tok.NewTokenParser(&cmn.AuthConf{OIDC: oidcConf}, nil, nil)
	err := emptyTkParser.InitKeyCache(t.Context())
	tassert.Fatalf(t, err == nil, "Failed to init key cache for token parser: %v", err)
	basicAdminClaims.Issuer = env.oidcSrv.URL
	tk := createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, env.keyID)
	_, err = emptyTkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation failure with no allowed issuers")

	basicAdminClaims.Issuer = "wrong-issuer"
	tk = createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, env.keyID)
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation failure with incorrect token issuer")

	basicAdminClaims.Issuer = ""
	tk = createTokenWithKeyID(t, basicAdminClaims, env.rsaKey, env.keyID)
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation failure with missing token issuer")
}

func TestNewTokenParser_InvalidDiscovery(t *testing.T) {
	// Test that an invalid discovery URL raises an error when calling InitKeyCache
	for _, url := range invalidIssuerURLs {
		oidcConf := &cmn.OIDCConf{
			AllowedIssuers: []string{url},
		}
		parser := tok.NewTokenParser(&cmn.AuthConf{OIDC: oidcConf}, nil, nil)
		err := parser.InitKeyCache(t.Context())
		tassert.Errorf(
			t, err != nil,
			"Token parser initialization should fail for invalid discovery URL: %q", url,
		)
	}
}

func TestNewTokenParser_PartiallyValidDiscovery(t *testing.T) {
	// Test that including both a valid and invalid discovery URL does NOT raise an error
	// Don't initialize token parser -- we'll use our own here with specific issuers
	env := setupTokenTestEnv(t, false)
	oidcConf := &cmn.OIDCConf{
		AllowedIssuers: []string{invalidIssuerURLs[0], env.oidcSrv.URL},
	}
	parser := tok.NewTokenParser(&cmn.AuthConf{OIDC: oidcConf}, getClientConf(t, env.oidcSrv.Certificate()), nil)
	err := parser.InitKeyCache(t.Context())
	tassert.Errorf(
		t, err == nil,
		"Token parser initialization should NOT fail with both a valid and invalid discovery URL: %v, got: %v", oidcConf.AllowedIssuers, err,
	)
}

func TestNewTokenParser_CertificateValidation(t *testing.T) {
	env := setupTokenTestEnv(t, false)
	oidcConf := &cmn.OIDCConf{AllowedIssuers: []string{env.oidcSrv.URL}}
	// No adding of oidc server certificate to trusted certs
	parser := tok.NewTokenParser(&cmn.AuthConf{OIDC: oidcConf}, nil, cacheConfNoRetry)
	err := parser.InitKeyCache(t.Context())
	tassert.Error(t, err != nil, "Token parser initialization should fail without certificate trust")
}

func TestNewTokenParser_InvalidJWKSURL(t *testing.T) {
	for _, badJwks := range invalidIssuerURLs {
		// Create a discovery server that returns invalid JWKS urls
		oidcSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/.well-known/openid-configuration" {
				resp := map[string]string{
					"jwks_uri": badJwks,
				}
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			http.NotFound(w, r)
		}))
		t.Cleanup(oidcSrv.Close)
		oidcConf := &cmn.OIDCConf{
			AllowedIssuers: []string{oidcSrv.URL},
		}
		clientConf := &tok.JWKSClientConf{
			TLSArgs: &cmn.TLSArgs{
				SkipVerify: true, // allow self-signed test certs
			},
		}
		parser := tok.NewTokenParser(&cmn.AuthConf{OIDC: oidcConf}, clientConf, nil)
		err := parser.InitKeyCache(t.Context())
		tassert.Errorf(
			t, err != nil,
			"Token parser initialization should fail for invalid JWKS URL: %q", badJwks,
		)
	}
}
