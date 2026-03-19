// Package tok_test includes tests for tok pkg
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tok_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/signing"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
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

	hmacSigner = signing.NewHMACSigner(testHMACSigningSecret)
)

func newRegClaims() *jwt.RegisteredClaims {
	return &jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(futureTime),
		Subject:   testUser,
		Audience:  reqAud,
	}
}

func newStandardClaims(bckACL []*authn.BckACL, cluACL []*authn.CluACL) *tok.AISClaims {
	return tok.StandardClaims(newRegClaims(), bckACL, cluACL)
}

func newAdminClaims() *tok.AISClaims {
	return tok.AdminClaims(newRegClaims())
}

func newAdminClaimsWithIssuer(iss string) *tok.AISClaims {
	adminClaims := newAdminClaims()
	adminClaims.Issuer = iss
	return adminClaims
}

func newHMACParser(t *testing.T) tok.Parser {
	hmacConf := &cmn.AuthConf{
		Signature: &cmn.AuthSignatureConf{
			Method: "hmac",
			Key:    testHMACSigningSecret,
		},
		RequiredClaims: &cmn.RequiredClaimsConf{
			Aud: reqAud,
		},
	}
	hmacKeyProvider, err := tok.NewStaticKeyProvider(hmacConf)
	tassert.CheckFatal(t, err)
	return tok.NewTokenParser(hmacKeyProvider, hmacConf)
}

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

func TestAdminClaims(t *testing.T) {
	assertAdminClaims(t, newAdminClaims(), testUser)
}

func TestStandardClaimsBucket(t *testing.T) {
	cluster := "cid1"

	bckACL := makeBckACL(apc.AccessRO, cluster, "b1")

	claims := newStandardClaims([]*authn.BckACL{bckACL}, nil)

	// When comparing against bucket, don't include the cluster id in the namespace
	bck1 := &cmn.Bck{Name: "b1", Provider: "ais"}
	bck2 := &cmn.Bck{Name: "b2", Provider: "ais"}

	assertBucketClaims(t, claims, testUser, cluster, bck1, bck2)
}

func TestStandardClaimsCluster(t *testing.T) {
	cluster := "cid1"

	cluACL := makeCluACL(apc.ClusterAccessRO, cluster)
	claims := newStandardClaims(nil, []*authn.CluACL{cluACL})
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
	tokenStr, err := hmacSigner.SignToken(newAdminClaims())
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	claims, err := newHMACParser(t).ValidateToken(t.Context(), tokenStr)
	tassert.Errorf(t, err == nil, "Expected successful validation but got %v", err)
	assertAdminClaims(t, claims, testUser)
}

// Test validating an expired token with an unsupported signing method
func TestValidateToken_Unsupported(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tassert.Fatalf(t, err == nil, "Failed to generate ecdsa key: %v", err)
	tk := jwt.NewWithClaims(jwt.SigningMethodES256, newAdminClaims())
	tokenStr, err := tk.SignedString(key)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	_, err = newHMACParser(t).ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with invalid signing method to fail")
}

// Test that validating an expired token raises the appropriate exception
func TestValidateToken_Expired(t *testing.T) {
	c := newAdminClaims()
	c.ExpiresAt = jwt.NewNumericDate(previousTime)
	tokenStr, err := hmacSigner.SignToken(c)
	tassert.Fatalf(t, err == nil, "AdminJWT token generation failed: %v", err)
	_, err = newHMACParser(t).ValidateToken(t.Context(), tokenStr)
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
	tokenStr, err := hmacSigner.SignToken(c)
	tassert.Fatalf(t, err == nil, "Token generation failed: %v", err)
	_, err = newHMACParser(t).ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, jwt.ErrTokenInvalidClaims), "Error raised from JWT parse from invalid claims")
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with wrong audience to fail")
}

func TestValidateToken_NoAudience(t *testing.T) {
	c := &tok.AISClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(futureTime),
		},
	}
	tokenStr, err := hmacSigner.SignToken(c)
	tassert.Fatalf(t, err == nil, "Token generation failed: %v", err)
	_, err = newHMACParser(t).ValidateToken(t.Context(), tokenStr)
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
	tokenStr, err := hmacSigner.SignToken(c)
	tassert.Fatalf(t, err == nil, "Token generation failed: %v", err)
	_, err = newHMACParser(t).ValidateToken(t.Context(), tokenStr)
	tassert.Fatal(t, errors.Is(err, tok.ErrInvalidToken), "Expected validating token with missing subject and username to fail")
	tassert.Fatal(t, errors.Is(err, tok.ErrNoSubject), "Expected validating token with missing subject and username to fail")
}

func TestCheckPermissions_Denied(t *testing.T) {
	cluID := "clu"
	bck := &cmn.Bck{Name: "bck", Provider: apc.AIS}
	noPerms := newStandardClaims(nil, nil)

	for perm := apc.AccessAttrs(1); perm < apc.AceMax; perm <<= 1 {
		err := noPerms.CheckPermissions(cluID, bck, perm)
		tassert.Errorf(t, err != nil, "%s should be denied", perm.Describe(false))
	}
}

func TestCheckPermissions_Granted(t *testing.T) {
	cluID := "clu"
	bck := cmn.Bck{Name: "bck", Provider: apc.AIS}
	cluScope := apc.ClusterAccessRW | apc.AceAdmin

	for perm := apc.AccessAttrs(1); perm < apc.AceMax; perm <<= 1 {
		t.Run(perm.Describe(false), func(t *testing.T) {
			var claims *tok.AISClaims
			if perm&cluScope != 0 {
				claims = newStandardClaims(nil,
					[]*authn.CluACL{{ID: cluID, Access: perm}})
			} else {
				claims = newStandardClaims(
					[]*authn.BckACL{{Bck: cmn.Bck{Name: bck.Name, Provider: apc.AIS, Ns: cmn.Ns{UUID: cluID}}, Access: perm}}, nil)
			}
			err := claims.CheckPermissions(cluID, &bck, perm)
			tassert.Errorf(t, err == nil, "%s should be allowed, got: %v", perm.Describe(false), err)
		})
	}
}

func TestValidateToken_IssLookup(t *testing.T) {
	env := setupTokenTestEnv(t, true)
	// Test with claims issued by the allowed issuer
	tk := createTokenWithKeyID(t, newAdminClaimsWithIssuer(env.oidcSrv.URL), env.rsaKey, env.keyID)
	_, err := env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Failed to validate RSA-signed token: %v", err)
}

func TestValidateToken_IssLookupNoInit(t *testing.T) {
	env := setupTokenTestEnv(t, false)

	// Test with claims issued by the allowed issuer
	tk := createTokenWithKeyID(t, newAdminClaimsWithIssuer(env.oidcSrv.URL), env.rsaKey, env.keyID)
	_, err := env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected token validation to fail with uninitialized cache manager")

	// Init and repeat
	env.keyCache.Init(t.Context())
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Expected validation to succeed after initialization without pre-population: %v", err)

	// "Proper" pre-population after dynamic lookup
	jwksCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = env.keyCache.PopulateJWKSCache(jwksCtx)
	tassert.Fatalf(t, err == nil, "Failed to populate key manager cache: %v", err)
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Errorf(t, err == nil, "Expected validation to still succeed after manual pre-population: %v", err)
}

func TestValidateToken_IssLookupNoKey(t *testing.T) {
	env := setupTokenTestEnv(t, true)

	emptyKeyToken := createTokenWithKeyID(t, newAdminClaims(), env.rsaKey, "")
	_, err := env.tkParser.ValidateToken(t.Context(), emptyKeyToken)
	tassert.Errorf(t, err != nil, "Expected validation failure with no key id in token header")

	badKeyToken := createTokenWithKeyID(t, newAdminClaims(), env.rsaKey, "999")
	_, err = env.tkParser.ValidateToken(t.Context(), badKeyToken)
	tassert.Errorf(t, err != nil, "Expected validation failure with no matching key id in token header")
}

func TestValidateToken_IssLookupNotAllowed(t *testing.T) {
	env := setupTokenTestEnv(t, true)
	// Empty allowed issuer fails validation
	authConf := &cmn.AuthConf{OIDC: &cmn.OIDCConf{AllowedIssuers: []string{}}}
	keyCacheManager := tok.NewKeyCacheManager(authConf.OIDC, nil, nil, nil)
	initKeyCacheManager(t, keyCacheManager)
	emptyTkParser := tok.NewTokenParser(keyCacheManager, authConf)
	tk := createTokenWithKeyID(t, newAdminClaimsWithIssuer(env.oidcSrv.URL), env.rsaKey, env.keyID)
	_, err := emptyTkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation failure with no allowed issuers")

	tk = createTokenWithKeyID(t, newAdminClaimsWithIssuer("wrong-issuer"), env.rsaKey, env.keyID)
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation failure with incorrect token issuer")

	tk = createTokenWithKeyID(t, newAdminClaimsWithIssuer(""), env.rsaKey, env.keyID)
	_, err = env.tkParser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Expected validation failure with missing token issuer")
}

func TestValidateToken_IssLookupCertificateValidation(t *testing.T) {
	env := setupTokenTestEnv(t, false)
	oidcConf := &cmn.OIDCConf{AllowedIssuers: []string{env.oidcSrv.URL}}
	authConf := &cmn.AuthConf{OIDC: oidcConf}
	// Add OIDC server WITHOUT adding certificate to trusted certs
	clientNoTrust := getKeyCacheClient(t, nil)
	kcm := tok.NewKeyCacheManager(authConf.OIDC, clientNoTrust, cacheConfNoRetry, nil)
	// No error, but issuer isn't added
	initKeyCacheManager(t, kcm)
	parser := tok.NewTokenParser(kcm, authConf)
	// Token validation fails
	tk := createTokenWithKeyID(t, newAdminClaims(), env.rsaKey, env.keyID)
	_, err := parser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Token validation should fail without certificate trust on issuer")
	// Recreate with client using trusted certificate
	clientWithTrust := getKeyCacheClient(t, env.oidcSrv.Certificate())
	kcmWithTrust := tok.NewKeyCacheManager(authConf.OIDC, clientWithTrust, cacheConfNoRetry, nil)
	parser = tok.NewTokenParser(kcmWithTrust, authConf)
	_, err = parser.ValidateToken(t.Context(), tk)
	tassert.Error(t, err != nil, "Token parser initialization should succeed with issuer certificate trust")
}
