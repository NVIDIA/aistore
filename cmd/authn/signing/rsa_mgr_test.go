// Package signing manages keys the auth service uses for signing and verification
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package signing_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/signing"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
)

const (
	keyBits        = 2048
	tmpKeyFilename = "rsa_test.key"
)

func newRSAConfig(path string) *config.RSAKeyConfig {
	return &config.RSAKeyConfig{
		Filepath: path,
		Size:     keyBits,
	}
}

func defaultTestRSAConfig(t *testing.T) *config.RSAKeyConfig {
	keyPath := newTempKeyFile(t)
	return newRSAConfig(keyPath)
}

func genRandomPassphrase(t *testing.T) cmn.Censored {
	passBytes := make([]byte, 16)
	_, err := rand.Read(passBytes)
	tassert.Fatalf(t, err == nil, "failed to generate passphrase: %v", err)
	return cmn.Censored(hex.EncodeToString(passBytes))
}

func newTempKeyFile(t *testing.T) string {
	return filepath.Join(t.TempDir(), tmpKeyFilename)
}

func assertKeyBundleValid(t *testing.T, mgr *signing.RSAKeyManager) {
	tassert.Fatal(t, mgr.GetPublicKeyPEM() != "", "expected public key PEM to be set")

	jwks, err := mgr.GetJWKS()
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, jwks != nil, "expected JWKS to be set")
	tassert.Fatalf(t, jwks.Len() > 0, "expected JWKS to have at least one key")

	// Sign and validate: proves private key, public key, and kid are all consistent
	dummyClaims := tok.AdminClaims(time.Now().Add(time.Minute), "bundle-check", "")
	signed, err := mgr.SignToken(dummyClaims)
	tassert.CheckFatal(t, err)

	sigConf := mgr.GetSigConf()
	tassert.Fatal(t, sigConf != nil, "expected non-nil sig config from initialized manager")
	parser := tok.NewTokenParser(&cmn.AuthConf{Signature: sigConf}, nil)
	_, err = parser.ValidateToken(t.Context(), signed)
	tassert.CheckFatal(t, err)
}

func compareMgrKeyBundle(t *testing.T, expectedMgr, actualMgr *signing.RSAKeyManager) {
	tassert.Fatal(t, actualMgr.GetPublicKeyPEM() == expectedMgr.GetPublicKeyPEM(), "manager public key does not equal expected")

	expectedJWKS, err := expectedMgr.GetJWKS()
	tassert.CheckFatal(t, err)
	actualJWKS, err := actualMgr.GetJWKS()
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, expectedJWKS.Len() == actualJWKS.Len(), "manager jwks different length than expected, got: %d, wanted: %d", expectedJWKS.Len(), actualJWKS.Len())
	for it := expectedJWKS.Keys(t.Context()); it.Next(t.Context()); {
		pair := it.Pair()
		expectedJWK := pair.Value.(jwk.Key)
		actualJWK, ok := actualJWKS.LookupKeyID(expectedJWK.KeyID())
		tassert.Fatalf(t, ok, "expected key with keyID %q not found in actual set", expectedJWK.KeyID())
		tassert.Fatalf(t, jwk.Equal(expectedJWK, actualJWK), "key with keyID %q differs between expected and actual sets", expectedJWK.KeyID())
	}
	// Cross-validate: token signed by one manager must validate with the other's public key
	dummyClaims := tok.AdminClaims(time.Now().Add(time.Minute), "cross-check", "")
	signed, err := expectedMgr.SignToken(dummyClaims)
	tassert.CheckFatal(t, err)

	sigConf := actualMgr.GetSigConf()
	parser := tok.NewTokenParser(&cmn.AuthConf{Signature: sigConf}, nil)
	_, err = parser.ValidateToken(t.Context(), signed)
	tassert.CheckFatal(t, err)
}

func TestGetSigConf_RSA(t *testing.T) {
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t))
	err := mgr.Init()
	tassert.CheckFatal(t, err)

	sig := mgr.GetSigConf()
	tassert.Fatalf(t, sig.Method == cmn.SigMethodRSA, "expected RSA method, got %v", sig.Method)
	// We don't know pubKey at deploy time, so just make sure it's set and valid
	tassert.Fatal(t, string(sig.Key) != "", "expected non-nil public key")
	block, _ := pem.Decode([]byte(sig.Key))
	tassert.Fatal(t, block != nil, "expected PEM block in public key")
	var pub any
	pub, err = x509.ParsePKIXPublicKey(block.Bytes)
	tassert.Fatalf(t, err == nil, "expected no error parsing RSA public key, got %v", err)
	_, ok := pub.(*rsa.PublicKey)
	tassert.Fatal(t, ok, "expected public key string to be valid RSA public key")
}

func TestIsInitializedFalse(t *testing.T) {
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t))
	tassert.Fatal(t, mgr.GetPublicKeyPEM() == "", "expected public key PEM to be empty")
	jwks, err := mgr.GetJWKS()
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, jwks.Len() == 0, "expected returned JWKS to be initialized but empty")
	sig := mgr.GetSigConf()
	tassert.Fatal(t, sig == nil, "expected nil sig config if not initialized")
	signed, err := mgr.SignToken(tok.AdminClaims(time.Now().Add(time.Minute), "uninitialized", ""))
	tassert.Fatal(t, err != nil, "expected error signing token if not initialized")
	tassert.Fatal(t, signed == "", "expected empty signed token string if not initialized")
}

func TestRSAKeyManagerLoad(t *testing.T) {
	// Write out private key with a previous manager
	conf := defaultTestRSAConfig(t)
	passphrase := genRandomPassphrase(t)
	writer := signing.NewRSAKeyManager(conf, passphrase)
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)

	mgr := signing.NewRSAKeyManager(conf, passphrase)
	err = mgr.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, mgr)

	// Verify all bundle data matches
	compareMgrKeyBundle(t, writer, mgr)
}

func TestSignToken(t *testing.T) {
	futureTime := time.Now().Add(1 * time.Hour)
	testSub := "testUser"
	testAud := "testAudience"
	basicAdminClaims := tok.AdminClaims(futureTime, testSub, testAud)
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t))
	err := mgr.Init()
	tassert.CheckFatal(t, err)

	signed, err := mgr.SignToken(basicAdminClaims)
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, signed != "", "expected non-empty signed token")

	// Validate the signed token using the public key from GetSigConf
	sigConf := mgr.GetSigConf()
	tassert.Fatal(t, sigConf != nil, "expected non-nil sig config")
	parser := tok.NewTokenParser(&cmn.AuthConf{Signature: sigConf}, nil)

	claims, err := parser.ValidateToken(t.Context(), signed)
	tassert.CheckFatal(t, err)

	// Verify claims roundtrip
	sub, err := claims.GetSubject()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, sub == testSub, "expected subject %q, got %q", testSub, sub)
	tassert.Fatal(t, claims.IsAdmin, "expected admin claim to be true")
	gotAud, err := claims.GetAudience()
	tassert.CheckFatal(t, err)
	expectedAud := jwt.ClaimStrings{testAud}
	tassert.Fatalf(t, slices.Equal(gotAud, expectedAud), "expected audience %v, got %v", expectedAud, gotAud)

	// Verify kid header is set and matches a key in JWKS
	rawToken, _, err := jwt.NewParser().ParseUnverified(signed, &tok.AISClaims{})
	tassert.CheckFatal(t, err)
	kid, ok := rawToken.Header["kid"].(string)
	tassert.Fatal(t, ok && kid != "", "expected 'kid' header to be set in signed token")

	jwks, err := mgr.GetJWKS()
	tassert.CheckFatal(t, err)
	_, found := jwks.LookupKeyID(kid)
	tassert.Fatalf(t, found, "expected kid %q from signed token to exist in manager JWKS", kid)
}

// Test the unencrypted key path: create and load with no passphrase
func TestRSAKeyManagerNoPassphrase(t *testing.T) {
	keyPath := newTempKeyFile(t)
	conf := newRSAConfig(keyPath)

	writer := signing.NewRSAKeyManager(conf, "")
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)

	// Load the unencrypted key with a fresh manager
	reader := signing.NewRSAKeyManager(conf, "")
	err = reader.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, reader)

	compareMgrKeyBundle(t, writer, reader)
}

// Loading an unencrypted key must fail when a passphrase is configured
func TestRSAKeyManagerUnencryptedWithPassphrase(t *testing.T) {
	keyPath := newTempKeyFile(t)
	conf := newRSAConfig(keyPath)
	// Write an unencrypted key
	writer := signing.NewRSAKeyManager(conf, "")
	err := writer.Init()
	tassert.CheckFatal(t, err)

	// Try to load with a passphrase — should fail (raw PEM isn't valid encrypted data)
	reader := signing.NewRSAKeyManager(conf, genRandomPassphrase(t))
	err = reader.Init()
	tassert.Fatal(t, err != nil, "expected Init to fail when loading unencrypted key with passphrase configured")
}
