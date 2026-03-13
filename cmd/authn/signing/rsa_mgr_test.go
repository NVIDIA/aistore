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
	"encoding/json"
	"encoding/pem"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/kvdb/mock"
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

func getAndAssertPubKey(t *testing.T, mgr *signing.RSAKeyManager) string {
	validationConf := mgr.ValidationConf()
	tassert.Fatal(t, validationConf != nil, "expected non-nil validation conf")
	tassert.Fatal(t, validationConf.PubKey != nil, "expected public key to be non-nil in validation conf")
	assertValidPublicKey(t, *validationConf.PubKey)
	return *validationConf.PubKey
}

func assertValidPublicKey(t *testing.T, pubKeyPEM string) {
	block, _ := pem.Decode([]byte(pubKeyPEM))
	tassert.Fatal(t, block != nil, "expected valid PEM block in public key")
	pub, parseErr := x509.ParsePKIXPublicKey(block.Bytes)
	tassert.Fatalf(t, parseErr == nil, "expected parseable public key, got %v", parseErr)
	_, ok := pub.(*rsa.PublicKey)
	tassert.Fatal(t, ok, "expected RSA public key type")
}

func assertKeyBundleValid(t *testing.T, mgr *signing.RSAKeyManager) {
	getAndAssertPubKey(t, mgr)

	jwks, err := mgr.GetJWKS()
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, jwks != nil, "expected JWKS to be set")
	tassert.Fatalf(t, jwks.Len() > 0, "expected JWKS to have at least one key")

	// Sign and validate: proves private key, public key, and kid are all consistent
	dummyClaims := tok.AdminClaims(time.Now().Add(time.Minute), "bundle-check", "")
	signed, err := mgr.SignToken(dummyClaims)
	tassert.CheckFatal(t, err)

	parser := tok.NewTokenParser(mgr, nil)
	_, err = parser.ValidateToken(t.Context(), signed)
	tassert.CheckFatal(t, err)
}

func compareMgrKeyBundle(t *testing.T, expectedMgr, actualMgr *signing.RSAKeyManager) {
	actualPub := getAndAssertPubKey(t, actualMgr)
	expectedPub := getAndAssertPubKey(t, expectedMgr)
	tassert.Fatal(t, actualPub == expectedPub, "manager public key does not equal expected")

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
	// Cross-validate: token signed by one manager must validate with the other's JWKS
	dummyClaims := tok.AdminClaims(time.Now().Add(time.Minute), "cross-check", "")
	signed, err := expectedMgr.SignToken(dummyClaims)
	tassert.CheckFatal(t, err)

	parser := tok.NewTokenParser(actualMgr, nil)
	_, err = parser.ValidateToken(t.Context(), signed)
	tassert.CheckFatal(t, err)
}

func TestInitNoDB(t *testing.T) {
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t), nil)
	err := mgr.Init()
	tassert.Fatalf(t, err != nil, "expected Init to fail with no DB provided")
}

func TestRSAKeyManagerLoad(t *testing.T) {
	// Write out private key with a previous manager; share JWKS storage so load can read what create persisted
	conf := defaultTestRSAConfig(t)
	passphrase := genRandomPassphrase(t)
	db := &mock.KeyDataStorage{}
	writer := signing.NewRSAKeyManager(conf, passphrase, db)
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)

	mgr := signing.NewRSAKeyManager(conf, passphrase, db)
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
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t), &mock.KeyDataStorage{})
	err := mgr.Init()
	tassert.CheckFatal(t, err)

	signed, err := mgr.SignToken(basicAdminClaims)
	tassert.CheckFatal(t, err)
	tassert.Fatal(t, signed != "", "expected non-empty signed token")

	parser := tok.NewTokenParser(mgr, nil)

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
	jwksStore := &mock.KeyDataStorage{}

	writer := signing.NewRSAKeyManager(conf, "", jwksStore)
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)

	// Load the unencrypted key with a fresh manager
	reader := signing.NewRSAKeyManager(conf, "", jwksStore)
	err = reader.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, reader)

	compareMgrKeyBundle(t, writer, reader)
}

// Loading an unencrypted key must fail when a passphrase is configured
func TestRSAKeyManagerUnencryptedWithPassphrase(t *testing.T) {
	keyPath := newTempKeyFile(t)
	conf := newRSAConfig(keyPath)
	db := &mock.KeyDataStorage{}
	// Write an unencrypted key
	writer := signing.NewRSAKeyManager(conf, "", db)
	err := writer.Init()
	tassert.CheckFatal(t, err)

	// Try to load with a passphrase — should fail (raw PEM isn't valid encrypted data)
	reader := signing.NewRSAKeyManager(conf, genRandomPassphrase(t), db)
	err = reader.Init()
	tassert.Fatal(t, err != nil, "expected Init to fail when loading unencrypted key with passphrase configured")
}

func TestValidationConf_RSA(t *testing.T) {
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t), &mock.KeyDataStorage{})
	err := mgr.Init()
	tassert.CheckFatal(t, err)

	conf := mgr.ValidationConf()

	// ValidationConf must populate PubKey, not Secret
	tassert.Fatal(t, conf.PubKey != nil, "expected non-nil PubKey pointer")
	tassert.Fatal(t, *conf.PubKey != "", "expected non-empty public key PEM")
	tassert.Fatal(t, conf.Secret == "", "expected empty Secret field for RSA signer")

	// Public key must be a valid PEM-encoded RSA public key
	assertValidPublicKey(t, *conf.PubKey)
}

func TestKeyWithNoMetadata(t *testing.T) {
	// Write out private key with a previous manager
	conf := defaultTestRSAConfig(t)
	passphrase := genRandomPassphrase(t)
	db := &mock.KeyDataStorage{}
	writer := signing.NewRSAKeyManager(conf, passphrase, db)
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)

	// New manager gets a new JWKS storage mock, so we load key but no existing metadata
	mgr := signing.NewRSAKeyManager(conf, passphrase, &mock.KeyDataStorage{})
	err = mgr.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, mgr)

	// Verify all bundle data matches
	compareMgrKeyBundle(t, writer, mgr)
}

func TestKeyWithInvalidMetadata(t *testing.T) {
	// Write out private key with a previous manager
	conf := defaultTestRSAConfig(t)
	passphrase := genRandomPassphrase(t)
	db := &mock.KeyDataStorage{}
	writer := signing.NewRSAKeyManager(conf, passphrase, db)
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)

	// Metadata will fail marshaling with invalid json error, so will fail loading
	storageDriver := mock.NewKeyDataStorage(make(json.RawMessage, 10), nil)
	// New manager loads key but fails metadata loading -- init must fail
	mgr := signing.NewRSAKeyManager(conf, passphrase, storageDriver)
	err = mgr.Init()
	tassert.Fatalf(t, err != nil, "expected Init to fail when loading invalid metadata")
}

// TestPersistKeyDataFailsInLegacyPath: key exists, no key data
// PersistKeyData error in legacy path causes init failure
func TestPersistKeyDataFailsInLegacyPath(t *testing.T) {
	conf := defaultTestRSAConfig(t)
	passphrase := genRandomPassphrase(t)
	writerStore := &mock.KeyDataStorage{}
	writer := signing.NewRSAKeyManager(conf, passphrase, writerStore)
	err := writer.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, writer)
	// New manager: same key path, empty key data (legacy path), but persist fails
	persistErr := errors.New("storage unavailable")
	readerStore := mock.NewKeyDataStorage(nil, persistErr)
	mgr := signing.NewRSAKeyManager(conf, passphrase, readerStore)
	err = mgr.Init()
	tassert.Fatalf(t, err != nil, "expected Init to fail when PersistKeyData fails in legacy path")
}

// PersistKeyData error in rotateKey path causes key update failure: key file must not be created.
func TestPersistKeyDataFailsOnCreate(t *testing.T) {
	keyPath := newTempKeyFile(t)
	conf := newRSAConfig(keyPath)
	persistErr := errors.New("persist failed")
	db := mock.NewKeyDataStorage(nil, persistErr)
	mgr := signing.NewRSAKeyManager(conf, genRandomPassphrase(t), db)
	err := mgr.Init()
	tassert.Fatalf(t, err != nil, "expected Init to fail when PersistKeyData fails on create")
	// Key file must not have been written
	_, err = os.Stat(keyPath)
	tassert.Fatalf(t, err != nil && errors.Is(err, fs.ErrNotExist), "key file must not exist after failed create, got err: %v", err)
}

// Rotate multiple times: each rotation changes the active key, accumulates all public keys in JWKS,
// and tokens signed with any prior key remain valid.
func TestRotateKey(t *testing.T) {
	db := &mock.KeyDataStorage{}
	mgr := signing.NewRSAKeyManager(defaultTestRSAConfig(t), genRandomPassphrase(t), db)
	err := mgr.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, mgr)

	const numRotations = 3
	tokens := make([]string, numRotations+1)
	pubKeys := make([]string, numRotations+1)
	claims := tok.AdminClaims(time.Now().Add(time.Hour), "rotate-test", "")

	pubKeys[0] = getAndAssertPubKey(t, mgr)
	tokens[0], err = mgr.SignToken(claims)
	tassert.CheckFatal(t, err)

	for i := 1; i <= numRotations; i++ {
		err = mgr.RotateKey()
		tassert.CheckFatal(t, err)

		pubKeys[i] = getAndAssertPubKey(t, mgr)
		tassert.Fatalf(t, pubKeys[i] != pubKeys[i-1], "rotation %d: public key should change", i)

		jwks, err := mgr.GetJWKS()
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, jwks.Len() == i+1, "rotation %d: expected %d keys in JWKS, got %d", i, i+1, jwks.Len())

		tokens[i], err = mgr.SignToken(claims)
		tassert.CheckFatal(t, err)
	}

	// New parser with the same rsa key manager should validate all token signatures
	parser := tok.NewTokenParser(mgr, nil)
	for _, token := range tokens {
		_, err = parser.ValidateToken(t.Context(), token)
		tassert.CheckFatal(t, err)
	}
}

// After rotation, a new manager loading from the same config and storage must validate with both keys
func TestRotateKeyPersistsAcrossReload(t *testing.T) {
	conf := defaultTestRSAConfig(t)
	passphrase := genRandomPassphrase(t)
	db := &mock.KeyDataStorage{}
	mgr := signing.NewRSAKeyManager(conf, passphrase, db)
	err := mgr.Init()
	tassert.CheckFatal(t, err)

	claims := tok.AdminClaims(time.Now().Add(time.Hour), "rotate-test", "")
	preToken, err := mgr.SignToken(claims)
	tassert.CheckFatal(t, err)

	err = mgr.RotateKey()
	tassert.CheckFatal(t, err)

	postToken, err := mgr.SignToken(claims)
	tassert.CheckFatal(t, err)

	// Simulate server restart: new manager with same config + storage
	mgr2 := signing.NewRSAKeyManager(conf, passphrase, db)
	err = mgr2.Init()
	tassert.CheckFatal(t, err)
	assertKeyBundleValid(t, mgr2)

	parser := tok.NewTokenParser(mgr2, nil)
	_, err = parser.ValidateToken(t.Context(), preToken)
	tassert.CheckFatal(t, err)
	_, err = parser.ValidateToken(t.Context(), postToken)
	tassert.CheckFatal(t, err)

	jwks, err := mgr2.GetJWKS()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, jwks.Len() == 2, "expected 2 keys in JWKS after reload, got %d", jwks.Len())
}
