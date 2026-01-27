// Package config manages config for the auth service
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"encoding/pem"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const keyBits = 2048

func genRandomPassphrase(t *testing.T) cmn.Censored {
	passBytes := make([]byte, 16)
	_, err := rand.Read(passBytes)
	tassert.Fatalf(t, err == nil, "failed to generate passphrase: %v", err)
	return cmn.Censored(hex.EncodeToString(passBytes))
}

func createAndSaveKey(t *testing.T, dst string, passphrase cmn.Censored) *RSAKeyManager {
	// Create manager with passphrase
	mgr := NewRSAKeyManager(dst, keyBits, passphrase)

	// Generate and set a key
	key, err := rsa.GenerateKey(rand.Reader, keyBits)
	tassert.Fatalf(t, err == nil, "failed to generate RSA key: %v", err)
	err = mgr.setKey(key)
	tassert.Fatalf(t, err == nil, "failed to set key: %v", err)

	// Save to disk
	err = mgr.saveToDisk()
	tassert.Fatalf(t, err == nil, "saveToDisk failed: %v", err)
	return mgr
}

func newTempKeyFile(t *testing.T) string {
	t.Helper()

	tmpFile, err := os.CreateTemp(t.TempDir(), "rsa_test_*.key")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	keyPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Remove(keyPath); err != nil {
			t.Errorf("failed to remove temp file %s: %v", keyPath, err)
		}
	})

	return keyPath
}

// Helper function to use key manager functions to encrypt invalid content for testing
func encryptInvalid(t *testing.T, keyPath string, passphrase cmn.Censored, content []byte) {
	mgr := NewRSAKeyManager(keyPath, keyBits, passphrase)
	encrypted, err := mgr.encryptPrivateKey(content)
	if err != nil {
		t.Fatalf("failed to encrypt invalid PKCS8: %v", err)
	}
	// Cannot simply key manager save because the content is not a valid private key
	if err := os.WriteFile(keyPath, encrypted, 0o600); err != nil {
		t.Fatalf("failed to write corrupted key file: %v", err)
	}
}

func TestRSAKeyManager_SaveAndLoad(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	mgr := createAndSaveKey(t, keyPath, passphrase)

	// Create new manager and load
	mgr2 := NewRSAKeyManager(keyPath, keyBits, passphrase)
	err := mgr2.loadFromDisk()
	tassert.CheckFatal(t, err)

	// Verify keys match
	tassert.Fatal(t, mgr.privateKey.Equal(mgr2.privateKey), "loaded key does not match saved key")
	tassert.Fatal(t, mgr.publicKeyPEM == mgr2.publicKeyPEM, "loaded public key PEM does not match saved")
}

func TestRSAKeyManager_LoadNoPassphrase(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	createAndSaveKey(t, keyPath, passphrase)
	// Verify manager without passphrase fails to load
	mgr2 := NewRSAKeyManager(keyPath, keyBits, "")
	err := mgr2.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected loadFromDisk to fail without passphrase")
}

func TestRSAKeyManager_LoadInvalidPassphrase(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	createAndSaveKey(t, keyPath, passphrase)
	// Verify manager with invalid passphrase fails to load
	mgr2 := NewRSAKeyManager(keyPath, keyBits, "valid-but-incorrect-phrase")
	err := mgr2.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected loadFromDisk to fail with incorrect passphrase")
}

func TestRSAKeyManager_LoadCorruptedEncryptedKey_TooShort(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	createAndSaveKey(t, keyPath, passphrase)

	// Corrupt the file by truncating it to be shorter than saltSize
	corruptedData := make([]byte, saltSize-1)
	if err := os.WriteFile(keyPath, corruptedData, 0o600); err != nil {
		t.Fatalf("failed to write corrupted key file: %v", err)
	}

	mgr := NewRSAKeyManager(keyPath, keyBits, passphrase)
	err := mgr.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected loadFromDisk to fail with corrupted encrypted key (too short)")
}

func TestRSAKeyManager_LoadCorruptedEncryptedKey_InvalidContent(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	createAndSaveKey(t, keyPath, passphrase)

	// Pass the length check but fail during decryption (invalid PEM block)
	encryptInvalid(t, keyPath, passphrase, make([]byte, 200))

	mgr := NewRSAKeyManager(keyPath, keyBits, passphrase)
	err := mgr.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected loadFromDisk to fail with invalid key structure")
}

func TestRSAKeyManager_LoadCorruptedEncryptedKey_InvalidPEM(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	createAndSaveKey(t, keyPath, passphrase)

	invalidPEM := []byte("this is random data, not encoded as valid PEM")
	encryptInvalid(t, keyPath, passphrase, invalidPEM)

	mgr := NewRSAKeyManager(keyPath, keyBits, passphrase)
	err := mgr.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected loadFromDisk to fail with invalid PEM after decryption")
}

func TestRSAKeyManager_LoadCorruptedEncryptedKey_InvalidPKCS8(t *testing.T) {
	keyPath := newTempKeyFile(t)
	passphrase := genRandomPassphrase(t)
	createAndSaveKey(t, keyPath, passphrase)

	// Create a PEM block with random bytes instead of valid PKCS8 data
	invalidPKCS8 := pem.EncodeToMemory(&pem.Block{
		Type:  PrivateKeyPEMType,
		Bytes: []byte("this is encoded in a PEM block but not valid PKCS8 data"),
	})
	encryptInvalid(t, keyPath, passphrase, invalidPKCS8)

	mgr := NewRSAKeyManager(keyPath, keyBits, passphrase)
	err := mgr.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected loadFromDisk to fail with invalid PKCS8 data after decryption")
}
