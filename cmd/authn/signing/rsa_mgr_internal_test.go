// Package signing manages keys the auth service uses for signing and verification
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package signing

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	aisapc "github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	keyBits        = 2048
	tmpKeyFilename = "rsa_test.key"
)

func genRandomPassphrase(t *testing.T) cmn.Censored {
	passBytes := make([]byte, 16)
	_, err := rand.Read(passBytes)
	tassert.Fatalf(t, err == nil, "failed to generate passphrase: %v", err)
	return cmn.Censored(hex.EncodeToString(passBytes))
}

// Helper function to use key manager functions to encrypt invalid content for testing
func encryptInvalid(t *testing.T, keyPath string, passphrase cmn.Censored, content []byte) {
	conf := &config.RSAKeyConfig{
		Filepath: keyPath,
		Size:     keyBits,
	}
	mgr := NewRSAKeyManager(conf, passphrase)
	encrypted, err := mgr.encryptPrivateKey(content)
	if err != nil {
		t.Fatalf("failed to encrypt invalid PKCS8: %v", err)
	}
	// Cannot simply key manager save because the content is not a valid private key
	if err := os.WriteFile(keyPath, encrypted, 0o600); err != nil {
		t.Fatalf("failed to write corrupted key file: %v", err)
	}
}

func TestRSAKeyManager_LoadEncryptedKeyFailures(t *testing.T) {
	type corruptFn func(t *testing.T, keyPath string, pass cmn.Censored)

	tests := []struct {
		name    string
		mgrPass *string
		corrupt corruptFn
		msg     string
	}{
		{
			name:    "no passphrase provided",
			mgrPass: aisapc.Ptr(""),
			msg:     "expected loadFromDisk to fail without passphrase",
		},
		{
			name:    "invalid passphrase",
			mgrPass: aisapc.Ptr("valid-but-incorrect-phrase"),
			msg:     "expected loadFromDisk to fail with incorrect passphrase",
		},
		{
			name: "corrupted too short",
			corrupt: func(t *testing.T, keyPath string, _ cmn.Censored) {
				data := make([]byte, saltSize-1)
				if err := os.WriteFile(keyPath, data, 0o600); err != nil {
					t.Fatalf("failed to write corrupted key file: %v", err)
				}
			},
			msg: "expected loadFromDisk to fail with corrupted encrypted key (too short)",
		},
		{
			name: "corrupted too short for nonce",
			corrupt: func(t *testing.T, keyPath string, _ cmn.Censored) {
				// Data passes saltSize check but fails saltSize+nonceSize check
				data := make([]byte, saltSize+1)
				if err := os.WriteFile(keyPath, data, 0o600); err != nil {
					t.Fatalf("failed to write corrupted key file: %v", err)
				}
			},
			msg: "expected loadFromDisk to fail with data too short for nonce",
		},
		{
			name: "invalid content",
			corrupt: func(t *testing.T, keyPath string, pass cmn.Censored) {
				encryptInvalid(t, keyPath, pass, make([]byte, 200))
			},
			msg: "expected loadFromDisk to fail with invalid key structure",
		},
		{
			name: "invalid PEM",
			corrupt: func(t *testing.T, keyPath string, pass cmn.Censored) {
				data := []byte("this is random data, not encoded as valid PEM")
				encryptInvalid(t, keyPath, pass, data)
			},
			msg: "expected loadFromDisk to fail with invalid PEM after decryption",
		},
		{
			name: "invalid PKCS8",
			corrupt: func(t *testing.T, keyPath string, pass cmn.Censored) {
				data := pem.EncodeToMemory(&pem.Block{
					Type:  privateKeyPEMType,
					Bytes: []byte("this is encoded in a PEM block but not valid PKCS8 data"),
				})
				encryptInvalid(t, keyPath, pass, data)
			},
			msg: "expected loadFromDisk to fail with invalid PKCS8 data after decryption",
		},
		{
			name: "key file is directory",
			corrupt: func(t *testing.T, keyPath string, _ cmn.Censored) {
				// Remove the file and create a directory with the same name
				if err := os.Remove(keyPath); err != nil {
					t.Fatalf("failed to remove key file: %v", err)
				}
				if err := os.Mkdir(keyPath, 0o755); err != nil {
					t.Fatalf("failed to create directory: %v", err)
				}
				t.Cleanup(func() {
					os.Remove(keyPath)
				})
			},
			msg: "expected loadFromDisk to fail when key file is a directory",
		},
		{
			name: "key file is empty",
			corrupt: func(t *testing.T, keyPath string, _ cmn.Censored) {
				if err := os.Truncate(keyPath, 0); err != nil {
					t.Fatalf("failed to truncate key file: %v", err)
				}
			},
			msg: "expected loadFromDisk to fail when key file is empty",
		},
		{
			name: "non-RSA key type",
			corrupt: func(t *testing.T, keyPath string, pass cmn.Censored) {
				// Generate an EC key instead of RSA
				ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if err != nil {
					t.Fatalf("failed to generate EC key: %v", err)
				}
				keyBytes, err := x509.MarshalPKCS8PrivateKey(ecKey)
				if err != nil {
					t.Fatalf("failed to marshal EC key: %v", err)
				}
				pemData := pem.EncodeToMemory(&pem.Block{
					Type:  privateKeyPEMType,
					Bytes: keyBytes,
				})
				// Encrypt it with the same passphrase so decryption succeeds
				encryptInvalid(t, keyPath, pass, pemData)
			},
			msg: "expected loadFromDisk to fail with non-RSA key type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writePass := genRandomPassphrase(t)
			keyPath := filepath.Join(t.TempDir(), tmpKeyFilename)

			mgrConf := &config.RSAKeyConfig{Filepath: keyPath, Size: keyBits}
			writer := NewRSAKeyManager(mgrConf, writePass)
			err := writer.createKey()
			tassert.CheckFatal(t, err)

			// Apply the custom corruption function if testing a corrupted key on disk
			if tt.corrupt != nil {
				tt.corrupt(t, keyPath, writePass)
			}

			// Use writer's passphrase unless the test explicitly overrides it
			readPass := writePass
			if tt.mgrPass != nil {
				readPass = cmn.Censored(*tt.mgrPass)
			}
			// A new manager should fail loading the key in all of the above scenarios
			mgr := NewRSAKeyManager(mgrConf, readPass)
			err = mgr.loadFromDisk()
			tassert.Fatal(t, err != nil, tt.msg)
		})
	}
}
