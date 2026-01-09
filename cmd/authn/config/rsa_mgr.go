// Package config manages config for the auth service
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

// RSAKeyManager is responsible for the lifecycle of RSA key pairs
// TODO: currently, only written at init time; key rotation will require sync
type RSAKeyManager struct {
	keyFilePath  string
	privateKey   *rsa.PrivateKey
	publicKeyPEM string
	keySize      int
}

func NewRSAKeyManager(keyFilePath string, keySize int) *RSAKeyManager {
	if keySize < MinRSAKeyBits {
		keySize = MinRSAKeyBits
	}
	return &RSAKeyManager{
		keySize:     keySize,
		keyFilePath: keyFilePath,
	}
}

// Init sets up an RSA key pair, using one from disk if provided
// Must only be called at init time -- key rotation not yet implemented
func (r *RSAKeyManager) Init() error {
	// Try to load existing key
	err := r.loadFromDisk()
	if err == nil {
		nlog.Infof("Loaded existing RSA private key from %s", r.keyFilePath)
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		nlog.Infof("No RSA key found on disk at %q", r.keyFilePath)
	} else {
		nlog.Infof("Failed to load RSA key from file %q [err: %v]", r.keyFilePath, err)
	}
	// Generate RSA key pair based on length from config
	key, err := rsa.GenerateKey(rand.Reader, r.keySize)
	if err != nil {
		return err
	}
	nlog.Infof("Generated new RSA key pair with modulus size %d bytes", key.Size())
	err = r.setKey(key)
	if err != nil {
		return err
	}
	return r.saveToDisk()
}

func (r *RSAKeyManager) loadFromDisk() error {
	fi, err := os.Stat(r.keyFilePath)
	if err != nil {
		return fmt.Errorf("stat key file: %w", err)
	}
	if fi.IsDir() {
		return errors.New("key file is a directory")
	}
	if fi.Size() == 0 {
		return errors.New("key file is empty")
	}
	keyBytes, err := os.ReadFile(r.keyFilePath)
	if err != nil {
		return fmt.Errorf("read key file: %w", err)
	}
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return errors.New("decoding private key PEM block failed")
	}
	keyAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse PKCS8 private key: %w", err)
	}
	key, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return errors.New("key type invalid")
	}
	if keyErr := key.Validate(); keyErr != nil {
		return fmt.Errorf("key validation failed: %w", keyErr)
	}
	return r.setKey(key)
}

func (r *RSAKeyManager) setKey(key *rsa.PrivateKey) error {
	r.privateKey = key
	// Convert to PKIX bytes
	pubBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return err
	}
	r.publicKeyPEM = string(pem.EncodeToMemory(&pem.Block{
		Type:  PublicKeyPEMType,
		Bytes: pubBytes,
	}))
	return nil
}

func (r *RSAKeyManager) saveToDisk() error {
	if r.privateKey == nil {
		return errors.New("no private key to save")
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(r.privateKey)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  PrivateKeyPEMType,
		Bytes: keyBytes,
	})

	// Write to file with restricted permissions (0600 - owner read/write only)
	if err := os.WriteFile(r.keyFilePath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("failed to write RSA key to %s: %w", r.keyFilePath, err)
	}

	nlog.Infof("Saved RSA private key to %s", r.keyFilePath)
	return nil
}

func (r *RSAKeyManager) GetPrivateKey() *rsa.PrivateKey {
	return r.privateKey
}

func (r *RSAKeyManager) GetPublicKeyPEM() string {
	return r.publicKeyPEM
}
