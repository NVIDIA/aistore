// Package config manages config for the auth service
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"

	"golang.org/x/crypto/pbkdf2"
)

// Private Key encryption constants
const (
	saltSize         = 32
	keyLength        = 32
	pbkdf2Iterations = 100000
)

var (
	errEncryptedDataTooShort = errors.New("encrypted data is too short")
)

// RSAKeyManager is responsible for the lifecycle of RSA key pairs
// TODO: currently, only written at init time; key rotation will require sync
type RSAKeyManager struct {
	keyFilePath  string
	privateKey   *rsa.PrivateKey
	publicKeyPEM string
	keySize      int
	passphrase   cmn.Censored
}

func NewRSAKeyManager(keyFilePath string, keySize int, passphrase cmn.Censored) *RSAKeyManager {
	return &RSAKeyManager{
		keySize:     keySize,
		keyFilePath: keyFilePath,
		passphrase:  passphrase,
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
	// Any failed attempt to load an existing key from disk is treated as an error
	// This includes loading an unencrypted key if passphrase is set
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to load RSA key from file %q: %w", r.keyFilePath, err)
	}
	nlog.Infof("No RSA key found on disk at %q, generating...", r.keyFilePath)
	// Generate RSA key pair based on length from config
	key, err := rsa.GenerateKey(rand.Reader, r.keySize)
	if err != nil {
		return err
	}
	nlog.Infof("Generated new RSA key pair with modulus size %d bits", r.keySize)
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
	rawBytes, err := os.ReadFile(r.keyFilePath)
	if err != nil {
		return fmt.Errorf("read key file: %w", err)
	}
	keyBytes, err := r.parseKeyBytes(rawBytes)
	if err != nil {
		return err
	}
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return errors.New("decoding PEM block failed")
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

// Given a slice of key file bytes, decrypt if possible and return the bytes to parse as PEM
func (r *RSAKeyManager) parseKeyBytes(keyBytes []byte) ([]byte, error) {
	if r.passphrase == "" {
		return keyBytes, nil
	}
	keyBytes, err := r.decryptPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt private key: %w", err)
	}
	return keyBytes, nil
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
	if r.passphrase != "" {
		keyPEM, err = r.encryptPrivateKey(keyPEM)
		if err != nil {
			return fmt.Errorf("failed to encrypt private key: %w", err)
		}
	}
	// Write to file with restricted permissions (0600 - owner read/write only)
	if err := os.WriteFile(r.keyFilePath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("failed to write RSA key to %s: %w", r.keyFilePath, err)
	}

	nlog.Infof("Saved RSA private key to %s", r.keyFilePath)
	return nil
}

func (r *RSAKeyManager) encryptPrivateKey(keyPEM []byte) ([]byte, error) {
	// Generate a random salt for key derivation
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}
	gcm, err := createGCM(string(r.passphrase), salt)
	if err != nil {
		return nil, err
	}
	// Generate nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt the data
	ciphertext := gcm.Seal(nil, nonce, keyPEM, nil)

	// Format: [salt][nonce][ciphertext]
	encrypted := make([]byte, 0, len(salt)+len(nonce)+len(ciphertext))
	encrypted = append(encrypted, salt...)
	encrypted = append(encrypted, nonce...)
	encrypted = append(encrypted, ciphertext...)

	return encrypted, nil
}

func (r *RSAKeyManager) decryptPrivateKey(encrypted []byte) ([]byte, error) {
	if len(encrypted) < saltSize {
		return nil, errEncryptedDataTooShort
	}
	salt := encrypted[:saltSize]

	gcm, err := createGCM(string(r.passphrase), salt)
	if err != nil {
		return nil, err
	}
	if len(encrypted) <= saltSize+gcm.NonceSize() {
		return nil, errEncryptedDataTooShort
	}
	nonce := encrypted[saltSize : saltSize+gcm.NonceSize()]
	ciphertext := encrypted[saltSize+gcm.NonceSize():]

	// Decrypt the data
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

func (r *RSAKeyManager) GetPrivateKey() *rsa.PrivateKey {
	return r.privateKey
}

func (r *RSAKeyManager) GetPublicKeyPEM() string {
	return r.publicKeyPEM
}

func createGCM(pass string, salt []byte) (cipher.AEAD, error) {
	// Derive a key from the passphrase using PBKDF2
	key := pbkdf2.Key([]byte(pass), salt, pbkdf2Iterations, keyLength, sha256.New)

	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	return gcm, nil
}
