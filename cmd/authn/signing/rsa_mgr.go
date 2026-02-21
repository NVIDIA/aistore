// Package signing manages keys the auth service uses for signing and verification
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package signing

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
	"io/fs"
	"os"
	"sync"
	"sync/atomic"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"golang.org/x/crypto/pbkdf2"
)

// Private Key encryption constants
const (
	saltSize         = 32
	keyLength        = 32
	pbkdf2Iterations = 100000
)

// Constants for creating RSA keys and key set
const (
	signingKeyUsage      = "sig"
	publicKeyPEMType     = "PUBLIC KEY"
	privateKeyPEMType    = "PRIVATE KEY"
	tmpFileSuffix        = ".tmp"
	MinPassphraseLength  = 8
	MinPassphraseEntropy = 3
)

const msgUninitialized = "init failed (fatal) or never called"

var (
	errEncryptedDataTooShort = errors.New("encrypted data is too short")
)

// RSAKeyManager is responsible for the lifecycle of RSA key pairs
// TODO: currently, only written at init time; key rotation will require sync
type (
	JWKSProvider interface {
		GetJWKS() (jwk.Set, error)
	}

	RSAKeyManager struct {
		conf       *config.RSAKeyConfig
		passphrase cmn.Censored
		rotateMu   sync.Mutex
		bundle     atomic.Pointer[keyBundle]
	}

	// keyBundle groups an RSA key pair and its derived JWKS for atomic operations
	keyBundle struct {
		privateKey   *rsa.PrivateKey
		publicKeyPEM string
		keyID        string
		jwks         jwk.Set
	}
)

// interface guard
var _ tok.Signer = (*RSAKeyManager)(nil)
var _ JWKSProvider = (*RSAKeyManager)(nil)

func NewRSAKeyManager(conf *config.RSAKeyConfig, passphrase cmn.Censored) *RSAKeyManager {
	// All fields are value types, copy to avoid external mutation
	c := *conf
	return &RSAKeyManager{
		conf:       &c,
		passphrase: passphrase,
	}
}

// Init sets up an RSA key pair, using one from disk if provided
// Must only be called at init time -- key rotation not yet implemented
func (r *RSAKeyManager) Init() error {
	path := r.conf.Filepath
	// Try to load existing key
	err := r.loadFromDisk()
	if err == nil {
		nlog.Infof("Loaded existing RSA private key from %s", path)
		return nil
	}
	// No existing file -- generate and persist a new one
	if errors.Is(err, fs.ErrNotExist) {
		nlog.Infof("No RSA key found on disk at %q, generating...", path)
		return r.createKey()
	}
	// Any failed attempt to load an existing key from disk is treated as an error
	// This includes loading an unencrypted key if passphrase is set
	return fmt.Errorf("failed to load RSA key from file %q: %w", path, err)
}

func (r *RSAKeyManager) loadFromDisk() error {
	rawBytes, err := r.loadFileBytes()
	if err != nil {
		return err
	}
	keyBytes, err := r.parseKeyBytes(rawBytes)
	if err != nil {
		return err
	}
	return r.parseAndLoadKey(keyBytes)
}

func (r *RSAKeyManager) loadFileBytes() ([]byte, error) {
	rawBytes, err := os.ReadFile(r.conf.Filepath)
	if err != nil {
		return nil, err
	}
	if len(rawBytes) == 0 {
		return nil, errors.New("private key file is empty")
	}
	return rawBytes, nil
}

func (r *RSAKeyManager) parseAndLoadKey(fileBytes []byte) error {
	block, _ := pem.Decode(fileBytes)
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
	// TODO: persist JWKS instead of regenerating only from current key
	bundle, err := createKeyBundle(key, jwk.NewSet())
	if err != nil {
		return err
	}
	r.bundle.Store(bundle)
	return nil
}

// Generate a new RSA key pair and the derived bundle and atomically save it to disk and update in memory
func (r *RSAKeyManager) createKey() error {
	key, err := rsa.GenerateKey(rand.Reader, r.conf.Size)
	if err != nil {
		return err
	}
	nlog.Infof("Generated new RSA key pair with modulus size %d bits", r.conf.Size)
	bundle, err := createKeyBundle(key, jwk.NewSet())
	if err != nil {
		return err
	}
	r.rotateMu.Lock()
	defer r.rotateMu.Unlock()
	return r.commitUnderLock(bundle)
}

// commitUnderLock writes to disk atomically, then updates memory
// Must be called under lock to ensure file and memory contents are kept in sync
func (r *RSAKeyManager) commitUnderLock(bundle *keyBundle) error {
	// TODO: persist entire key bundle
	err := r.saveToDisk(bundle.privateKey)
	if err != nil {
		return err
	}
	// If succeeded, update in-memory state for current bundle
	r.bundle.Store(bundle)
	return nil
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

func (r *RSAKeyManager) saveToDisk(key *rsa.PrivateKey) error {
	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  privateKeyPEMType,
		Bytes: keyBytes,
	})
	if r.passphrase != "" {
		keyPEM, err = r.encryptPrivateKey(keyPEM)
		if err != nil {
			return fmt.Errorf("failed to encrypt private key: %w", err)
		}
	}
	// Write to tmp file with restricted permissions (0600 - owner read/write only)
	finalPath := r.conf.Filepath
	tmpPath := finalPath + tmpFileSuffix
	if err := os.WriteFile(tmpPath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("failed to write temp key file: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		rmErr := os.Remove(tmpPath)
		if rmErr != nil {
			nlog.Warningln("failed to remove temp key file:", rmErr)
		}
		return fmt.Errorf("failed to rename key file: %w", err)
	}

	nlog.Infof("Saved RSA private key to %s", finalPath)
	return nil
}

func (r *RSAKeyManager) encryptPrivateKey(keyPEM []byte) ([]byte, error) {
	// Generate a random salt for key derivation
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}
	gcm, err := createGCM(r.passphrase, salt)
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

	gcm, err := createGCM(r.passphrase, salt)
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

func createGCM(pass cmn.Censored, salt []byte) (cipher.AEAD, error) {
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

// createKeyBundle creates a keyBundle struct with the associated public key PEM and JWKS
// Modifies the given jwks
func createKeyBundle(key *rsa.PrivateKey, jwks jwk.Set) (*keyBundle, error) {
	pubPEM, err := getPubPEM(key)
	if err != nil {
		return nil, err
	}
	jwkKey, err := createPubJWK(key)
	if err != nil {
		return nil, err
	}
	err = jwks.AddKey(jwkKey)
	if err != nil {
		return nil, err
	}

	return &keyBundle{
		privateKey:   key,
		publicKeyPEM: pubPEM,
		keyID:        jwkKey.KeyID(),
		jwks:         jwks,
	}, nil
}

func getPubPEM(key *rsa.PrivateKey) (string, error) {
	// Convert to PKIX bytes
	pubBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", err
	}
	pubPEM := string(pem.EncodeToMemory(&pem.Block{
		Type:  publicKeyPEMType,
		Bytes: pubBytes,
	}))
	return pubPEM, nil
}

// Create JWK from the RSA private key with extra metadata
func createPubJWK(pKey *rsa.PrivateKey) (jwk.Key, error) {
	jwkKey, jwkErr := deriveKeyWithID(pKey)
	if jwkErr != nil {
		return nil, jwkErr
	}
	if err := jwkKey.Set(jwk.AlgorithmKey, authn.SigningMethodRS256); err != nil {
		return nil, fmt.Errorf("failed to set algorithm: %w", err)
	}
	if err := jwkKey.Set(jwk.KeyUsageKey, signingKeyUsage); err != nil {
		return nil, fmt.Errorf("failed to set key usage: %w", err)
	}
	return jwkKey.PublicKey()
}

// deriveKeyWithID derives a JWK key from a private key and assigns an ID for JWKS lookup
func deriveKeyWithID(key *rsa.PrivateKey) (jwk.Key, error) {
	jwkKey, jwkErr := jwk.FromRaw(key)
	if jwkErr != nil {
		return nil, fmt.Errorf("failed to parse JWK from RSA key: %w", jwkErr)
	}
	if err := jwk.AssignKeyID(jwkKey); err != nil {
		return nil, fmt.Errorf("failed to assign key id: %w", err)
	}
	return jwkKey, nil
}

func (r *RSAKeyManager) ValidationConf() *authn.ServerConf {
	if c := r.bundle.Load(); c != nil {
		return &authn.ServerConf{PubKey: &c.publicKeyPEM}
	}
	debug.Assert(false, msgUninitialized)
	return nil
}

func (r *RSAKeyManager) GetJWKS() (jwk.Set, error) {
	if c := r.bundle.Load(); c != nil {
		return c.jwks.Clone()
	}
	debug.Assert(false, msgUninitialized)
	return jwk.NewSet(), nil
}

func (r *RSAKeyManager) GetSigConf() *cmn.AuthSignatureConf {
	if c := r.bundle.Load(); c != nil {
		// Public key is not sensitive, but must be a censored type in conf
		return &cmn.AuthSignatureConf{Key: cmn.Censored(c.publicKeyPEM), Method: cmn.SigMethodRSA}
	}
	debug.Assert(false, msgUninitialized)
	return nil
}

// SignToken signs JWT claims with the current RSA private key and includes the key ID header
func (r *RSAKeyManager) SignToken(c jwt.Claims) (string, error) {
	b := r.bundle.Load()
	if b == nil || b.privateKey == nil {
		return "", errors.New("RSA key manager not initialized")
	}
	t := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	t.Header["kid"] = b.keyID
	return t.SignedString(b.privateKey)
}
