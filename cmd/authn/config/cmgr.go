// Package config manages config for the auth service
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"

	"github.com/lestrrat-go/jwx/v2/jwk"
)

const (
	ServiceName = "AuthN"
)

// Constants for creating RSA keys and key set
const (
	SigningKeyUsage   = "sig"
	PublicKeyPEMType  = "PUBLIC KEY"
	PrivateKeyPEMType = "PRIVATE KEY"
	MinRSAKeyBits     = 2048
)

// ConfManager Used by AuthN to interface with the shared authN config used on both API and server-side
// Provides utilities used only by the authN service (not API)
type ConfManager struct {
	filePath    string
	keyFilePath string
	conf        atomic.Pointer[authn.Config]
	jwks        jwk.Set
	mu          sync.Mutex
}

func NewConfManager() *ConfManager {
	cm := &ConfManager{}
	cm.conf.Store(&authn.Config{})
	return cm
}

// Server setup and general config functions

func (cm *ConfManager) GetConf() *authn.Config {
	return cm.conf.Load()
}

func (cm *ConfManager) UpdateConf(cu *authn.ConfigToUpdate) error {
	var newExpiry *cos.Duration
	// Validate updated values
	if cu.Server == nil {
		return errors.New("configuration is empty")
	}
	if cu.Server.Secret != nil && *cu.Server.Secret == "" {
		return errors.New("secret not defined")
	}
	if cu.Server.Expire != nil {
		exp, err := time.ParseDuration(*cu.Server.Expire)
		if err != nil {
			return fmt.Errorf("invalid time format %s: %v", *cu.Server.Expire, err)
		}
		d := cos.Duration(exp)
		newExpiry = &d
	}
	// Lock to allow partial updates
	cm.mu.Lock()
	defer cm.mu.Unlock()
	old := cm.conf.Load()
	next := *old // shallow copy
	if cu.Server.Secret != nil {
		next.Server.Secret = *cu.Server.Secret
	}
	if newExpiry != nil {
		next.Server.Expire = *newExpiry
	}
	// re-init to copy into private fields
	next.Init()
	cm.conf.Store(&next)
	if cm.filePath != "" {
		if err := jsp.SaveMeta(cm.filePath, cm.conf.Load(), nil); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConfManager) Init(configPath string) {
	conf, err := cm.loadFromDisk(configPath)
	if err != nil {
		cos.ExitLogf("%s service failed to start: %v", ServiceName, err)
	}
	if val := os.Getenv(env.AisAuthSecretKey); val != "" {
		conf.Server.Secret = val
	}
	conf.Init()
	cm.conf.Store(conf)
	if cm.HasHMACSecret() {
		return
	}
	nlog.Infof("No HMAC secret provided via config or %q, initializing with RSA", env.AisAuthSecretKey)
	cm.initKeyFile(configPath)
	err = cm.initRSA()
	if err != nil {
		cos.ExitLogf("Failed to initialize RSA key: %v", err)
	}
}

func (cm *ConfManager) loadFromDisk(configPath string) (*authn.Config, error) {
	cm.filePath = configPath
	rawConf := &authn.Config{}
	if _, err := jsp.LoadMeta(configPath, rawConf); err != nil {
		return nil, fmt.Errorf("failed to load configuration from %q: %v", configPath, err)
	}
	if rawConf.Verbose() {
		nlog.Infof("Loaded configuration from %s", configPath)
	}
	return rawConf, nil
}

func (cm *ConfManager) SaveToDisk(configPath string) error {
	return jsp.SaveMeta(configPath, cm.conf.Load(), nil)
}

func (cm *ConfManager) GetLogDir() string {
	return cos.GetEnvOrDefault(env.AisAuthLogDir, cm.conf.Load().Log.Dir)
}

func (cm *ConfManager) ParseExternalURL() (*url.URL, error) {
	raw := cos.GetEnvOrDefault(env.AisAuthExternalURL, cm.conf.Load().Net.ExternalURL)
	if raw != "" {
		return url.Parse(raw)
	}
	// fallback to localhost if not configured
	return cm.getLocalHost(), nil
}

func (cm *ConfManager) getLocalHost() *url.URL {
	scheme := "http"
	if cm.IsHTTPS() {
		scheme = "https"
	}
	port := cm.GetPort()
	host := cmn.HostPort("localhost", port)
	u := &url.URL{
		Scheme: scheme,
		Host:   host,
	}
	nlog.Warningf("No external URL configured for %s; defaulting to %s", ServiceName, u.String())
	return u
}

func (cm *ConfManager) IsHTTPS() bool {
	useHTTPS, err := cos.IsParseEnvBoolOrDefault(env.AisAuthUseHTTPS, cm.conf.Load().Net.HTTP.UseHTTPS)
	if err != nil {
		nlog.Errorf("Failed to parse %s: %v. Defaulting to false", env.AisAuthUseHTTPS, err)
	}
	return useHTTPS
}

func (cm *ConfManager) GetServerCert() string {
	return cos.GetEnvOrDefault(env.AisAuthServerCrt, cm.conf.Load().Net.HTTP.Certificate)
}

func (cm *ConfManager) GetServerKey() string {
	return cos.GetEnvOrDefault(env.AisAuthServerKey, cm.conf.Load().Net.HTTP.Key)
}

func (cm *ConfManager) GetPort() (s string) {
	s = os.Getenv(env.AisAuthPort)
	if s == "" {
		s = strconv.Itoa(cm.conf.Load().Net.HTTP.Port)
	}
	return
}

func (cm *ConfManager) IsVerbose() bool {
	return cm.conf.Load().Verbose()
}

func (cm *ConfManager) GetSigConf() (*cmn.AuthSignatureConf, error) {
	conf := cm.conf.Load()
	if secret := conf.Secret(); secret != "" {
		return &cmn.AuthSignatureConf{Key: secret, Method: cmn.SigMethodHMAC}, nil
	}
	if key := conf.Server.PubKey; key != nil {
		// Public key is not sensitive, but must be a censored type in conf
		return &cmn.AuthSignatureConf{Key: cmn.Censored(*key), Method: cmn.SigMethodRSA}, nil
	}
	return nil, fmt.Errorf("invalid config for %s service: no validation key provided", ServiceName)
}

func (cm *ConfManager) GetExpiry() time.Duration {
	return cm.conf.Load().Expire()
}

func (cm *ConfManager) GetDefaultTimeout() time.Duration {
	return time.Duration(cm.conf.Load().Timeout.Default)
}

//////////
// HMAC //
//////////

func (cm *ConfManager) HasHMACSecret() bool { return cm.conf.Load().Secret() != "" }

func (cm *ConfManager) GetSecret() cmn.Censored {
	return cm.conf.Load().Secret()
}

func (cm *ConfManager) GetSecretChecksum() string {
	secret := string(cm.conf.Load().Secret())
	return cos.ChecksumB2S(cos.UnsafeB(secret), cos.ChecksumSHA256)
}

//////////////////
// RSA and OIDC //
//////////////////

// Determine location for private key persistence
func (cm *ConfManager) initKeyFile(configPath string) {
	if pFile := os.Getenv(env.AisAuthPrivateKeyFile); pFile != "" {
		cm.keyFilePath = pFile
	} else {
		configDir := filepath.Dir(configPath)
		cm.keyFilePath = filepath.Join(configDir, fname.AuthNRSAKey)
	}
}

// Initialize an RSA key pair, using one from disk if provided
// Must only be called at init time -- key rotation not yet implemented
func (cm *ConfManager) initRSA() error {
	// Try to load existing key
	err := cm.loadRSAFromDisk()
	if err == nil {
		nlog.Infof("Loaded existing RSA private key from %s", cm.keyFilePath)
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		nlog.Infof("No RSA key found on disk at %q", cm.keyFilePath)
	} else {
		nlog.Infof("Failed to load RSA key from file %q [err: %v]", cm.keyFilePath, err)
	}
	// Generate RSA key pair based on length from config
	key, err := rsa.GenerateKey(rand.Reader, cm.GetKeySize())
	if err != nil {
		return err
	}
	nlog.Infof("Generated new RSA key pair with modulus size %d bytes", key.Size())
	err = cm.setRSAFromPrivate(key)
	if err != nil {
		return err
	}
	return cm.saveRSAToDisk()
}

func (cm *ConfManager) loadRSAFromDisk() error {
	fi, err := os.Stat(cm.keyFilePath)
	if err != nil {
		return fmt.Errorf("stat key file: %w", err)
	}
	if fi.IsDir() {
		return errors.New("key file is a directory")
	}
	if fi.Size() == 0 {
		return errors.New("key file is empty")
	}
	keyBytes, err := os.ReadFile(cm.keyFilePath)
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
	return cm.setRSAFromPrivate(key)
}

// Updates both the service config and the jwks
// If called from outside init, must be called under lock
func (cm *ConfManager) setRSAFromPrivate(key *rsa.PrivateKey) error {
	// Convert to PKIX bytes
	pubBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return err
	}
	// Encode as PEM string
	pubPem := string(pem.EncodeToMemory(&pem.Block{
		Type:  PublicKeyPEMType,
		Bytes: pubBytes,
	}))
	cm.setRSAKey(key, pubPem)
	return cm.createPubJWKS()
}

// Must be called under lock to enable partial update of config
func (cm *ConfManager) setRSAKey(pKey *rsa.PrivateKey, pubStr string) {
	old := cm.conf.Load()
	next := *old
	next.SetPrivateKey(pKey)
	next.Server.PubKey = &pubStr
	next.Init()
	cm.conf.Store(&next)
}

func (cm *ConfManager) saveRSAToDisk() error {
	privateKey := cm.GetPrivateKey()
	if privateKey == nil {
		return errors.New("no private key to save")
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  PrivateKeyPEMType,
		Bytes: keyBytes,
	})

	// Write to file with restricted permissions (0600 - owner read/write only)
	if err := os.WriteFile(cm.keyFilePath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("failed to write RSA key to %s: %w", cm.keyFilePath, err)
	}

	nlog.Infof("Saved RSA private key to %s", cm.keyFilePath)
	return nil
}

func (cm *ConfManager) GetPrivateKey() *rsa.PrivateKey {
	return cm.conf.Load().GetPrivateKey()
}

func (cm *ConfManager) GetPublicKeyString() *string {
	return cm.conf.Load().Server.PubKey
}

func (cm *ConfManager) GetKeySet() jwk.Set {
	return cm.jwks
}

func (cm *ConfManager) GetKeySize() int {
	bits := cm.conf.Load().Server.RSAKeyBits
	if bits < MinRSAKeyBits {
		return MinRSAKeyBits
	}
	return bits
}

// Sets jwks for the manager, must be called under lock when creating RSA
func (cm *ConfManager) createPubJWKS() error {
	conf := cm.conf.Load()
	pubJWK, err := createPubJWK(conf.GetPrivateKey())
	if err != nil {
		return fmt.Errorf("failed to generate public JWK: %w", err)
	}
	keySet := jwk.NewSet()
	err = keySet.AddKey(pubJWK)
	if err != nil {
		return fmt.Errorf("failed to add public key to keyset: %w", err)
	}
	cm.jwks = keySet
	return nil
}

func createPubJWK(pKey *rsa.PrivateKey) (jwk.Key, error) {
	jwkKey, err := jwk.FromRaw(pKey)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}
	// jwk Key does not automatically assign a key ID, so use the utility function to do this
	err = jwk.AssignKeyID(jwkKey)
	if err != nil {
		return nil, fmt.Errorf("failed to assign key id: %w", err)
	}
	// Set metadata "alg" and "use"
	err = jwkKey.Set(jwk.AlgorithmKey, authn.SigningMethodRS256)
	if err != nil {
		return nil, fmt.Errorf("failed to set algorithm: %w", err)
	}
	err = jwkKey.Set(jwk.KeyUsageKey, SigningKeyUsage)
	if err != nil {
		return nil, fmt.Errorf("failed to set key usage: %w", err)
	}
	return jwkKey.PublicKey()
}
