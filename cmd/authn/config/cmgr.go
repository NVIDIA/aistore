// Package config manages config for the auth service
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
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
	ServiceName   = "AuthN"
	defaultLogDir = "logs"
)

// Constants for creating RSA keys and key set
const (
	SigningKeyUsage      = "sig"
	PublicKeyPEMType     = "PUBLIC KEY"
	PrivateKeyPEMType    = "PRIVATE KEY"
	MinPassphraseLength  = 8
	MinPassphraseEntropy = 3
)

// ConfManager Used by AuthN to interface with the shared authN config used on both API and server-side
// Provides utilities used only by the authN service (not API)
type ConfManager struct {
	filePath string
	rsaMgr   *RSAKeyManager
	conf     atomic.Pointer[authn.Config]
	jwks     jwk.Set
	mu       sync.Mutex
}

func NewConfManager() *ConfManager {
	cm := &ConfManager{}
	cm.conf.Store(&authn.Config{})
	return cm
}

func resolveConfigPath(flagConf string) (string, error) {
	confPath := cos.GetEnvOrDefault(env.AisAuthConfDir, flagConf)
	if confPath == "" {
		return "", fmt.Errorf("missing %s configuration file (use '-config' or '%s')",
			ServiceName, env.AisAuthConfDir)
	}
	fi, err := os.Stat(confPath)
	if err != nil {
		var e *os.PathError
		if errors.As(err, &e) {
			err = e.Err
		}
		return "", fmt.Errorf("invalid file path %q: %v", confPath, err)
	}
	confPath, err = filepath.Abs(confPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for %q: %v", confPath, err)
	}
	if fi.IsDir() {
		return filepath.Join(confPath, fname.AuthNConfig), nil
	}
	return confPath, nil
}

// Server setup and general config functions

func (cm *ConfManager) GetConf() *authn.Config {
	return cm.conf.Load()
}

func (cm *ConfManager) UpdateConf(cu *authn.ConfigToUpdate) error {
	// Validate updated values
	if err := cu.Validate(); err != nil {
		return fmt.Errorf("invalid config update: %v", err)
	}
	// Lock to allow partial updates
	cm.mu.Lock()
	defer cm.mu.Unlock()
	old := cm.conf.Load()
	next := *old // shallow copy
	if cu.Server.Secret != nil {
		next.Server.Secret = *cu.Server.Secret
	}
	if cu.Server.Expire != nil {
		next.Server.Expire = *cu.Server.Expire
	}
	// Validate config after updates before storing
	err := next.Validate()
	if err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}
	// re-init to copy into private fields
	next.Init()
	cm.conf.Store(&next)
	return cm.saveToDisk()
}

func (cm *ConfManager) Init(cfgPathArg string) {
	filePath, err := resolveConfigPath(cfgPathArg)
	if err != nil {
		cos.ExitLogf("Invalid config for %s: %v", ServiceName, err)
	}
	cm.filePath = filePath
	// Load from disk first because log directory is configurable
	conf, err := cm.loadFromDisk()
	if err != nil {
		cos.ExitLogf("%s service failed to start: %v", ServiceName, err)
	}
	err = cm.initLogs()
	if err != nil {
		cos.ExitLogf("Failed to set up logger: %v", err)
	}
	// Log config loading after initializing the logger
	nlog.Infoln("Loaded configuration from", cm.filePath)
	if val := os.Getenv(env.AisAuthSecretKey); val != "" {
		conf.Server.Secret = val
	}
	conf.Init()
	cm.conf.Store(conf)
	if cm.HasHMACSecret() {
		return
	}
	nlog.Infof("No HMAC secret provided via config or %q, initializing with RSA", env.AisAuthSecretKey)
	cm.initRSA()
}

func (cm *ConfManager) initLogs() error {
	logDir := cos.GetEnvOrDefault(env.AisAuthLogDir, cm.conf.Load().Log.Dir)
	// If not provided, use a subdirectory in the same directory as the config
	if logDir == "" {
		logDir = filepath.Join(filepath.Dir(cm.filePath), defaultLogDir)
	}
	if err := cos.CreateDir(logDir); err != nil {
		return fmt.Errorf("failed to create log dir %q, err: %v", logDir, err)
	}
	nlog.SetPre(logDir, "auth")
	nlog.Infoln("Logging to", logDir)
	return nil
}

func (cm *ConfManager) loadFromDisk() (*authn.Config, error) {
	rawConf := &authn.Config{}
	if _, err := jsp.LoadMeta(cm.filePath, rawConf); err != nil {
		return nil, fmt.Errorf("failed to load configuration from %q: %v", cm.filePath, err)
	}
	if err := rawConf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration from %q: %v", cm.filePath, err)
	}
	return rawConf, nil
}

func (cm *ConfManager) saveToDisk() error {
	return jsp.SaveMeta(cm.filePath, cm.conf.Load(), nil)
}

func (cm *ConfManager) GetLogFlushInterval() time.Duration {
	return cm.conf.Load().Log.FlushInterval.D()
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

func (cm *ConfManager) initRSA() {
	keyFilePath := cos.GetEnvOrDefault(env.AisAuthPrivateKeyFile, filepath.Join(filepath.Dir(cm.filePath), fname.AuthNRSAKey))
	passphrase, err := validatePassphrase()
	if err != nil {
		cos.ExitLogf("Failed RSA key passphrase validation for %s: %v", env.AisAuthPrivateKeyPass, err)
	}
	cm.rsaMgr = NewRSAKeyManager(keyFilePath, cm.conf.Load().Server.RSAKeyBits, passphrase)
	if err := cm.rsaMgr.Init(); err != nil {
		cos.ExitLogf("Failed to initialize RSA key: %v", err)
	}

	cm.updateConfPubKey()
	if err := cm.createPubJWKS(); err != nil {
		cos.ExitLogf("Failed to initialize JWKS from RSA key: %v", err)
	}
}

func validatePassphrase() (cmn.Censored, error) {
	passphrase, found := os.LookupEnv(env.AisAuthPrivateKeyPass)
	if !found {
		return "", nil
	}
	if len(passphrase) < MinPassphraseLength {
		return "", fmt.Errorf("too short (must be at least %d characters)", MinPassphraseLength)
	}
	if cos.Entropy(passphrase) < MinPassphraseEntropy {
		return "", errors.New("passphrase strength too low. Try increasing length or complexity")
	}
	disallowed := "\t\n\r"
	if strings.ContainsAny(passphrase, disallowed) {
		return "", fmt.Errorf("cannot contain whitespace escape sequences: %q", disallowed)
	}
	return cmn.Censored(passphrase), nil
}

// If called outside init, must be called under lock to enable partial update of config
func (cm *ConfManager) updateConfPubKey() {
	old := cm.conf.Load()
	next := *old
	next.Server.PubKey = apc.Ptr(cm.rsaMgr.GetPublicKeyPEM())
	next.Init()
	cm.conf.Store(&next)
}

func (cm *ConfManager) GetPrivateKey() *rsa.PrivateKey {
	if cm.rsaMgr == nil {
		return nil
	}
	return cm.rsaMgr.GetPrivateKey()
}

func (cm *ConfManager) GetPublicKeyString() *string {
	return cm.conf.Load().Server.PubKey
}

func (cm *ConfManager) GetKeySet() jwk.Set {
	return cm.jwks
}

func (cm *ConfManager) GetDBType() string {
	return cm.conf.Load().Server.DBConf.DBType
}

func (cm *ConfManager) GetDBPath() string {
	dbConf := cm.conf.Load().Server.DBConf
	if fp := dbConf.Filepath; fp != "" {
		return fp
	}
	return filepath.Join(filepath.Dir(cm.filePath), fname.AuthNDB)
}

// Sets jwks for the manager, must be called under lock when creating RSA
func (cm *ConfManager) createPubJWKS() error {
	pubJWK, err := createPubJWK(cm.rsaMgr.GetPrivateKey())
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
