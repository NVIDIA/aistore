// Package config manages config for the auth service
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
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
)

const (
	ServiceName   = "AuthN"
	defaultLogDir = "logs"
)

// ConfManager Used by AuthN to interface with the shared authN config used on both API and server-side
// Provides utilities used only by the authN service (not API)
type (
	ConfManager struct {
		filePath    string
		conf        atomic.Pointer[authn.Config]
		mu          sync.Mutex
		externalURL *url.URL
	}
	RSAKeyConfig struct {
		Filepath string
		Size     int
		// AllowKeyGeneration controls whether the manager may auto-generate a new
		// RSA key pair when none is found at Filepath.  Set to false when the key
		// is externally provisioned (e.g. mounted from a Kubernetes Secret for
		// multi-replica deployments): a missing key is then a configuration error,
		// not a recoverable situation, and key rotation via the API is also blocked.
		AllowKeyGeneration bool
	}
)

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
	conf, err := cm.loadFromDisk()
	if err != nil {
		cos.ExitLogf("%s service failed to start: %v", ServiceName, err)
	}
	// Store raw config so initLogs and initExternalURL can read it
	cm.conf.Store(conf)
	err = cm.initLogs()
	if err != nil {
		cos.ExitLogf("Failed to set up logger: %v", err)
	}
	err = cm.initExternalURL()
	if err != nil {
		cos.ExitLogf("Failed to set up external URL: %v", err)
	}
	nlog.Infoln("Loaded configuration from", cm.filePath)
	if val := os.Getenv(env.AisAuthSecretKey); val != "" {
		conf.Server.Secret = val
	}
	conf.Init()
	cm.conf.Store(conf)
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

// Parse the URL configured for an external client to access this service
// Note this is most likely different from what we serve, because of port mappings, tls termination, etc.
func (cm *ConfManager) initExternalURL() error {
	var (
		external *url.URL
		err      error
	)
	raw := cos.GetEnvOrDefault(env.AisAuthExternalURL, cm.conf.Load().Net.ExternalURL)
	if raw != "" {
		external, err = authn.ParseExternalURL(raw)
		if err != nil {
			return err
		}
	} else {
		// fallback to localhost if not configured
		external = cm.getLocalHost()
	}
	nlog.Infof("Using %s as externally accessible URL", external)
	cm.externalURL = external
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

func (cm *ConfManager) GetExpiry() time.Duration {
	return cm.conf.Load().Expire()
}

func (cm *ConfManager) GetDefaultTimeout() time.Duration {
	return time.Duration(cm.conf.Load().Timeout.Default)
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

func (cm *ConfManager) GetExternalURL() *url.URL {
	return cm.externalURL
}

//////////
// HMAC //
//////////

func (cm *ConfManager) GetSecret() cmn.Censored {
	return cm.conf.Load().Secret()
}

/////////
// RSA //
/////////

func (cm *ConfManager) GetRSAConfig() *RSAKeyConfig {
	keyFilePath := cos.GetEnvOrDefault(env.AisAuthPrivateKeyFile, filepath.Join(filepath.Dir(cm.filePath), fname.AuthNRSAKey))
	// When using Redis, the key must be pre-provisioned and shared across all
	// replicas (e.g. from a Kubernetes Secret).  Auto-generation would produce a
	// different key on every replica, breaking cross-replica token validation.
	allowGen := cm.GetDBType() != "Redis"
	return &RSAKeyConfig{
		Filepath:           keyFilePath,
		Size:               cm.conf.Load().Server.RSAKeyBits,
		AllowKeyGeneration: allowGen,
	}
}

//////////
// Redis //
//////////

// GetRedisConf returns the Redis connection configuration, with env vars taking precedence over config file values.
func (cm *ConfManager) GetRedisConf() *authn.RedisConf {
	base := cm.conf.Load().Server.DBConf.Redis
	c := base // copy

	if v := os.Getenv(env.AisAuthRedisAddr); v != "" {
		c.Addr = v
	}
	if v := os.Getenv(env.AisAuthRedisPassword); v != "" {
		c.Password = v
	}
	if v := os.Getenv(env.AisAuthRedisDB); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.DB = n
		}
	}
	if c.Addr == "" {
		c.Addr = "localhost:6379"
	}
	return &c
}
