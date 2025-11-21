// Package config manages config for the auth service
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	ServiceName = "AuthN"
)

// ConfManager Used by AuthN to interface with the shared authN config used on both API and server-side
// Provides utilities used only by the authN service (not API)
type ConfManager struct {
	conf atomic.Pointer[authn.Config]
	mu   sync.Mutex
}

func NewConfManager() *ConfManager {
	cm := &ConfManager{}
	cm.conf.Store(&authn.Config{})
	return cm
}

func NewConfManagerWithConf(c *authn.Config) *ConfManager {
	cm := &ConfManager{}
	cm.conf.Store(c)
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
	return nil
}

func (cm *ConfManager) Init(configDir string) {
	conf := cm.conf.Load()
	if configDir != "" {
		conf = loadFromDisk(configDir)
	}
	if val := os.Getenv(env.AisAuthSecretKey); val != "" {
		conf.Server.Secret = val
	}
	conf.Init()
	cm.conf.Store(conf)
	if !cm.HasHMACSecret() {
		cos.ExitLogf("Secret key not provided. Set in config or override with %q", env.AisAuthSecretKey)
	}
}

func loadFromDisk(configDir string) *authn.Config {
	configPath := filepath.Join(configDir, fname.AuthNConfig)
	rawConf := &authn.Config{}
	if _, err := jsp.LoadMeta(configPath, rawConf); err != nil {
		cos.ExitLogf("Failed to load configuration from %q: %v", configPath, err)
	}
	if rawConf.Verbose() {
		nlog.Infof("Loaded configuration from %s", configPath)
	}
	return rawConf
}

func (cm *ConfManager) SaveToDisk(configPath string) error {
	return jsp.SaveMeta(configPath, cm.conf.Load(), nil)
}

func (cm *ConfManager) GetLogDir() string {
	return cos.GetEnvOrDefault(env.AisAuthLogDir, cm.conf.Load().Log.Dir)
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

func (cm *ConfManager) GetPort() string {
	portStr := os.Getenv(env.AisAuthPort)
	if portStr == "" {
		portStr = fmt.Sprintf(":%d", cm.conf.Load().Net.HTTP.Port)
	} else {
		portStr = ":" + portStr
	}
	return portStr
}

func (cm *ConfManager) IsVerbose() bool {
	return cm.conf.Load().Verbose()
}

func (cm *ConfManager) GetSigConf() (*cmn.AuthSignatureConf, error) {
	if secret := cm.conf.Load().Secret(); secret != "" {
		return &cmn.AuthSignatureConf{Key: secret, Method: cmn.SigMethodHMAC}, nil
	}
	if key := cm.conf.Load().Server.PubKey; key != nil {
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

/////////
// RSA //
/////////

func (cm *ConfManager) GetPublicKeyString() *string {
	return cm.conf.Load().Server.PubKey
}
