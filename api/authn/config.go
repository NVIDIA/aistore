// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// Constraints
const (
	minRSAKeyBits     = 2048
	minAuthExpiration = cos.Duration(time.Minute)
	// minPort avoids 0-1023 system-reserved port range
	minPort             = 1024
	maxPort             = 65535
	minTimeout          = cos.Duration(time.Second)
	minLogFlushInterval = cos.Duration(10 * time.Second)
	maxLogLevel         = 5
)

// Defaults
const (
	// ForeverTokenTime is a duration of 20 years, used to define, effectively, no expiration on tokens
	// Used when user-provided token expiration time is zero
	ForeverTokenTime        = cos.Duration(20 * 365 * 24 * time.Hour)
	defaultRSAKeyBits       = minRSAKeyBits
	defaultLogFlushInterval = cos.Duration(30 * time.Second)
	defaultTimeout          = cos.Duration(30 * time.Second)
	defaultPort             = 52001
)

type (
	Config struct {
		Server  ServerConf  `json:"auth"`
		Log     LogConf     `json:"log"`
		Net     NetConf     `json:"net"`
		Timeout TimeoutConf `json:"timeout"`
	}
	LogConf struct {
		Dir           string       `json:"dir"`
		Level         string       `json:"level"`
		FlushInterval cos.Duration `json:"flush_interval"`
	}
	NetConf struct {
		ExternalURL string   `json:"external_url"`
		HTTP        HTTPConf `json:"http"`
	}
	HTTPConf struct {
		Certificate string `json:"server_crt"`
		Key         string `json:"server_key"`
		Port        int    `json:"port"`
		UseHTTPS    bool   `json:"use_https"`
	}
	ServerConf struct {
		psecret *string       `json:"-"`
		pexpire *cos.Duration `json:"-"`
		Secret  string        `json:"secret"`
		// Determines when the secret or key expires
		// Also used to determine max-age for client caches of JWKS
		Expire cos.Duration `json:"expiration_time"`
		PubKey *string      `json:"public_key"`
		// Size of RSA private key to generate
		RSAKeyBits int `json:"rsa_key_bits"`
	}

	// TimeoutConf sets the default timeout for the HTTP client used by the auth manager
	TimeoutConf struct {
		Default cos.Duration `json:"default_timeout"`
	}
	ConfigToUpdate struct {
		Server *ServerConfToSet `json:"auth"`
	}
	ServerConfToSet struct {
		Secret *string       `json:"secret,omitempty"`
		Expire *cos.Duration `json:"expiration_time,omitempty"`
	}
	// TokenList is a list of tokens pushed by authn
	TokenList struct {
		Tokens  []string `json:"tokens"`
		Version int64    `json:"version,string"`
	}
)

var (
	_ jsp.Opts = (*Config)(nil)

	authcfgJspOpts = jsp.Plain() // TODO: use CCSign(MetaverAuthNConfig)
	authtokJspOpts = jsp.Plain() // ditto MetaverTokens
)

func (*Config) JspOpts() jsp.Options { return authcfgJspOpts }

func (c *Config) Init() {
	c.Server.psecret = &c.Server.Secret
	c.Server.pexpire = &c.Server.Expire
}

func (c *Config) Verbose() bool {
	if c.Log.Level == "" {
		return false
	}
	level, err := strconv.Atoi(c.Log.Level)
	debug.AssertNoErr(err)
	return level > 3
}

func (c *Config) Secret() cmn.Censored  { return cmn.Censored(*c.Server.psecret) }
func (c *Config) Expire() time.Duration { return time.Duration(*c.Server.pexpire) }

func (c *Config) Validate() error {
	if err := c.Server.Validate(); err != nil {
		return err
	}
	if err := c.Log.Validate(); err != nil {
		return err
	}
	if err := c.Net.Validate(); err != nil {
		return err
	}
	return c.Timeout.Validate()
}

func (c *ServerConf) Validate() error {
	if c.Expire == 0 {
		c.Expire = ForeverTokenTime
	}
	if c.Expire < minAuthExpiration {
		return fmt.Errorf("invalid auth.expiration_time=%s, (expected 0 (infinite) or >= %s)", c.Expire, minAuthExpiration)
	}
	if c.RSAKeyBits == 0 {
		c.RSAKeyBits = defaultRSAKeyBits
	}
	if c.RSAKeyBits < minRSAKeyBits {
		return fmt.Errorf("invalid auth.rsa_key_bits=%d, (must be >= %d)", c.RSAKeyBits, minRSAKeyBits)
	}
	return nil
}

func (c *LogConf) Validate() error {
	if c.Level != "" {
		level, err := strconv.Atoi(c.Level)
		if err != nil {
			return fmt.Errorf("invalid log.level=%q (expected integer string)", c.Level)
		}
		if level < 0 || level > maxLogLevel {
			return fmt.Errorf("invalid log.level=%d (expected value between 0 and %d)", level, maxLogLevel)
		}
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = defaultLogFlushInterval
	}
	if c.FlushInterval < minLogFlushInterval {
		return fmt.Errorf("invalid log.flush_interval=%s (expected >= %s)", c.FlushInterval, minLogFlushInterval)
	}
	return nil
}

func (c *NetConf) Validate() error {
	return c.HTTP.Validate()
}

func (c *HTTPConf) Validate() error {
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.Port < minPort || c.Port > maxPort {
		return fmt.Errorf("invalid net.http.port=%d (expected %d-%d)", c.Port, minPort, maxPort)
	}
	if c.UseHTTPS {
		if c.Certificate == "" {
			return errors.New("net.http.server_crt required when use_https=true")
		}
		if c.Key == "" {
			return errors.New("net.http.server_key required when use_https=true")
		}
	}
	return nil
}

func (c *TimeoutConf) Validate() error {
	if c.Default == 0 {
		c.Default = defaultTimeout
	}
	if c.Default < minTimeout {
		return fmt.Errorf("invalid timeout.default_timeout=%s (expected >= %s)", c.Default, minTimeout)
	}
	return nil
}

func (cu *ConfigToUpdate) Validate() error {
	if cu.Server == nil {
		return errors.New("configuration is empty")
	}
	if cu.Server.Secret != nil && *cu.Server.Secret == "" {
		return errors.New("secret defined but empty string")
	}
	return nil
}
