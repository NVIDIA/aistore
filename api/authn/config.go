// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// Constraints
const (
	MinAuthExpiration = cos.Duration(time.Minute) // minimum JWT lifetime
	// minPort avoids 0-1023 system-reserved port range
	minPort             = 1024
	maxPort             = 65535
	minTimeout          = cos.Duration(time.Second)
	minLogFlushInterval = cos.Duration(10 * time.Second)
	maxLogLevel         = 5
)

// Defaults
const (
	defaultLogFlushInterval = cos.Duration(30 * time.Second)
	defaultTimeout          = cos.Duration(30 * time.Second)
	defaultPort             = 52001
	defaultTokenExpiration  = cos.Duration(24 * time.Hour)
	defaultMaxTokenAge      = cos.Duration(90 * 24 * time.Hour)
)

// Signing key management modes
const (
	// SigningKeyModeExternal signals that the signing key pair is managed outside this process
	// (e.g. mounted from a secret manager or HSM). Auto-generation and API-driven
	// rotation are disabled; a missing key file is a fatal configuration error.
	SigningKeyModeExternal = "external"
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
		psecret *string `json:"-"`
		Secret  string  `json:"secret"`
		// Default lifetime for issued JWT
		Expire cos.Duration `json:"expiration_time"`
		// Max JWT lifetime
		MaxTokenAge cos.Duration `json:"max_token_age"`
		// Only used for validating signing key public key against AIS clusters
		PubKey *string `json:"public_key"`
		// Deprecated: use signing_key.bits instead.
		RSAKeyBits int `json:"rsa_key_bits,omitempty"`
		// Config for key signing -- currently only RSA is supported
		SigningKey SigningKeyConf `json:"signing_key"`
		// Config for authN database
		DBConf DatabaseConf `json:"db"`
	}
	SigningKeyConf struct {
		Bits int `json:"bits,omitempty"`
		// "external": key is managed outside this process; auto-generation and API rotation are disabled
		Mode string `json:"mode,omitempty"`
	}
	DatabaseConf struct {
		DBType   string `json:"type"`
		Filepath string `json:"filepath"`
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
}

func (c *Config) Verbose() bool {
	if c.Log.Level == "" {
		return false
	}
	level, err := strconv.Atoi(c.Log.Level)
	debug.AssertNoErr(err)
	return level > 3
}

func (c *Config) Secret() cmn.Censored {
	return cmn.Censored(*c.Server.psecret)
}

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
	if c.MaxTokenAge == 0 {
		c.MaxTokenAge = defaultMaxTokenAge
	}
	if c.SigningKey.Bits == 0 && c.RSAKeyBits != 0 {
		c.SigningKey.Bits = c.RSAKeyBits
	}
	if err := c.SigningKey.validate(); err != nil {
		return err
	}
	if c.Expire == 0 {
		c.Expire = defaultTokenExpiration
	}
	if c.Expire < MinAuthExpiration {
		return fmt.Errorf("invalid auth.expiration_time=%s, expected 0 (default 24h) or >= %s", c.Expire, MinAuthExpiration)
	}
	if c.MaxTokenAge < MinAuthExpiration {
		return fmt.Errorf("invalid auth.max_token_age=%s, expected >= %s", c.MaxTokenAge, MinAuthExpiration)
	}
	if c.Expire > c.MaxTokenAge {
		return fmt.Errorf("invalid config: auth.expiration_time=%s cannot exceed auth.max_token_age=%s", c.Expire, c.MaxTokenAge)
	}
	return nil
}

func (c *SigningKeyConf) validate() error {
	if c.Bits == 0 {
		c.Bits = cos.RSAKeyDefaultBits
	}
	if c.Bits < cos.RSAKeyMinBits {
		return fmt.Errorf("invalid auth.signing_key.bits=%d (must be >= %d)", c.Bits, cos.RSAKeyMinBits)
	}
	if c.Mode != "" && c.Mode != SigningKeyModeExternal {
		return fmt.Errorf("invalid auth.signing_key.mode=%q (valid values: %q or empty)", c.Mode, SigningKeyModeExternal)
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
	if c.ExternalURL != "" {
		if _, err := ParseExternalURL(c.ExternalURL); err != nil {
			return err
		}
	}
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

func ParseExternalURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("external URL %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("external URL %q: missing host", raw)
	}
	return u, nil
}
