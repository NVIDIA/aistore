// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func validConfig() *Config {
	return &Config{
		Server:  ServerConf{Secret: "test-secret", Expire: MinAuthExpiration},
		Log:     LogConf{Level: "3", FlushInterval: minLogFlushInterval},
		Net:     NetConf{HTTP: HTTPConf{Port: minPort}},
		Timeout: TimeoutConf{Default: minTimeout},
	}
}

func TestConfigValidateValid(t *testing.T) {
	tassert.CheckFatal(t, validConfig().Validate())

	// HTTPS with cert and key
	c := validConfig()
	c.Net.HTTP.UseHTTPS = true
	c.Net.HTTP.Certificate = "/path/to/cert"
	c.Net.HTTP.Key = "/path/to/key"
	tassert.CheckFatal(t, c.Validate())

	// zero expire gets default
	c = validConfig()
	c.Server.Expire = 0
	tassert.CheckFatal(t, c.Validate())
	tassert.Errorf(t, c.Server.Expire > 0, "expected Expire default, got %v", c.Server.Expire)

	// zero FlushInterval gets default
	c = validConfig()
	c.Log.FlushInterval = 0
	tassert.CheckFatal(t, c.Validate())
	tassert.Errorf(t, c.Log.FlushInterval == defaultLogFlushInterval, "expected FlushInterval default, got %v", c.Log.FlushInterval)

	// zero SigningKey.Bits gets default
	c = validConfig()
	c.Server.SigningKey.Bits = 0
	tassert.CheckFatal(t, c.Validate())
	tassert.Errorf(t, c.Server.SigningKey.Bits == cos.RSAKeyDefaultBits, "expected SigningKey.Bits default, got %d", c.Server.SigningKey.Bits)

	// external mode is accepted
	c = validConfig()
	c.Server.SigningKey.Mode = SigningKeyModeExternal
	tassert.CheckFatal(t, c.Validate())

	// valid external url is parsed
	c = validConfig()
	c.Net.ExternalURL = "https://auth.example.com:8443"
	tassert.CheckFatal(t, c.Validate())
}

func TestServerConfValidate(t *testing.T) {
	t.Run("MaxTokenAgeDefaulted", func(t *testing.T) {
		sc := ServerConf{Secret: "s", Expire: cos.Duration(time.Hour)}
		tassert.CheckFatal(t, sc.Validate())
		tassert.Errorf(t, sc.MaxTokenAge == defaultMaxTokenAge,
			"expected MaxTokenAge default %v, got %v", defaultMaxTokenAge, sc.MaxTokenAge)
	})
	t.Run("MaxTokenAgePreserved", func(t *testing.T) {
		custom := cos.Duration(7 * 24 * time.Hour)
		sc := ServerConf{Secret: "s", Expire: cos.Duration(time.Hour), MaxTokenAge: custom}
		tassert.CheckFatal(t, sc.Validate())
		tassert.Errorf(t, sc.MaxTokenAge == custom,
			"expected MaxTokenAge %v, got %v", custom, sc.MaxTokenAge)
	})
	t.Run("ExpireExceedsMaxTokenAge", func(t *testing.T) {
		sc := ServerConf{Secret: "s", Expire: cos.Duration(48 * time.Hour), MaxTokenAge: cos.Duration(24 * time.Hour)}
		tassert.Errorf(t, sc.Validate() != nil, "expected error when Expire > MaxTokenAge")
	})
	t.Run("ExpireEqualsMaxTokenAge", func(t *testing.T) {
		sc := ServerConf{Secret: "s", Expire: cos.Duration(24 * time.Hour), MaxTokenAge: cos.Duration(24 * time.Hour)}
		tassert.CheckFatal(t, sc.Validate())
	})
	t.Run("ExpireWithinMaxTokenAge", func(t *testing.T) {
		sc := ServerConf{Secret: "s", Expire: cos.Duration(time.Hour), MaxTokenAge: cos.Duration(48 * time.Hour)}
		tassert.CheckFatal(t, sc.Validate())
	})
}

func TestSigningKeyConfValidate(t *testing.T) {
	t.Run("BitsDefaulted", func(t *testing.T) {
		conf := SigningKeyConf{}
		tassert.CheckFatal(t, conf.validate())
		tassert.Errorf(t, conf.Bits == cos.RSAKeyDefaultBits,
			"expected SigningKey.Bits default %d, got %d", cos.RSAKeyDefaultBits, conf.Bits)
	})
	t.Run("BitsPreserved", func(t *testing.T) {
		conf := SigningKeyConf{Bits: cos.RSAKeyMinBits + 1024}
		tassert.CheckFatal(t, conf.validate())
		tassert.Errorf(t, conf.Bits == cos.RSAKeyMinBits+1024,
			"expected SigningKey.Bits %d, got %d", cos.RSAKeyMinBits+1024, conf.Bits)
	})
	t.Run("BitsTooSmall", func(t *testing.T) {
		conf := SigningKeyConf{Bits: cos.RSAKeyMinBits - 1}
		tassert.Errorf(t, conf.validate() != nil, "expected error when signing key bits < minimum")
	})
	t.Run("ExternalModeAccepted", func(t *testing.T) {
		conf := SigningKeyConf{Mode: SigningKeyModeExternal}
		tassert.CheckFatal(t, conf.validate())
	})
	t.Run("InvalidMode", func(t *testing.T) {
		conf := SigningKeyConf{Mode: "auto"}
		tassert.Errorf(t, conf.validate() != nil, "expected error for invalid signing key mode")
	})
}

func TestServerConfValidateLegacyRSAKeyBits(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{
			name: "LegacyOnly",
			raw:  `{"secret":"test-secret","rsa_key_bits":4096}`,
			want: 4096,
		},
		{
			name: "SigningKeyOverridesLegacy",
			raw:  `{"secret":"test-secret","rsa_key_bits":4096,"signing_key":{"bits":3072}}`,
			want: 3072,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var conf ServerConf
			tassert.CheckFatal(t, cos.JSON.Unmarshal([]byte(tt.raw), &conf))
			tassert.CheckFatal(t, conf.Validate())
			tassert.Errorf(t, conf.SigningKey.Bits == tt.want,
				"expected SigningKey.Bits %d, got %d", tt.want, conf.SigningKey.Bits)
		})
	}
}

func TestConfigValidateInvalid(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Config)
	}{
		{"expire too short", func(c *Config) { c.Server.Expire = MinAuthExpiration - 1 }},
		{"signing key bits too small", func(c *Config) { c.Server.SigningKey.Bits = cos.RSAKeyMinBits - 1 }},
		{"signing key mode invalid", func(c *Config) { c.Server.SigningKey.Mode = "auto" }},
		{"bad log level", func(c *Config) { c.Log.Level = "verbose" }},
		{"log level too low", func(c *Config) { c.Log.Level = "-1" }},
		{"log level too high", func(c *Config) { c.Log.Level = "6" }},
		{"flush interval too short", func(c *Config) { c.Log.FlushInterval = minLogFlushInterval - 1 }},
		{"external URL no scheme", func(c *Config) { c.Net.ExternalURL = "example.com:8443" }},
		{"external URL bad scheme", func(c *Config) { c.Net.ExternalURL = "ftp://example.com" }},
		{"external URL no host", func(c *Config) { c.Net.ExternalURL = "http://" }},
		{"port too low", func(c *Config) { c.Net.HTTP.Port = minPort - 1 }},
		{"port too high", func(c *Config) { c.Net.HTTP.Port = maxPort + 1 }},
		{"HTTPS without cert", func(c *Config) { c.Net.HTTP.UseHTTPS = true }},
		{"HTTPS without key", func(c *Config) { c.Net.HTTP.UseHTTPS = true; c.Net.HTTP.Certificate = "/cert" }},
		{"timeout too short", func(c *Config) { c.Timeout.Default = minTimeout - 1 }},
	}
	for _, tc := range cases {
		c := validConfig()
		tc.modify(c)
		tassert.Errorf(t, c.Validate() != nil, "%s: expected error", tc.name)
	}
}
