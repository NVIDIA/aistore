// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package authn

import (
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"
)

func validConfig() *Config {
	return &Config{
		Server:  ServerConf{Secret: "test-secret", Expire: minAuthExpiration},
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

	// zero RSAKeyBits gets default
	c = validConfig()
	c.Server.RSAKeyBits = 0
	tassert.CheckFatal(t, c.Validate())
	tassert.Errorf(t, c.Server.RSAKeyBits == defaultRSAKeyBits, "expected RSAKeyBits default, got %d", c.Server.RSAKeyBits)
}

func TestConfigValidateInvalid(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Config)
	}{
		{"expire too short", func(c *Config) { c.Server.Expire = minAuthExpiration - 1 }},
		{"RSA bits too small", func(c *Config) { c.Server.RSAKeyBits = minRSAKeyBits - 1 }},
		{"bad log level", func(c *Config) { c.Log.Level = "verbose" }},
		{"log level too low", func(c *Config) { c.Log.Level = "-1" }},
		{"log level too high", func(c *Config) { c.Log.Level = "6" }},
		{"flush interval too short", func(c *Config) { c.Log.FlushInterval = minLogFlushInterval - 1 }},
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
