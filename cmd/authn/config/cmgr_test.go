// Package config_test contains tests for the auth config package
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package config_test

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func newConfManagerWithConf(t *testing.T, c *authn.Config) *config.ConfManager {
	path := writeConfToDisk(t, c)
	cm := config.NewConfManager()
	cm.Init(path)
	return cm
}

// helper to create a minimal valid authn.Config that can be modified before use
func newConf() *authn.Config {
	return &authn.Config{
		Server: authn.ServerConf{
			Secret: "test-secret",
			Expire: cos.Duration(time.Hour),
		},
		Log: authn.LogConf{
			Dir:   "/tmp/authn-log",
			Level: "4",
		},
		Net: authn.NetConf{
			HTTP: authn.HTTPConf{
				Certificate: "/tmp/cert",
				Key:         "/tmp/key",
				Port:        12345,
				UseHTTPS:    true,
			},
		},
		Timeout: authn.TimeoutConf{
			Default: cos.Duration(time.Second),
		},
	}
}

// Create and initialize a base config
func newBaseConfig() *authn.Config {
	c := newConf()
	c.Init()
	return c
}

func writeConfToDisk(t *testing.T, c *authn.Config) string {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "authn.json")
	err := jsp.SaveMeta(path, c, nil)
	tassert.Fatalf(t, err == nil, "failed to write config: %v", err)
	return path
}

func compareSecret(t *testing.T, cm *config.ConfManager, expected string) {
	actual := string(cm.GetSecret())
	tassert.Errorf(t, actual == expected, "expected secret %q, got %q", expected, actual)
}

func compareExpiry(t *testing.T, cm *config.ConfManager, expected time.Duration) {
	actual := cm.GetExpiry()
	tassert.Errorf(t, actual == expected, "expected expire %v, got %v", expected, actual)
}

func validateHTTPConf(t *testing.T, cm *config.ConfManager, expected *authn.HTTPConf) {
	cert := cm.GetServerCert()
	tassert.Errorf(t, cert == expected.Certificate, "expected cert %q, got %q", expected.Certificate, cert)
	key := cm.GetServerKey()
	tassert.Errorf(t, key == expected.Key, "expected key %q, got %q", expected.Key, key)
}

func TestNewConfManagerAndGetConf(t *testing.T) {
	cm := config.NewConfManager()
	tassert.Fatal(t, cm.GetConf() != nil, "GetConf returned nil after NewConfManager")
}

func TestUpdateConf(t *testing.T) {
	cm := newConfManagerWithConf(t, newBaseConfig())

	// empty config
	err := cm.UpdateConf(&authn.ConfigToUpdate{})
	tassert.Error(t, err != nil, "expected error for empty ConfigToUpdate")

	// invalid secret
	empty := ""
	err = cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{Secret: &empty},
	})
	tassert.Error(t, err != nil, "expected error for empty secret in config update")

	// valid update
	newSecret := "new-secret"
	newExpire := cos.Duration(2 * time.Hour)
	err = cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{
			Secret: &newSecret,
			Expire: &newExpire,
		},
	})
	tassert.Fatalf(t, err == nil, "UpdateConf failed: %v", err)
	compareSecret(t, cm, newSecret)
	compareExpiry(t, cm, 2*time.Hour)
}

func TestUpdateConfNoExpiry(t *testing.T) {
	cm := newConfManagerWithConf(t, newBaseConfig())

	// valid update
	newSecret := "new-secret"
	err := cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{
			Secret: &newSecret,
		},
	})
	tassert.Fatalf(t, err == nil, "UpdateConf failed: %v", err)
	compareSecret(t, cm, newSecret)
	actual := cm.GetExpiry()
	tassert.Errorf(t, actual > 0, "expected expire to be set to some positive default, got %v", actual)
}

func TestInitEnv(t *testing.T) {
	c := newConf()
	c.Server.Secret = ""
	c.Init()
	path := writeConfToDisk(t, c)

	const envSecret = "env-secret-only"
	t.Setenv(env.AisAuthSecretKey, envSecret)

	cm := config.NewConfManager()
	cm.Init(path)
	compareSecret(t, cm, envSecret)
}

func TestInitFromDisk(t *testing.T) {
	// Clear env var set in pipeline for first write -- use value from disk
	t.Setenv(env.AisAuthSecretKey, "")
	base := newBaseConfig()
	path := writeConfToDisk(t, base)
	cm := config.NewConfManager()
	cm.Init(path)
	compareSecret(t, cm, base.Server.Secret)
	compareExpiry(t, cm, base.Expire())

	// override secret via env
	const envSecret = "env-secret"
	t.Setenv(env.AisAuthSecretKey, envSecret)

	cm.Init(path)
	compareSecret(t, cm, envSecret)
}

func TestGetDBType(t *testing.T) {
	base := newBaseConfig()
	path := writeConfToDisk(t, base)
	cm := config.NewConfManager()
	cm.Init(path)
	tassert.Errorf(t, cm.GetDBType() == "", "expected empty db type, got %s", cm.GetDBType())
	// Test DB from config is loaded
	expected := "customDB"
	base.Server.DBConf = authn.DatabaseConf{DBType: expected}
	path = writeConfToDisk(t, base)
	cm2 := config.NewConfManager()
	cm2.Init(path)
	got := cm2.GetDBType()
	tassert.Errorf(t, got == expected, "expected type %s, got %s", expected, got)
}

func TestGetDBPath(t *testing.T) {
	base := newBaseConfig()
	path := writeConfToDisk(t, base)
	cm := config.NewConfManager()
	cm.Init(path)
	expected := filepath.Join(filepath.Dir(path), fname.AuthNDB)
	got := cm.GetDBPath()
	tassert.Errorf(t, got == expected, "expected %q, got %q", expected, got)
	// Test with custom path
	expected = "/custom/path/file.db"
	base.Server.DBConf = authn.DatabaseConf{Filepath: expected}
	path = writeConfToDisk(t, base)
	cm2 := config.NewConfManager()
	cm2.Init(path)
	got = cm2.GetDBPath()
	tassert.Errorf(t, got == expected, "expected %q, got %q", expected, got)
}

func TestGetLogFlushInterval(t *testing.T) {
	base := newBaseConfig()
	expected := 20 * time.Second
	base.Log.FlushInterval = cos.Duration(expected)
	cm := newConfManagerWithConf(t, base)

	got := cm.GetLogFlushInterval()
	tassert.Errorf(t, got == expected, "expected %v, got %v", expected, got)
}

func TestParseExternalURL_Env(t *testing.T) {
	urlStr := "https://example.com:8443/base"
	t.Setenv(env.AisAuthExternalURL, urlStr)
	// Not set in base
	cm := newConfManagerWithConf(t, newBaseConfig())
	u, err := cm.ParseExternalURL()
	tassert.Fatalf(t, err == nil, "expected parse with no error, got: %v", err)
	tassert.Errorf(t, u.String() == urlStr, "expected URL %q, got %q", urlStr, u.String())
}

func TestParseExternalURL_Conf(t *testing.T) {
	urlStr := "https://example.com:8443/base"
	base := newBaseConfig()
	base.Net.ExternalURL = urlStr
	cm := newConfManagerWithConf(t, base)
	u, err := cm.ParseExternalURL()
	tassert.Fatalf(t, err == nil, "expected parse with no error, got: %v", err)
	tassert.Errorf(t, u.String() == urlStr, "expected URL %q, got %q", urlStr, u.String())
}

func TestParseExternalURL_FallbackHTTP(t *testing.T) {
	// First, when not using https
	expectedURL := "http://localhost:12345"
	base := newBaseConfig()
	base.Net.HTTP.UseHTTPS = false
	cm := newConfManagerWithConf(t, base)
	u, err := cm.ParseExternalURL()
	tassert.Fatalf(t, err == nil, "expected parse with no error, got: %v", err)
	tassert.Errorf(t, u.String() == expectedURL, "expected URL %q, got %q", expectedURL, u.String())
}

func TestParseExternalURL_FallbackHTTPS(t *testing.T) {
	// First, when not using https
	expectedURL := "https://localhost:12345"
	base := newBaseConfig()
	cm := newConfManagerWithConf(t, base)
	u, err := cm.ParseExternalURL()
	tassert.Fatalf(t, err == nil, "expected parse with no error, got: %v", err)
	tassert.Errorf(t, u.String() == expectedURL, "expected URL %q, got %q", expectedURL, u.String())
}

func TestIsHTTPS(t *testing.T) {
	base := newBaseConfig()
	base.Net.HTTP.UseHTTPS = true
	cm := newConfManagerWithConf(t, base)
	tassert.Errorf(t, cm.IsHTTPS(), "expected IsHTTPS true when set in config")

	base.Net.HTTP.UseHTTPS = false
	cm = newConfManagerWithConf(t, base)
	tassert.Errorf(t, !cm.IsHTTPS(), "expected IsHTTPS false when config says false and env unset")

	t.Setenv(env.AisAuthUseHTTPS, "true")
	tassert.Errorf(t, cm.IsHTTPS(), "expected IsHTTPS true when env is true")
}

func TestGetServerCertAndKey(t *testing.T) {
	confCert := "/default/cert"
	confKey := "/default/key"
	base := newBaseConfig()
	expectedHTTP := &authn.HTTPConf{
		Certificate: confCert,
		Key:         confKey,
		Port:        52001,
		UseHTTPS:    true,
	}
	base.Net.HTTP = *expectedHTTP

	// Use http settings from config
	cm := newConfManagerWithConf(t, base)
	validateHTTPConf(t, cm, expectedHTTP)

	envCert := "/env/cert"
	envKey := "/env/key"
	t.Setenv(env.AisAuthServerCrt, envCert)
	t.Setenv(env.AisAuthServerKey, envKey)
	expectedHTTP.Certificate = envCert
	expectedHTTP.Key = envKey
	// Without updating config, env vars should override when getting through config manager
	validateHTTPConf(t, cm, expectedHTTP)
}

func TestGetPort(t *testing.T) {
	base := newBaseConfig()
	base.Net.HTTP.Port = 52001
	cm := newConfManagerWithConf(t, base)

	// default from config
	port := cm.GetPort()
	tassert.Errorf(t, port == "52001", "expected 52001, got %q", port)

	// env override
	t.Setenv(env.AisAuthPort, "9999")
	port = cm.GetPort()
	tassert.Errorf(t, cm.GetPort() == "9999", "expected 9999, got %q", port)
}

func TestIsVerbose(t *testing.T) {
	base := newBaseConfig()
	base.Log.Level = "4"
	cm := newConfManagerWithConf(t, base)
	tassert.Errorf(t, cm.IsVerbose(), "expected IsVerbose true for level 4")

	base.Log.Level = "1"
	cm = newConfManagerWithConf(t, base)
	tassert.Errorf(t, !cm.IsVerbose(), "expected IsVerbose false for level 1")
}

func TestGetSigConf(t *testing.T) {
	// HMAC path -- default base config here has a secret set
	base := newBaseConfig()
	cm := newConfManagerWithConf(t, base)
	sig := cm.GetSigConf()
	tassert.Fatalf(t, sig.Method == cmn.SigMethodHMAC, "expected HMAC method, got %v", sig.Method)
	expectedSec := base.Server.Secret
	tassert.Fatalf(t, string(sig.Key) == expectedSec, "expected key %q, got %q", expectedSec, sig.Key)
}

func TestGetSigConfEmpty(t *testing.T) {
	// If no HMAC secret is configured, return nil signature config
	c := newConf()
	c.Server.Secret = ""
	c.Init()
	cm := newConfManagerWithConf(t, c)
	sig := cm.GetSigConf()
	tassert.Fatalf(t, sig == nil, "expected nil signature conf, got %v", sig)
}

func TestGetExpiryAndDefaultTimeout(t *testing.T) {
	base := newBaseConfig()
	base.Server.Expire = cos.Duration(3 * time.Hour)
	cm := newConfManagerWithConf(t, base)

	exp := cm.GetExpiry()
	tassert.Errorf(t, exp == 3*time.Hour, "expected expiry 3h, got %v", exp)
}

func TestGetTimeout(t *testing.T) {
	base := newBaseConfig()
	confTimeout := 10 * time.Second
	base.Timeout.Default = cos.Duration(confTimeout)
	cm := newConfManagerWithConf(t, base)
	timeout := cm.GetDefaultTimeout()
	tassert.Errorf(t, timeout == confTimeout, "expected default timeout, got %v", timeout)
}

func TestHMACSecret(t *testing.T) {
	base := newBaseConfig()
	base.Server.Secret = "some-secret"
	cm := newConfManagerWithConf(t, base)
	tassert.Fatal(t, cm.HasHMACSecret(), "expected HasHMACSecret true")
	tassert.Fatal(t, cm.GetSecret() != "", "expected non-empty secret")
	base.Server.Secret = ""
	cm = newConfManagerWithConf(t, base)
	tassert.Fatal(t, !cm.HasHMACSecret(), "expected HasHMACSecret false")
	tassert.Fatal(t, cm.GetSecret() == "", "expected empty secret")
}

func TestGetSecretChecksum(t *testing.T) {
	base := newBaseConfig()
	base.Server.Secret = "some-secret"
	cm := newConfManagerWithConf(t, base)

	cs1 := cm.GetSecretChecksum()
	tassert.Fatal(t, cs1 != "", "expected non-empty checksum")

	base.Server.Secret = "other-secret"
	cm = newConfManagerWithConf(t, base)
	cs2 := cm.GetSecretChecksum()
	tassert.Fatal(t, cs1 != cs2, "expected checksum to change when secret changes")
}

// TestAuth prefix ensures we run as part of the auth tests with TEST_RACE true to run with go test -race
func TestAuthConfManagerConcurrency(t *testing.T) {
	cm := newConfManagerWithConf(t, newBaseConfig())

	var started sync.WaitGroup
	var done sync.WaitGroup
	var ops atomic.Int64

	started.Add(1)
	done.Add(1)

	go func() {
		defer done.Done()
		// signal that the goroutine has started and cm is being read
		started.Done()

		for range 1000 {
			cm.GetConf()
			cm.HasHMACSecret()
			cm.GetExpiry()
			ops.Add(1)
		}
	}()

	// wait until the goroutine is in its loop
	started.Wait()

	// update fields individually while reads are happening
	newSecret := "updated"
	if err := cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{Secret: &newSecret},
	}); err != nil {
		t.Fatalf("UpdateConf failed in concurrent test: %v", err)
	}
	newExpiry := cos.Duration(5 * time.Hour)

	if err := cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{Expire: &newExpiry},
	}); err != nil {
		t.Fatalf("UpdateConf failed in concurrent test: %v", err)
	}

	// wait for the reader goroutine to finish
	done.Wait()

	// Secret should be updated
	compareSecret(t, cm, newSecret)
	compareExpiry(t, cm, 5*time.Hour)

	if ops.Load() == 0 {
		t.Fatal("expected some concurrent operations")
	}
}
