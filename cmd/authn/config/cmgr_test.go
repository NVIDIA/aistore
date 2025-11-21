// Package config_test contains tests for the auth config package
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package config_test

import (
	"errors"
	"os"
	"os/exec"
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
	"github.com/NVIDIA/aistore/tools/tassert"
)

// helper to create a minimal valid authn.Config
func newBaseConfig() *authn.Config {
	c := &authn.Config{
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
	c.Init()
	return c
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

func TestNewConfManagerWithConf(t *testing.T) {
	base := newBaseConfig()
	cm := config.NewConfManagerWithConf(base)
	got := cm.GetConf()
	tassert.Fatal(t, got != nil, "GetConf returned nil")
	tassert.Fatalf(t, got.Server.Secret == base.Server.Secret, "expected secret %q, got %q", base.Server.Secret, got.Server.Secret)
}

func TestUpdateConf(t *testing.T) {
	base := newBaseConfig()
	cm := config.NewConfManagerWithConf(base)

	// empty config
	err := cm.UpdateConf(&authn.ConfigToUpdate{})
	tassert.Error(t, err != nil, "expected error for empty ConfigToUpdate")

	// invalid secret
	empty := ""
	err = cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{Secret: &empty},
	})
	tassert.Error(t, err != nil, "expected error for empty secret in config update")

	// invalid duration
	exp := "not-a-duration"
	err = cm.UpdateConf(&authn.ConfigToUpdate{
		Server: &authn.ServerConfToSet{Expire: &exp},
	})
	tassert.Error(t, err != nil, "expected error for invalid duration in config update")

	// valid update
	newSecret := "new-secret"
	newExpire := "2h"
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

func TestInitEnv(t *testing.T) {
	cm := config.NewConfManager()

	const envSecret = "env-secret-only"
	t.Setenv(env.AisAuthSecretKey, envSecret)

	cm.Init("") // should not attempt to load from disk
	compareSecret(t, cm, envSecret)
}

func TestInitNoSecret(t *testing.T) {
	if os.Getenv("TEST_INIT_NO_SECRET") == "1" {
		// Init with no secret should exit via cos.ExitLogf
		cm := config.NewConfManager()
		// no config dir, no env secret
		cm.Init("")
		t.Fatal("Init did not exit as expected")
	}
	// Run init in a child process to catch fatal exit
	cmd := exec.Command(os.Args[0], "-test.run=TestInitNoSecret")
	cmd.Env = append(os.Environ(), "TEST_INIT_NO_SECRET=1")

	err := cmd.Run()
	tassert.Fatal(t, err != nil, "expected non-zero exit code")
	var exitErr *exec.ExitError
	ok := errors.As(err, &exitErr)
	tassert.Fatalf(t, ok && exitErr.ExitCode() != 0, "expected process to exit with non-zero code, got %v", err)
}

func TestInitFromDisk(t *testing.T) {
	// Clear env var set in pipeline
	t.Setenv(env.AisAuthSecretKey, "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, fname.AuthNConfig)
	base := newBaseConfig()

	// write (uninitialized) base config via ConfManager.SaveToDisk
	cmSave := config.NewConfManagerWithConf(base)
	if err := cmSave.SaveToDisk(configPath); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	// override secret via env
	const envSecret = "env-secret"
	t.Setenv(env.AisAuthSecretKey, envSecret)

	cm := config.NewConfManager()
	cm.Init(dir)
	compareSecret(t, cm, envSecret)
	compareExpiry(t, cm, base.Expire())
}

func TestGetLogDir(t *testing.T) {
	base := newBaseConfig()
	base.Log.Dir = "/default/log"
	cm := config.NewConfManagerWithConf(base)

	// default
	got := cm.GetLogDir()
	tassert.Errorf(t, got == base.Log.Dir, "expected %q, got %q", base.Log.Dir, got)

	// env override
	override := "/env/log"
	t.Setenv(env.AisAuthLogDir, override)
	got = cm.GetLogDir()
	tassert.Errorf(t, got == override, "expected %q, got %q", override, got)
}

func TestIsHTTPS(t *testing.T) {
	base := newBaseConfig()
	base.Net.HTTP.UseHTTPS = true
	cm := config.NewConfManagerWithConf(base)
	tassert.Errorf(t, cm.IsHTTPS(), "expected IsHTTPS true when set in config")

	base.Net.HTTP.UseHTTPS = false
	cm = config.NewConfManagerWithConf(base)
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
	cm := config.NewConfManagerWithConf(base)
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
	cm := config.NewConfManagerWithConf(base)

	// default from config
	port := cm.GetPort()
	tassert.Errorf(t, port == ":52001", "expected :52001, got %q", port)

	// env override
	t.Setenv(env.AisAuthPort, "9999")
	port = cm.GetPort()
	tassert.Errorf(t, cm.GetPort() == ":9999", "expected :9999, got %q", port)
}

func TestIsVerbose(t *testing.T) {
	base := newBaseConfig()
	base.Log.Level = "4"
	cm := config.NewConfManagerWithConf(base)
	tassert.Errorf(t, cm.IsVerbose(), "expected IsVerbose true for level 4")

	base.Log.Level = "1"
	cm = config.NewConfManagerWithConf(base)
	tassert.Errorf(t, !cm.IsVerbose(), "expected IsVerbose false for level 1")
}

func TestGetSigConf(t *testing.T) {
	base := newBaseConfig()
	// HMAC path
	cm := config.NewConfManagerWithConf(base)
	sig, err := cm.GetSigConf()
	tassert.Fatalf(t, err == nil, "GetSigConf failed: %v", err)
	tassert.Fatalf(t, sig.Method == cmn.SigMethodHMAC, "expected HMAC method, got %v", sig.Method)
	expectedSec := base.Server.Secret
	tassert.Fatalf(t, string(sig.Key) == expectedSec, "expected key %q, got %q", expectedSec, sig.Key)

	// RSA path: no secret, but PubKey set
	base.Server.Secret = ""
	pub := "public-key"
	base.Server.PubKey = &pub
	base.Init()
	cm = config.NewConfManagerWithConf(base)
	sig, err = cm.GetSigConf()
	tassert.Fatalf(t, err == nil, "GetSigConf failed for RSA: %v", err)
	tassert.Fatalf(t, sig.Method == cmn.SigMethodRSA, "expected RSA method, got %v", sig.Method)
	tassert.Fatalf(t, string(sig.Key) == pub, "expected key %q, got %q", pub, sig.Key)

	// invalid: neither secret nor pubkey
	base.Server.PubKey = nil
	base.Server.Secret = ""
	base.Init()
	cm = config.NewConfManagerWithConf(base)
	_, err = cm.GetSigConf()
	tassert.Error(t, err != nil, "expected error when no secret or pubkey")
}

func TestGetExpiryAndDefaultTimeout(t *testing.T) {
	base := newBaseConfig()
	base.Server.Expire = cos.Duration(3 * time.Hour)
	base.Init()
	cm := config.NewConfManagerWithConf(base)

	exp := cm.GetExpiry()
	tassert.Errorf(t, exp == 3*time.Hour, "expected expiry 3h, got %v", exp)
}

func TestGetTimeout(t *testing.T) {
	base := newBaseConfig()
	confTimeout := 10 * time.Second
	base.Timeout.Default = cos.Duration(confTimeout)
	cm := config.NewConfManagerWithConf(base)
	timeout := cm.GetDefaultTimeout()
	tassert.Errorf(t, timeout == confTimeout, "expected default timeout, got %v", timeout)
}

func TestHMACSecret(t *testing.T) {
	base := newBaseConfig()
	base.Server.Secret = "some-secret"
	base.Init()
	cm := config.NewConfManagerWithConf(base)
	tassert.Fatal(t, cm.HasHMACSecret(), "expected HasHMACSecret true")
	tassert.Fatal(t, cm.GetSecret() != "", "expected non-empty secret")
	base.Server.Secret = ""
	base.Init()
	cm = config.NewConfManagerWithConf(base)
	tassert.Fatal(t, !cm.HasHMACSecret(), "expected HasHMACSecret false")
	tassert.Fatal(t, cm.GetSecret() == "", "expected empty secret")
}

func TestGetSecretChecksum(t *testing.T) {
	base := newBaseConfig()
	base.Server.Secret = "some-secret"
	base.Init()
	cm := config.NewConfManagerWithConf(base)

	// checksum should be deterministic and non-empty
	cs1 := cm.GetSecretChecksum()
	tassert.Fatal(t, cs1 != "", "expected non-empty checksum")

	// changing secret changes checksum
	base.Server.Secret = "other-secret"
	base.Init()
	cm = config.NewConfManagerWithConf(base)
	cs2 := cm.GetSecretChecksum()
	tassert.Fatal(t, cs1 != cs2, "expected checksum to change when secret changes")
}

func TestGetPublicKeyString(t *testing.T) {
	base := newBaseConfig()
	if cm := config.NewConfManagerWithConf(base); cm.GetPublicKeyString() != nil {
		t.Fatal("expected nil PubKey when not set")
	}

	pub := "public-key"
	base.Server.PubKey = &pub
	base.Init()
	cm := config.NewConfManagerWithConf(base)
	if got := cm.GetPublicKeyString(); got == nil || *got != pub {
		t.Fatalf("expected PubKey %q, got %#v", pub, got)
	}
}

// TestAuth prefix ensures we run as part of the auth tests with TEST_RACE true to run with go test -race
func TestAuthConfManagerConcurrency(t *testing.T) {
	base := newBaseConfig()
	cm := config.NewConfManagerWithConf(base)

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
	newExpiry := "5h"

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
