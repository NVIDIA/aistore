// Package config_test contains tests for the auth config package
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLoadFromDisk_InvalidContent(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, fname.AuthNConfig)
	err := os.WriteFile(path, []byte("not valid json"), 0o644)
	tassert.CheckFatal(t, err)

	cm := &ConfManager{filePath: path}
	_, err = cm.loadFromDisk()
	tassert.Fatal(t, err != nil, "expected error when loading invalid JSON")
}

func TestLoadFromDisk_Success(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, fname.AuthNConfig)

	cfg := &authn.Config{}
	cfg.Init()

	err := jsp.SaveMeta(path, cfg, nil)
	tassert.Fatalf(t, err == nil, "failed to write config: %v", err)

	cm := NewConfManager()
	cm.filePath = path
	got, err := cm.loadFromDisk()
	tassert.Fatalf(t, err == nil, "unexpected error loading config from disk: %v", err)
	tassert.Fatal(t, got != nil, "expected non-nil config")
}

func TestResolveConfigPath_Invalid(t *testing.T) {
	_, err := resolveConfigPath("")
	tassert.Fatalf(t, err != nil, "expected error for missing path arg")
	_, err = resolveConfigPath("nonexistent")
	tassert.Fatalf(t, err != nil, "expected error for invalid path")
}

// This tests otherwise unreachable code when saving and loading valid config
func TestGetSigConf_Invalid(t *testing.T) {
	// start with a base that has no secret set
	base := &authn.Config{
		Server: authn.ServerConf{},
	}
	base.Init()
	// special behavior here -- run without config manager init to avoid RSA creation
	cm := NewConfManager()
	cm.conf.Store(base)
	// this config manager is invalid: has neither secret nor pubKey
	_, err := cm.GetSigConf()
	tassert.Error(t, err != nil, "expected error when no secret or pubKey")
}

func TestValidatePassphrase(t *testing.T) {
	tests := []struct {
		name      string
		envValue  string
		envSet    bool
		wantPass  string
		wantError bool
	}{
		{
			name:      "env not set",
			envSet:    false,
			wantPass:  "",
			wantError: false,
		},
		{
			name:      "env set empty",
			envSet:    true,
			wantPass:  "",
			wantError: true,
		},
		{
			name:      "minimum length",
			envValue:  "word123!",
			envSet:    true,
			wantError: false,
		},
		{
			name:      "low entropy",
			envValue:  "aaa bbb ccc",
			envSet:    true,
			wantError: true,
		},
		{
			name:      "with space",
			envValue:  "my pass phrase",
			envSet:    true,
			wantError: false,
		},
		{
			name:      "with tab",
			envValue:  "valid\tpass",
			envSet:    true,
			wantError: true,
		},
		{
			name:      "with newline",
			envValue:  "valid\npass",
			envSet:    true,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSet {
				t.Setenv("AIS_AUTHN_PRIVATE_KEY_PASS", tt.envValue)
			}

			gotPass, err := validatePassphrase()

			if tt.wantError {
				tassert.Errorf(t, err != nil, "expected error for passphrase %q", tt.envValue)
			} else {
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, string(gotPass) == tt.envValue, "got passphrase %q, expected %q", gotPass, tt.envValue)
			}
		})
	}
}
