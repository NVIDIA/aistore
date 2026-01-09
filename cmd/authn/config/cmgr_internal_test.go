// Package config_test contains tests for the auth config package
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLoadFromDisk_Invalid(t *testing.T) {
	conf := NewConfManager()
	_, err := conf.loadFromDisk("")
	tassert.Fatal(t, err != nil, "expected error when loading from invalid path")
}

func TestLoadFromDisk_Success(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "authn.json")

	cfg := &authn.Config{}
	cfg.Init()

	err := jsp.SaveMeta(path, cfg, nil)
	tassert.Fatalf(t, err == nil, "failed to write config: %v", err)

	cm := NewConfManager()
	got, err := cm.loadFromDisk(path)
	tassert.Fatalf(t, err == nil, "unexpected error loading config from disk: %v", err)
	tassert.Fatal(t, got != nil, "expected non-nil config")
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
