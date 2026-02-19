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
