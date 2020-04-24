// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestConfigTestEnv(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	mockVars := &jsp.ConfigCLI{
		ConfFile: filepath.Join(thisFileDir(t), "configs", "configtest.json"),
	}

	jsp.LoadConfig(mockVars)
}

func TestConfigFSPaths(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	mockVars := &jsp.ConfigCLI{
		ConfFile: filepath.Join(thisFileDir(t), "configs", "configfspaths.json"),
	}

	jsp.LoadConfig(mockVars)
}

func thisFileDir(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(1)
	tassert.Fatalf(t, ok, "Taking path of a file failed")
	return filepath.Dir(filename)
}

func TestValidateMpath(t *testing.T) {
	mpaths := []string{
		"tmp", // not absolute path
		"/",   // root
	}
	for _, mpath := range mpaths {
		_, err := cmn.ValidateMpath(mpath)
		if err == nil {
			t.Errorf("validation of invalid mountpath: %q succeeded", mpath)
		}
	}
}
