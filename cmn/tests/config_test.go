/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestConfigTestEnv(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	mockVars := &cmn.ConfigCLI{
		ConfFile: filepath.Join(thisFileDir(t), "configs", "configtest.json"),
	}

	_, _, err := cmn.LoadConfigErr(mockVars)
	tassert.CheckFatal(t, err)
}

func TestConfigFSPaths(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	mockVars := &cmn.ConfigCLI{
		ConfFile: filepath.Join(thisFileDir(t), "configs", "configfspaths.json"),
	}

	_, _, err := cmn.LoadConfigErr(mockVars)
	tassert.CheckFatal(t, err)
}

func TestConfigNoFSPaths(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	mockVars := &cmn.ConfigCLI{
		ConfFile: filepath.Join(thisFileDir(t), "configs", "confignofspaths.json"),
	}

	_, _, err := cmn.LoadConfigErr(mockVars)
	tassert.Fatalf(t, err != nil, "Expected LoadConfigErr to return error")
}

func thisFileDir(t *testing.T) string {
	_, filename, _, ok := runtime.Caller(1)
	tassert.Fatalf(t, ok, "Taking path of a file failed")
	return filepath.Dir(filename)
}
