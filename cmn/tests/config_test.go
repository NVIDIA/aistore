// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

func TestConfigTestEnv(t *testing.T) {
	oldConfig := cmn.GCO.Get()
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	confPath := filepath.Join(thisFileDir(t), "configs", "config.json")
	localConfPath := filepath.Join(thisFileDir(t), "configs", "confignet.json")
	newConfig := cmn.Config{}
	err := cmn.LoadConfig(confPath, localConfPath, cmn.Proxy, &newConfig)
	tassert.CheckFatal(t, err)
}

func TestConfigFSPaths(t *testing.T) {
	var (
		oldConfig     = cmn.GCO.Get()
		confPath      = filepath.Join(thisFileDir(t), "configs", "config.json")
		localConfPath = filepath.Join(thisFileDir(t), "configs", "configmpaths.json")
	)
	defer func() {
		cmn.GCO.BeginUpdate()
		cmn.GCO.CommitUpdate(oldConfig)
	}()

	var localConf cmn.LocalConfig
	_, err := jsp.LoadMeta(localConfPath, &localConf)
	tassert.CheckFatal(t, err)
	newConfig := cmn.Config{}
	err = cmn.LoadConfig(confPath, localConfPath, cmn.Target, &newConfig)
	tassert.CheckFatal(t, err)

	mpaths := localConf.FSP.Paths
	tassert.Fatalf(t, len(newConfig.FSP.Paths) == len(mpaths), "mountpath count %v != %v", len(newConfig.FSP.Paths), len(mpaths))
	for p := range mpaths {
		tassert.Fatalf(t, newConfig.FSP.Paths.Contains(p), "%q not in config FSP", p)
	}
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
