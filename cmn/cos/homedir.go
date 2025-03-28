// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"os"
	"os/user"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
)

func HomeDir() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		Errorf("%v", err)
		return os.UserHomeDir()
	}
	return currentUser.HomeDir, nil
}

func HomeConfigDir(subdir string) (configDir string) {
	home, err := HomeDir()
	if err != nil {
		debug.AssertNoErr(err)
		Errorf("%v", err)
	}
	// $HOME/.config/ais/<subdir>
	return filepath.Join(home, fname.HomeConfigsDir, fname.HomeAIS, subdir)
}
