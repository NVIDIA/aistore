// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"os"
	"os/user"
)

func HomeDir() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		Errorf("%v", err)
		return os.UserHomeDir()
	}
	return currentUser.HomeDir, nil
}
