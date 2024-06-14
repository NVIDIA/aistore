// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"github.com/NVIDIA/aistore/cmn/nlog"
	"golang.org/x/sys/unix"
)

func getFSStats(path string) (fsStats unix.Statfs_t, err error) {
	if err = unix.Statfs(path, &fsStats); err != nil {
		nlog.Errorf("failed to statfs %q, err: %v", path, err)
	}
	return
}
