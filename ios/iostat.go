// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
)

type (
	IOStater interface {
		GetMpathUtil(mpath string, now time.Time) int64
		GetAllMpathUtils(now time.Time) (map[string]int64, map[string]*atomic.Int32)
		AddMpath(mpath string, fs string)
		RemoveMpath(mpath string)
		LogAppend(log []string) []string
		GetSelectedDiskStats() (m map[string]*SelectedDiskStats)
	}
)
