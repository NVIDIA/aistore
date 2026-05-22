// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// SparseWarn rate-limits per-LOM warnings: log the first 10, then every 100th
// up to 1000, then powers of two; or always when the per-module verbose level
// is at or above 5.

const (
	back2backFirst = 10
)

func Sparse(cnt int64) bool {
	return cnt <= back2backFirst || (cnt <= 1000 && cnt%100 == 0) || cnt&(cnt-1) == 0
}

func SparseWarn(mod int, cnt int64, args ...any) {
	if Rom.V(5, mod) || Sparse(cnt) {
		nlog.Warningln(args...)
	}
}
