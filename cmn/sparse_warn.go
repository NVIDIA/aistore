// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// SparseWarn rate-limits repeated warnings: log the first cos.SparseFirst,
// then every 100th up to 1000, then powers of two; or always when the
// per-module verbose level is at or above 5.
func SparseWarn(mod int, cnt int64, args ...any) {
	if Rom.V(5, mod) || cos.Sparse(cnt) {
		nlog.WarningDepth(1, args...)
	}
}
