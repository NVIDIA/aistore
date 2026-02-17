// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/core"
)

// common ref-counted quiescence
func RefcntQuiCB(refc *atomic.Int32, maxTimeout, totalSoFar time.Duration) core.QuiRes {
	if refc.Load() > 0 {
		return core.QuiActive
	}
	if totalSoFar > maxTimeout {
		return core.QuiTimeout
	}
	return core.QuiInactiveCB
}
