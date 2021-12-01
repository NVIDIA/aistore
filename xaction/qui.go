// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
)

// common ref-counted quiescence
func RefcntQuiCB(refc *atomic.Int32, maxTimeout, totalSoFar time.Duration) cluster.QuiRes {
	if refc.Load() > 0 {
		return cluster.QuiActive
	}
	if totalSoFar > maxTimeout {
		return cluster.QuiTimeout
	}
	return cluster.QuiInactiveCB
}
