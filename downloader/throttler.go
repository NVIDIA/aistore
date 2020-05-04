// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import "github.com/NVIDIA/aistore/cmn"

type (
	throttler struct {
		sema *cmn.DynSemaphore
	}
)

func newThrottler(limits DlLimits) *throttler {
	if limits.Connections == 0 {
		return &throttler{}
	}
	return &throttler{
		sema: cmn.NewDynSemaphore(int(limits.Connections)),
	}
}

func (t *throttler) acquire() {
	if t.sema == nil {
		return
	}
	t.sema.Acquire()
}

func (t *throttler) release() {
	if t.sema == nil {
		return
	}
	t.sema.Release()
}
