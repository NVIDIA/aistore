// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	rdebug "runtime/debug"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	minFreeMem   = 30 * time.Minute
	forceFreeMem = 10 * time.Minute
)

var (
	lastTrigOOM int64
	runningOOM  int64
)

// FreeMemToOS executes in a separate goroutine with at least so-many minutes (above)
// between the runs. It calls GC and returns allocated memory to the operating system.
// Notes:
//   - `forceFreeMemTime` is expected to be significantly greater than the time spent
//     in the goroutine
//   - still, an unlikely case of overlapping runs is treated via `runningOOM`
//   - for somewhat related OOS handling - esp. constants - see ais/tgtspace.go
func FreeMemToOS(force bool) bool {
	prev := ratomic.LoadInt64(&lastTrigOOM)
	ival := minFreeMem
	if force {
		ival = forceFreeMem
	}

	now := mono.NanoTime()
	elapsed := time.Duration(now - prev)
	if elapsed < ival {
		nlog.Infoln("not running - only", elapsed, "passed since the previous run")
		return false
	}
	if !ratomic.CompareAndSwapInt64(&runningOOM, 0, now) {
		nlog.Infoln("(still) running for", elapsed, "- nothing to do")
		return false
	}

	go _freeMem(now)
	return true
}

func _freeMem(started int64) {
	rdebug.FreeOSMemory()

	now := mono.NanoTime()
	if elapsed := time.Duration(now - started); elapsed > forceFreeMem/2 {
		nlog.Errorln("Warning: spent " + elapsed.String() + " freeing memory")
	}
	ratomic.StoreInt64(&lastTrigOOM, now)
	ratomic.StoreInt64(&runningOOM, 0)
}
