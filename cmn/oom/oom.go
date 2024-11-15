// Package oom: serialized goroutine to go ahead and run GC _now_
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package oom

import (
	rdebug "runtime/debug"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// FreeToOS executes in a separate goroutine with at least so-many minutes (above)
// between the runs. It calls GC and returns allocated memory to the operating system.
// Notes:
// - forceTime (below) is expected to be >> the time spent in the goroutine
// - still, an unlikely overlap is handled via `running` atomic

const (
	ivalTime  = 32 * time.Minute
	forceTime = 4 * time.Minute
)

var (
	last    int64
	running int64
)

func FreeToOS(force bool) bool {
	prev := ratomic.LoadInt64(&last)
	ival := ivalTime
	if force {
		ival = forceTime
	}

	now := mono.NanoTime()
	elapsed := time.Duration(now - prev)
	if elapsed < ival {
		nlog.Infoln("not running - only", elapsed, "passed since the previous run")
		return false
	}
	if !ratomic.CompareAndSwapInt64(&running, 0, now) {
		nlog.Infoln("(still) running for", elapsed, "- nothing to do")
		return false
	}

	go do(now)
	return true
}

func do(started int64) {
	rdebug.FreeOSMemory()

	now := mono.NanoTime()
	if elapsed := time.Duration(now - started); elapsed > (forceTime >> 1) {
		nlog.Warningln("spent", elapsed.String(), "freeing memory")
	}
	ratomic.StoreInt64(&last, now)
	ratomic.StoreInt64(&running, 0)
}
