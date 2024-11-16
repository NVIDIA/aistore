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

// FreeToOS executes in a separate goroutine with at least so-many minutes
// between the runs. It calls GC and returns allocated memory to the operating system.
// Notes:
// - forceTime (below) is expected to be >> the time spent in the goroutine
// - unlikely overlap is handled via `running` atomic

const (
	ivalTime  = 32 * time.Minute
	forceTime = 4 * time.Minute
)

var (
	last    int64
	running int64
)

func FreeToOS(force bool) bool {
	var (
		since time.Duration
		now   = mono.NanoTime()
		prev  = ratomic.LoadInt64(&last)
		ival  = ivalTime
	)
	if force {
		ival = forceTime
	}
	if prev > 0 {
		since = time.Duration(now - prev)
		if since < ival {
			nlog.Infoln("not running - only", since, "<", ival, "passed since the previous run")
			return false
		}
	}
	if !ratomic.CompareAndSwapInt64(&running, 0, now) {
		nlog.Infoln("still running [", since, "]")
		return false
	}

	go do(now)
	return true
}

func do(started int64) {
	rdebug.FreeOSMemory()

	now := mono.NanoTime()
	nlog.Warningln("free-mem runtime:", time.Duration(now-started))

	ratomic.StoreInt64(&last, now)
	ratomic.StoreInt64(&running, 0)
}
