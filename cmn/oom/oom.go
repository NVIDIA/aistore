// Package oom: serialized goroutine to go ahead and run GC _now_
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package oom

import (
	rdebug "runtime/debug"
	"time"

	"sync/atomic"

	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	ivalTime  = 32 * time.Minute
	forceTime = 4 * time.Minute
)

var (
	last    atomic.Int64
	running atomic.Int64
)

func FreeToOS(force bool) bool {
	var (
		since time.Duration
		now   = mono.NanoTime()
		prev  = last.Load()
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
	if !running.CompareAndSwap(0, now) {
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

	last.Store(now)
	running.Store(0)
}
