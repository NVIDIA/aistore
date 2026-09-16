// Package oom: serialized goroutine to go ahead and run GC _now_
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package oom

import (
	rdebug "runtime/debug"
	"time"

	"sync/atomic"

	"github.com/NVIDIA/aistore/cmn/cos"
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
	nskip   atomic.Int64 // calls skipped since the last run (sparse logging)
)

func FreeToOS(force bool) bool {
	var (
		now  = mono.NanoTime()
		prev = last.Load()
		ival = ivalTime
	)
	if force {
		ival = forceTime
	}
	if prev > 0 {
		if since := time.Duration(now - prev); since < ival {
			if n := nskip.Add(1); cos.Sparse(n) {
				nlog.Infoln("not running - only", since, "<", ival, "passed since the previous run [ skipped:", n, "]")
			}
			return false
		}
	}
	if !running.CompareAndSwap(0, now) {
		if n := nskip.Add(1); cos.Sparse(n) {
			nlog.Infoln("still running [ skipped:", n, "]")
		}
		return false
	}

	nskip.Store(0)
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
