// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
)

const timedDuration = time.Minute + time.Minute/2

const tag = "global GFN"

// get-from-neighbors (GFN) state
type gfnCtx struct {
	mtx    sync.Mutex
	exp    atomic.Int64
	upd    atomic.Int64
	lookup atomic.Bool
}

var gfn = &gfnCtx{}

func IsActiveGFN() bool {
	return gfn.lookup.Load() || (gfn.exp.Load() != 0 && gfn.upd.Load() > 0)
}

func ActivateTimedGFN() {
	if gfn.lookup.Load() {
		return
	}
	upd := int64(1)
	gfn.mtx.Lock()
	now := mono.NanoTime()
	if gfn.exp.Swap(now+timedDuration.Nanoseconds()) == 0 {
		gfn.upd.Store(1)
		gfn.mtx.Unlock()
		hk.Reg("timed-gfn"+hk.NameSuffix, deactivateTimed, 0 /*time.Duration*/)
		glog.Infoln(tag, "on timed", upd)
	} else {
		upd = gfn.upd.Inc()
		gfn.mtx.Unlock()
		glog.Infoln(tag, "on timed", upd)
	}
}

// Deactivates timed GFN only if timed GFN has been activated only once before.
func AbortTimedGFN() {
	gfn.mtx.Lock()
	if gfn.upd.Load() > 0 {
		gfn.upd.Dec()
	}
	gfn.mtx.Unlock()
}

// private

func deactivateTimed() time.Duration {
	gfn.mtx.Lock()
	now := mono.NanoTime()
	exp := gfn.exp.Swap(0)
	if gfn.lookup.Load() || exp <= now {
		gfn.upd.Store(0)
		gfn.mtx.Unlock()
		if exp > 0 {
			glog.Infoln(tag, "off timed")
		}
		return hk.UnregInterval
	}
	gfn.exp.Store(exp)
	gfn.mtx.Unlock()
	return time.Duration(exp - now + 100)
}

func activateGFN() bool {
	previous := gfn.lookup.Swap(true)
	if !previous {
		gfn.upd.Store(0)
		glog.Infoln(tag, "on")
	}
	return previous
}

func deactivateGFN() {
	gfn.lookup.Store(false)
	glog.Infoln(tag, "off")
}
