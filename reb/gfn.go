// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

const (
	gfnT = "gfn-timed"
	gfnG = "gfn-global"
)

// get-from-neighbors (GFN) state
type gfnCtx struct {
	mtx sync.Mutex
	exp atomic.Int64
	trc atomic.Int64
	gon atomic.Bool
}

var gfn = &gfnCtx{}

func IsActiveGFN() bool {
	return gfn.gon.Load() || (gfn.exp.Load() != 0 && gfn.trc.Load() > 0)
}

func ActivateTimedGFN() {
	if gfn.gon.Load() {
		return
	}
	gfn.mtx.Lock()
	now := mono.NanoTime()
	if gfn.exp.Swap(now+timedDuration.Nanoseconds()) == 0 {
		gfn.trc.Store(1)
		hk.Reg(gfnT+hk.NameSuffix, hkTimed, 0 /*time.Duration*/)
		gfn.mtx.Unlock()
		glog.Infoln(gfnT, 1)
	} else {
		trc := gfn.trc.Inc()
		gfn.mtx.Unlock()
		glog.Infoln(gfnT, trc)
	}
}

func hkTimed() time.Duration {
	gfn.mtx.Lock()
	now := mono.NanoTime()
	exp := gfn.exp.Swap(0)
	if gfn.gon.Load() || exp <= now {
		gfn.trc.Store(0)
		gfn.mtx.Unlock()
		if exp > 0 {
			glog.Infoln(gfnT, "off")
		}
		return hk.UnregInterval
	}
	gfn.exp.Store(exp)
	gfn.mtx.Unlock()
	return time.Duration(exp - now + 100)
}

func activateGFN() (prev bool) {
	if prev = gfn.gon.Swap(true); !prev {
		gfn.trc.Store(0) // see IsActiveGFN
		glog.Infoln(gfnG, "on")
	}
	return
}

func deactivateGFN() {
	gfn.gon.Store(false)
	glog.Infoln(gfnG, "off")
}
