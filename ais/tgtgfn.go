// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
)

type (
	baseGFN struct {
		tag    string
		lookup atomic.Bool
	}
	// The state that may influence GET logic when mountpath is added/enabled
	localGFN struct {
		baseGFN
	}
	// The state that may influence GET logic when new target joins cluster
	globalGFN struct {
		baseGFN
		mtx sync.Mutex
		exp atomic.Int64
		upd atomic.Int64
	}
)

const timedDuration = time.Minute + time.Minute/2

//////////////
// base gfn //
//////////////

func (gfn *baseGFN) active() bool { return gfn.lookup.Load() }

func (gfn *baseGFN) Activate() bool {
	previous := gfn.lookup.Swap(true)
	if !previous {
		glog.Infoln(gfn.tag, "on")
	}
	return previous
}

func (gfn *baseGFN) Deactivate() {
	gfn.lookup.Store(false)
	glog.Infoln(gfn.tag, "off")
}

////////////////
// global gfn //
////////////////

func (gfn *globalGFN) Activate() bool {
	previous := gfn.lookup.Swap(true)
	if !previous {
		gfn.upd.Store(0)
		glog.Infoln(gfn.tag, "on")
	}
	return previous
}

func (gfn *globalGFN) active() bool {
	return gfn.lookup.Load() || (gfn.exp.Load() != 0 && gfn.upd.Load() > 0)
}

func (gfn *globalGFN) activateTimed() {
	if gfn.lookup.Load() {
		return
	}
	upd := int64(1)
	gfn.mtx.Lock()
	now := mono.NanoTime()
	if gfn.exp.Swap(now+timedDuration.Nanoseconds()) == 0 {
		gfn.upd.Store(1)
		gfn.mtx.Unlock()
		hk.Reg(gfn.tag, gfn.hk)
		glog.Infoln(gfn.tag, "on timed", upd)
	} else {
		upd = gfn.upd.Inc()
		gfn.mtx.Unlock()
		glog.Infoln(gfn.tag, "on timed", upd)
	}
}

func (gfn *globalGFN) hk() time.Duration {
	gfn.mtx.Lock()
	now := mono.NanoTime()
	exp := gfn.exp.Swap(0)
	if gfn.lookup.Load() || exp <= now {
		gfn.upd.Store(0)
		gfn.mtx.Unlock()
		if exp > 0 {
			glog.Infoln(gfn.tag, "off timed")
		}
		return hk.UnregInterval
	}
	gfn.exp.Store(exp)
	gfn.mtx.Unlock()
	return time.Duration(exp - now + 100)
}

// Deactivates timed GFN only if timed GFN has been activated only once before.
func (gfn *globalGFN) abortTimed() {
	gfn.mtx.Lock()
	if gfn.upd.Load() > 0 {
		gfn.upd.Dec()
	}
	gfn.mtx.Unlock()
}
