// Package demand provides core functionality for the AIStore on-demand extended actions.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
)

const (
	idleRadius  = 2 * time.Second          // poll for relative quiescence
	idleDefault = time.Minute + idleRadius // hk => idle tick
)

type (
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	Demand interface {
		cluster.Xact
		IdleTimer() <-chan struct{}
		IncPending()
		DecPending()
		SubPending(n int)
	}
	DemandBase struct {
		XactBase
		mu      sync.RWMutex
		pending int64
		active  int64
		hkName  string
		idle    struct {
			d     time.Duration
			ticks *cos.StopCh
			last  int64 // mono.NanoTime
		}
		hkReg atomic.Bool
	}
)

////////////////
// DemandBase //
////////////////

func (r *DemandBase) Init(uuid, kind string, bck *cluster.Bck, idle time.Duration) (xdb *DemandBase) {
	r.hkName = kind + "/" + uuid
	r.idle.d = idleDefault
	if idle > 0 {
		r.idle.d = idle
	}
	r.idle.ticks = cos.NewStopCh()
	r.InitBase(uuid, kind, bck)
	r._initIdle()
	return
}

func (r *DemandBase) _initIdle() {
	r.active++
	r.idle.last = mono.NanoTime()
	r.hkReg.Store(true)
	hk.Reg(r.hkName, r.hkcb, 0 /*time.Duration*/)
}

func (r *DemandBase) hkcb() time.Duration {
	r.mu.Lock()
	if r.active == 0 {
		r.idle.ticks.Close() // signals the parent to finish and exit
	}
	r.active = 0
	r.mu.Unlock()
	return r.idle.d
}

func (r *DemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }

func (r *DemandBase) Pending() (cnt int64) {
	r.mu.RLock()
	cnt = r.pending
	r.mu.RUnlock()
	return
}
func (r *DemandBase) DecPending() { r.SubPending(1) }

func (r *DemandBase) IncPending() {
	debug.Assert(r.hkReg.Load())
	r.mu.Lock()
	r.pending++
	r.idle.last = 0
	r.active++
	r.mu.Unlock()
}

func (r *DemandBase) SubPending(n int) {
	r.mu.Lock()
	r.pending -= int64(n)
	debug.Assert(r.pending >= 0)
	if r.pending == 0 {
		r.idle.last = mono.NanoTime()
	}
	r.mu.Unlock()
}

func (r *DemandBase) Stop() {
	hk.Unreg(r.hkName)
	r.idle.ticks.Close()
}

func (r *DemandBase) Snap() cluster.XactionSnap { return r.ExtSnap() }

func (r *DemandBase) ExtSnap() *SnapExt {
	snap := &SnapExt{}
	r.ToSnap(&snap.Snap)
	if r.Bck() != nil {
		snap.Bck = r.Bck().Bck
	}
	snap.Ext = &BaseDemandStatsExt{IsIdle: r.likelyIdle()}
	return snap
}

func (r *DemandBase) Abort(err error) (ok bool) {
	if err == nil && !r.likelyIdle() {
		err = cmn.NewErrAborted(r.Name(), "x-demand", nil)
	}
	if ok = r.XactBase.Abort(err); ok {
		r.Finish(err)
	}
	return
}

// private: on-demand quiescence

func (r *DemandBase) quicb(_ time.Duration /*accum. wait time*/) cluster.QuiRes {
	if n := r.Pending(); n != 0 {
		debug.Assertf(r.Pending() > 0, "%s %d", r, n)
		return cluster.QuiActiveRet
	}
	return cluster.QuiInactiveCB
}

func (r *DemandBase) likelyIdle() bool {
	r.mu.RLock()
	last := r.idle.last
	r.mu.RUnlock()
	if mono.Since(last) < 2*idleRadius {
		return false
	}
	return r.Quiesce(idleRadius/2, r.quicb) == cluster.Quiescent
}
