// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
)

const (
	IdleDefault = time.Minute // hk -> idle tick
)

type (
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	Demand interface {
		core.Xact
		IdleTimer() <-chan struct{}
		IncPending()
		DecPending()
		SubPending(n int)
	}
	DemandBase struct {
		parentCB hk.HKCB
		ticks    *cos.StopCh
		hkName   string
		idle     struct {
			d    atomic.Int64 // duration hk idle
			last atomic.Int64 // mono.NanoTime
		}

		Base

		pending atomic.Int64
	}
)

////////////////
// DemandBase //
////////////////

// NOTE: override `Base.IsIdle`
func (r *DemandBase) IsIdle() bool {
	last := r.idle.last.Load()
	return last != 0 && mono.Since(last) >= max(cmn.Rom.MaxKeepalive()+time.Second, 4*time.Second)
}

func (r *DemandBase) Init(uuid, kind, ctlmsg string, bck *meta.Bck, idleDur time.Duration, hkcb ...hk.HKCB) {
	r.hkName = kind + "/" + uuid
	if idleDur > 0 {
		debug.Assert(idleDur >= IdleDefault || kind == apc.ActList, "Warning: ", idleDur, " is mostly expected >= ", IdleDefault, "(default)")
		r.idle.d.Store(int64(idleDur))
	} else {
		r.idle.d.Store(int64(IdleDefault))
	}
	if len(hkcb) == 0 {
		r.ticks = &cos.StopCh{}
		r.ticks.Init()
	} else {
		debug.Assert(len(hkcb) == 1)
		r.parentCB = hkcb[0]
		debug.Assert(r.parentCB != nil)
	}
	r.InitBase(uuid, kind, ctlmsg, bck)

	r.idle.last.Store(mono.NanoTime())
	hk.Reg(r.hkName+hk.NameSuffix, r.hkcb, 0 /*time.Duration*/)
}

// (e.g. usage: listed last page)
func (r *DemandBase) Reset(idleTime time.Duration) { r.idle.d.Store(int64(idleTime)) }

func (r *DemandBase) hkcb(now int64) time.Duration {
	last := r.idle.last.Load()
	idle := r.idle.d.Load()
	if last != 0 && now-last >= idle {
		if r.ticks != nil {
			// signal parent xaction to finish and exit (via `IdleTimer` chan)
			r.ticks.Close()
		}
		if r.parentCB != nil {
			r.parentCB(now)
		}
	}
	return time.Duration(idle)
}

func (r *DemandBase) IdleTimer() <-chan struct{} {
	return r.ticks.Listen()
}

func (r *DemandBase) Pending() (cnt int64) { return r.pending.Load() }
func (r *DemandBase) DecPending()          { r.SubPending(1) }

func (r *DemandBase) IncPending() {
	r.pending.Inc()
	r.idle.last.Store(0)
}

func (r *DemandBase) SubPending(n int) {
	debug.Assert(n > 0, n)
	pending := r.pending.Sub(int64(n))
	debug.Assert(pending >= 0)
	if pending == 0 {
		r.idle.last.Store(mono.NanoTime())
	}
}

func (r *DemandBase) Stop() {
	hk.Unreg(r.hkName + hk.NameSuffix)
	if r.ticks != nil {
		r.ticks.Close()
		r.ticks = nil
	}
}

func (r *DemandBase) Abort(err error) (ok bool) {
	if err == nil && !r.IsIdle() {
		err = cmn.NewErrAborted(r.Name(), "aborting non-idle", nil)
	}
	if ok = r.Base.Abort(err); ok {
		r.Finish()
	}
	return
}
