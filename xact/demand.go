// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
)

const (
	IdleDefault = time.Minute // hk -> idle tick
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
		hkName string
		idle   struct {
			ticks cos.StopCh
			d     time.Duration // hk idle
			last  atomic.Int64  // mono.NanoTime
		}

		Base

		pending atomic.Int64
		hkReg   atomic.Bool // mono.NanoTime
	}
)

////////////////
// DemandBase //
////////////////

// NOTE: override `Base.IsIdle`
func (r *DemandBase) IsIdle() bool {
	last := r.idle.last.Load()
	return last != 0 && mono.Since(last) >= max(cmn.Rom.MaxKeepalive(), 2*time.Second)
}

func (r *DemandBase) Init(uuid, kind string, bck *meta.Bck, idle time.Duration) {
	r.hkName = kind + "/" + uuid
	r.idle.d = IdleDefault
	if idle > 0 {
		r.idle.d = idle
	}
	r.idle.ticks.Init()
	r.InitBase(uuid, kind, bck)

	r.idle.last.Store(mono.NanoTime())
	r.hkReg.Store(true)
	hk.Reg(r.hkName+hk.NameSuffix, r.hkcb, 0 /*time.Duration*/)
}

func (r *DemandBase) hkcb() time.Duration {
	last := r.idle.last.Load()
	if last != 0 && mono.Since(last) >= r.idle.d {
		// signal parent xaction via IdleTimer() chan
		// to finish and exit
		r.idle.ticks.Close()
	}
	return r.idle.d
}

func (r *DemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }
func (r *DemandBase) Pending() (cnt int64)       { return r.pending.Load() }
func (r *DemandBase) DecPending()                { r.SubPending(1) }

func (r *DemandBase) IncPending() {
	debug.Assert(r.hkReg.Load())
	r.pending.Inc()
	r.idle.last.Store(0)
}

func (r *DemandBase) SubPending(n int) {
	if n == 0 {
		return
	}
	pending := r.pending.Sub(int64(n))
	debug.Assert(pending >= 0)
	if pending == 0 {
		r.idle.last.Store(mono.NanoTime())
	}
}

func (r *DemandBase) Stop() {
	hk.Unreg(r.hkName + hk.NameSuffix)
	r.idle.ticks.Close()
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
