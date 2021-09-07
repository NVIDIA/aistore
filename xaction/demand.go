// Package demand provides core functionality for the AIStore on-demand extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
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
		pending atomic.Int64
		active  atomic.Int64
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
	r.active.Inc()
	r.idle.last = mono.NanoTime()
	r.hkReg.Store(true)
	hk.Reg(r.hkName, r.hkcb)
}

func (r *DemandBase) hkcb() time.Duration {
	if r.active.Swap(0) == 0 {
		r.idle.ticks.Close() // signals the parent to finish and exit
	}
	return r.idle.d
}

func (r *DemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }

func (r *DemandBase) Pending() int64 { return r.pending.Load() }
func (r *DemandBase) DecPending()    { r.SubPending(1) }

func (r *DemandBase) IncPending() {
	debug.Assert(r.hkReg.Load())
	r.pending.Inc()
	r.idle.last = 0
	r.active.Inc()
}

func (r *DemandBase) SubPending(n int) {
	nn := r.pending.Sub(int64(n))
	debug.Assert(nn >= 0)
	if nn == 0 {
		r.idle.last = mono.NanoTime()
	}
}

func (r *DemandBase) Stop() {
	hk.Unreg(r.hkName)
	r.idle.ticks.Close()
}

func (r *DemandBase) Stats() cluster.XactStats { return r.ExtStats() }

func (r *DemandBase) ExtStats() *BaseXactStatsExt {
	stats := &BaseXactStatsExt{
		BaseXactStats: BaseXactStats{
			IDX:         r.ID(),
			KindX:       r.Kind(),
			StartTimeX:  r.StartTime(),
			EndTimeX:    r.EndTime(),
			ObjCountX:   r.ObjCount(),
			BytesCountX: r.BytesCount(),
			AbortedX:    r.Aborted(),
		},
	}
	if r.Bck() != nil {
		stats.BckX = r.Bck().Bck
	}
	stats.Ext = &BaseXactDemandStatsExt{IsIdle: r.likelyIdle()}
	return stats
}

func (r *DemandBase) Abort() {
	var err error
	if !r.aborted.CAS(false, true) {
		glog.Infoln("already aborted: " + r.String())
		return
	}
	if r.Kind() != cmn.ActList {
		if !r.likelyIdle() {
			err = cmn.NewErrAborted(r.Name(), "x-demand", nil)
		}
	}
	r._setEndTime(err)
	close(r.abrt)
	glog.Infoln("ABORT: " + r.String())
}

func (r *DemandBase) Finish(err error) {
	if r.Aborted() {
		return
	}
	if cmn.IsErrAborted(err) {
		if r.Kind() == cmn.ActList || r.likelyIdle() {
			err = nil
		}
	}
	r._setEndTime(err)
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
	if mono.Since(r.idle.last) < 2*idleRadius {
		return false
	}
	return r.Quiesce(idleRadius/2, r.quicb) == cluster.Quiescent
}
