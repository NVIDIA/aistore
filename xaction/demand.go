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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
)

// Default on-demand idle timeouts
const (
	totallyIdle = time.Minute // to confirm idle-ness we may in fact stay around for twice as much
	likelyIdle  = 4 * time.Second
)

type (
	//
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	//
	XactDemand interface {
		cluster.Xact
		IdleTimer() <-chan struct{}
		IncPending()
		DecPending()
		SubPending(n int)
		IsIdle() bool
	}

	idleInfo struct {
		dur    time.Duration
		ticks  *cmn.StopCh
		likely bool
	}

	XactDemandBase struct {
		XactBase

		pending atomic.Int64
		active  atomic.Int64
		hkName  string
		idle    idleInfo
		hkReg   atomic.Bool
	}
)

////////////////////
// XactDemandBase //
////////////////////

// NOTE: to fully initialize it, must call xact.InitIdle() upon return
func NewXDB(args Args, idleTimes ...time.Duration) *XactDemandBase {
	var (
		hkName   string
		idleTime = totallyIdle
		uuid     = args.ID.String()
	)
	if len(idleTimes) != 0 {
		idleTime = idleTimes[0]
	}
	if uuid == "" {
		hkName = args.Kind + cmn.GenUUID()
	} else {
		hkName = args.Kind + "/" + uuid
	}
	r := &XactDemandBase{
		XactBase: *NewXactBase(args),
		hkName:   hkName,
		idle:     idleInfo{dur: idleTime, ticks: cmn.NewStopCh()},
	}
	return r
}

func (r *XactDemandBase) InitIdle() {
	r.hkReg.Store(true)
	hk.Reg(r.hkName, func() time.Duration {
		active := r.active.Swap(0)
		if r.Pending() > 0 || active > 0 {
			r.idle.likely = false // not idle
		} else if active == 0 {
			if r.idle.likely {
				r.idle.ticks.Close() // idleness confirmed: send "idle tick".
			} else {
				// likely idle (haven't seen any activity): prepare to send "idle tick"
				r.idle.likely = true
			}
		}
		return r.idle.dur
	})
}

func (r *XactDemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }

func (r *XactDemandBase) IsIdle() bool   { return r.Pending() == 0 }
func (r *XactDemandBase) Pending() int64 { return r.pending.Load() }
func (r *XactDemandBase) IncPending() {
	debug.AssertMsg(r.hkReg.Load(), "unregistered at hk, forgot InitIdle?")
	r.pending.Inc()
	r.active.Inc()
}
func (r *XactDemandBase) DecPending() { r.SubPending(1) }
func (r *XactDemandBase) SubPending(n int) {
	r.pending.Sub(int64(n))
	debug.Assert(r.Pending() >= 0)
}

func (r *XactDemandBase) Stop() {
	hk.Unreg(r.hkName)
	r.idle.ticks.Close()
}

func (r *XactDemandBase) Stats() cluster.XactStats {
	return &BaseXactStatsExt{
		BaseXactStats: BaseXactStats{
			IDX:         r.ID().String(),
			KindX:       r.Kind(),
			StartTimeX:  r.StartTime(),
			EndTimeX:    r.EndTime(),
			BckX:        r.Bck(),
			ObjCountX:   r.ObjCount(),
			BytesCountX: r.BytesCount(),
			AbortedX:    r.Aborted(),
		},
	}
}

func (r *XactDemandBase) Abort() {
	if !r.aborted.CAS(false, true) {
		glog.Infof("already aborted: " + r.String())
		return
	}
	if r.Kind() == cmn.ActListObjects || r.isQuiescent() {
		r._setEndTime(nil)
	} else {
		r._setEndTime(cmn.NewAbortedError(r.String()))
	}
	close(r.abrt)
	glog.Infof("ABORT: " + r.String())
}

func (r *XactDemandBase) Finish(err error) {
	if r.Aborted() {
		return
	}
	if cmn.IsErrAborted(err) && (r.Kind() == cmn.ActListObjects || r.isQuiescent()) {
		err = nil
	}
	r._setEndTime(err)
}

// private: on-demand quiescence

func (r *XactDemandBase) quicb(elapsed time.Duration /*accum. wait time*/) cluster.QuiRes {
	switch {
	case elapsed >= likelyIdle:
		return cluster.QuiTimeout
	case !r.IsIdle():
		return cluster.QuiActive
	default:
		return cluster.QuiInactive
	}
}

func (r *XactDemandBase) isQuiescent() bool {
	return r.Quiesce(likelyIdle, r.quicb) == cluster.QuiInactive
}
