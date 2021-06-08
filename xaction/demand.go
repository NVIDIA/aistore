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
	"github.com/NVIDIA/aistore/hk"
)

// On-demand xaction's lifecycle is controlled by two different "idle" timeouts:
// a) totallyIdle - interval of time until xaction stops running and self-terminates;
// b) likelyIdle - the time to determine that xaction can be (fairly) safely terminated.
//
// Note that `totallyIdle` is usually measured in 10s of seconds or even minutes;
// `likelyIdle` timeout, on the other hand, is there to establish the likelihood
// of being idle based on the simple determination of no activity for a (certain) while.

const (
	totallyIdle = time.Minute
	likelyIdle  = 4 * time.Second
)

type (
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	XactDemand interface {
		cluster.Xact
		IdleTimer() <-chan struct{}
		IncPending()
		DecPending()
		SubPending(n int)
	}
	idle struct {
		totally, likely time.Duration
		ticks           *cos.StopCh
	}
	XactDemandBase struct {
		XactBase
		pending atomic.Int64
		active  atomic.Int64
		hkName  string
		idle    idle
		hkReg   atomic.Bool
	}
)

////////////////////
// XactDemandBase //
////////////////////

// NOTE: to fully initialize it, must call xact.InitIdle() upon return
func NewXDB(args Args, idleTimes ...time.Duration) *XactDemandBase {
	var (
		hkName          string
		totally, likely = totallyIdle, likelyIdle
		uuid            = args.ID.String()
	)
	if len(idleTimes) != 0 {
		debug.Assert(len(idleTimes) == 2)
		totally, likely = idleTimes[0], idleTimes[1]
		debug.Assert(totally > likely)
		debug.Assert(likely > time.Second/10)
	}
	if uuid == "" {
		hkName = args.Kind + cos.GenUUID()
	} else {
		hkName = args.Kind + "/" + uuid
	}
	return &XactDemandBase{
		XactBase: *NewXactBase(args),
		hkName:   hkName,
		idle:     idle{totally: totally, likely: likely, ticks: cos.NewStopCh()},
	}
}

func (r *XactDemandBase) InitIdle() {
	r.active.Inc()
	r.hkReg.Store(true)
	hk.Reg(r.hkName, r.hkcb)
}

func (r *XactDemandBase) hkcb() time.Duration {
	if r.active.Swap(0) == 0 {
		// NOTE: closing IdleTimer() channel signals a parent on-demand xaction
		//       to finish and exit
		r.idle.ticks.Close()
	}
	return r.idle.totally
}

func (r *XactDemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }

func (r *XactDemandBase) Pending() int64 { return r.pending.Load() }
func (r *XactDemandBase) DecPending()    { r.SubPending(1) }

func (r *XactDemandBase) IncPending() {
	debug.AssertMsg(r.hkReg.Load(), "unregistered at hk, forgot InitIdle?")
	r.pending.Inc()
	r.active.Inc()
}

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
	var err error
	if !r.aborted.CAS(false, true) {
		glog.Infof("already aborted: " + r.String())
		return
	}
	if r.Kind() != cmn.ActList {
		if !r.likelyIdle() {
			err = cmn.NewAbortedError(r.String())
		}
	}
	r._setEndTime(err)
	close(r.abrt)
	glog.Infof("ABORT: " + r.String())
}

func (r *XactDemandBase) Finish(err error) {
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

func (r *XactDemandBase) quicb(elapsed time.Duration /*accum. wait time*/) cluster.QuiRes {
	switch {
	case r.Pending() != 0:
		debug.Assertf(r.Pending() > 0, "%s %d", r, r.Pending())
		return cluster.QuiActive
	case elapsed >= r.idle.likely:
		return cluster.QuiTimeout
	default:
		return cluster.QuiInactive
	}
}

func (r *XactDemandBase) likelyIdle() bool {
	return r.Quiesce(likelyIdle, r.quicb) == cluster.QuiInactive
}
