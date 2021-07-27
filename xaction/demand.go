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
	Demand interface {
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
	DemandBase struct {
		XactBase
		pending atomic.Int64
		active  atomic.Int64
		hkName  string
		idle    idle
		hkReg   atomic.Bool
	}
)

////////////////
// DemandBase //
////////////////

func (r *DemandBase) Init(uuid, kind string, bck *cluster.Bck, idleTimes ...time.Duration) (xdb *DemandBase) {
	var (
		hkName          = kind + "/" + uuid
		totally, likely = totallyIdle, likelyIdle
	)
	if len(idleTimes) != 0 {
		debug.Assert(len(idleTimes) == 2)
		totally, likely = idleTimes[0], idleTimes[1]
		debug.Assert(totally > likely)
		debug.Assert(likely > time.Second/10)
	}
	{
		r.hkName = hkName
		r.idle.totally = totally
		r.idle.likely = likely
		r.idle.ticks = cos.NewStopCh()
	}
	r.InitBase(uuid, kind, bck)
	r._initIdle()
	return
}

func (r *DemandBase) _initIdle() {
	r.active.Inc()
	r.hkReg.Store(true)
	hk.Reg(r.hkName, r.hkcb)
}

func (r *DemandBase) hkcb() time.Duration {
	if r.active.Swap(0) == 0 {
		// NOTE: closing IdleTimer() channel signals a parent on-demand xaction
		//       to finish and exit
		r.idle.ticks.Close()
	}
	return r.idle.totally
}

func (r *DemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }

func (r *DemandBase) Pending() int64 { return r.pending.Load() }
func (r *DemandBase) DecPending()    { r.SubPending(1) }

func (r *DemandBase) IncPending() {
	debug.AssertMsg(r.hkReg.Load(), "unregistered at hk, forgot InitIdle?")
	r.pending.Inc()
	r.active.Inc()
}

func (r *DemandBase) SubPending(n int) {
	r.pending.Sub(int64(n))
	debug.Assert(r.Pending() >= 0)
}

func (r *DemandBase) Stop() {
	hk.Unreg(r.hkName)
	r.idle.ticks.Close()
}

func (r *DemandBase) Stats() cluster.XactStats {
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
	return stats
}

func (r *DemandBase) Abort() {
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

func (r *DemandBase) quicb(elapsed time.Duration /*accum. wait time*/) cluster.QuiRes {
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

func (r *DemandBase) likelyIdle() bool {
	return r.Quiesce(likelyIdle, r.quicb) == cluster.QuiInactive
}
