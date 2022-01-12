// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
)

type (
	Base struct {
		id      string
		kind    string
		bck     *cluster.Bck
		origBck cmn.Bck
		sutime  atomic.Int64
		eutime  atomic.Int64
		stats   struct {
			objs     atomic.Int64 // locally processed
			bytes    atomic.Int64
			outobjs  atomic.Int64 // transmit
			outbytes atomic.Int64
			inobjs   atomic.Int64 // receive
			inbytes  atomic.Int64
		}
		notif *NotifXact
		abort struct {
			mu  sync.RWMutex
			ch  chan error
			err error
		}
	}
	Marked struct {
		Xact        cluster.Xact
		Interrupted bool
	}
)

var IncInactive func()

// common helper to go-run and wait until it actually starts running
func GoRunW(xctn cluster.Xact) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go xctn.Run(wg)
	wg.Wait()
}

//////////////
// Base - partially implements Xact interface
//////////////

func (xctn *Base) InitBase(id, kind string, bck *cluster.Bck) {
	debug.AssertMsg(kind == cmn.ActETLInline || cos.IsValidUUID(id) || IsValidRebID(id), id)
	debug.AssertMsg(IsValidKind(kind), kind)
	xctn.id, xctn.kind = id, kind
	xctn.abort.ch = make(chan error, 1)
	xctn.bck = bck
	if xctn.bck != nil {
		xctn.origBck = bck.Bck
	}
	xctn.setStartTime(time.Now())
}

func (xctn *Base) ID() string        { return xctn.id }
func (xctn *Base) Kind() string      { return xctn.kind }
func (xctn *Base) Bck() *cluster.Bck { return xctn.bck }
func (xctn *Base) Finished() bool    { return xctn.eutime.Load() != 0 }

func (xctn *Base) Running() (yes bool) {
	yes = xctn.sutime.Load() != 0 && !xctn.Finished()
	debug.AssertMsg(!yes || xctn.ID() != "", xctn.String())
	return
}

//
// aborting
//
func (xctn *Base) ChanAbort() <-chan error { return xctn.abort.ch }

func (xctn *Base) IsAborted() bool { return xctn.Aborted() != nil }

func (xctn *Base) Aborted() (err error) {
	xctn.abort.mu.RLock()
	err = xctn.abort.err
	xctn.abort.mu.RUnlock()
	return
}

func (xctn *Base) AbortedAfter(d time.Duration) (err error) {
	sleep := cos.CalcProbeFreq(d)
	err = xctn.Aborted()
	for elapsed := time.Duration(0); elapsed < d && err == nil; elapsed += sleep {
		time.Sleep(sleep)
		err = xctn.Aborted()
	}
	return
}

func (xctn *Base) Abort(err error) (ok bool) {
	if err == nil {
		err = cmn.ErrXactNoErrAbort
	} else if errAborted := cmn.AsErrAborted(err); errAborted != nil {
		err = errAborted.Unwrap()
	}
	xctn.abort.mu.Lock()
	if err := xctn.abort.err; err != nil {
		xctn.abort.mu.Unlock()
		glog.Warningf("%s already aborted(%v)", xctn.Name(), err)
		return
	}
	xctn.abort.err = err
	xctn.abort.ch <- err
	close(xctn.abort.ch)
	xctn.abort.mu.Unlock()
	if xctn.Kind() != cmn.ActList {
		glog.Infof("%s aborted(%v)", xctn.Name(), err)
	}
	return true
}

// count all the way to duration; reset and adjust every time activity is detected
func (xctn *Base) Quiesce(d time.Duration, cb cluster.QuiCB) cluster.QuiRes {
	var (
		idle, total time.Duration
		sleep       = cos.CalcProbeFreq(d)
		dur         = d
	)
	if xctn.Aborted() != nil {
		return cluster.QuiAborted
	}
	for idle < dur {
		time.Sleep(sleep)
		if xctn.Aborted() != nil {
			return cluster.QuiAborted
		}
		total += sleep
		switch res := cb(total); res {
		case cluster.QuiInactiveCB: // NOTE: used by callbacks, converts to one of the returned codes
			idle += sleep
		case cluster.QuiActive:
			idle = 0                              // reset
			dur = cos.MinDuration(dur+sleep, 2*d) // bump up to 2x initial
		case cluster.QuiActiveRet:
			return cluster.QuiActiveRet
		case cluster.QuiDone:
			return cluster.QuiDone
		case cluster.QuiTimeout:
			return cluster.QuiTimeout
		}
	}
	return cluster.Quiescent
}

func (xctn *Base) Name() (s string) {
	var b string
	if xctn.bck != nil {
		b = "-" + xctn.origBck.String()
	}
	s = "x-" + xctn.Kind() + "[" + xctn.ID() + "]" + b
	return
}

func (xctn *Base) String() string {
	var (
		name = xctn.Name()
		s    = name + "-" + cos.FormatTimestamp(xctn.StartTime())
	)
	if !xctn.Finished() { // ok to (rarely) miss _aborted_ state as this is purely informational
		return s
	}
	etime := cos.FormatTimestamp(xctn.EndTime())
	if err := xctn.Aborted(); err != nil {
		s += "-[abrt: " + err.Error() + "]"
	}
	return s + "-" + etime
}

func (xctn *Base) StartTime() time.Time {
	u := xctn.sutime.Load()
	if u != 0 {
		return time.Unix(0, u)
	}
	return time.Time{}
}

func (xctn *Base) setStartTime(s time.Time) { xctn.sutime.Store(s.UnixNano()) }

func (xctn *Base) EndTime() time.Time {
	u := xctn.eutime.Load()
	if u != 0 {
		return time.Unix(0, u)
	}
	return time.Time{}
}

// upon completion, all xactions:
// - atomically set end-time
// - optionally, notify listener(s)
// - optionally, refresh local capacity stats, etc.
func (xctn *Base) notifyRefresh(err error) {
	// notifications
	if n := xctn.Notif(); n != nil {
		nl.OnFinished(n, err)
	}
	xactRecord := Table[xctn.kind]
	if xactRecord.RefreshCap {
		if cs, _ := fs.RefreshCapStatus(nil, nil); cs.Err != nil {
			glog.Error(cs.Err) // log warning
		}
	}

	IncInactive() // in re: HK cleanup long-time finished
}

func (xctn *Base) Notif() (n cluster.Notif) {
	if xctn.notif == nil {
		return
	}
	return xctn.notif
}

func (xctn *Base) AddNotif(n cluster.Notif) {
	xctn.notif = n.(*NotifXact)
	debug.Assert(xctn.notif.Xact != nil && xctn.notif.F != nil)
	debug.Assert(!n.Upon(cluster.UponProgress) || xctn.notif.P != nil)
}

func (xctn *Base) Finish(err error) {
	if xctn.eutime.CAS(0, 1) {
		xctn.eutime.Store(time.Now().UnixNano())
		xctn.notifyRefresh(err)
		if xctn.Kind() != cmn.ActList {
			glog.Infof("%s finished(%v)", xctn, err)
		}
	}
}

func (*Base) Result() (interface{}, error) {
	return nil, errors.New("getting result is not implemented")
}

// base stats: locally processed
func (xctn *Base) Objs() int64  { return xctn.stats.objs.Load() }
func (xctn *Base) Bytes() int64 { return xctn.stats.bytes.Load() }

func (xctn *Base) ObjsAdd(cnt int, size int64) {
	xctn.stats.objs.Add(int64(cnt))
	xctn.stats.bytes.Add(size)
}

// base stats: transmit
func (xctn *Base) OutObjs() int64  { return xctn.stats.outobjs.Load() }
func (xctn *Base) OutBytes() int64 { return xctn.stats.outbytes.Load() }

func (xctn *Base) OutObjsAdd(cnt int, size int64) {
	xctn.stats.outobjs.Add(int64(cnt))
	if size > 0 { // not unsized
		xctn.stats.outbytes.Add(size)
	}
}

// base stats: receive
func (xctn *Base) InObjs() int64  { return xctn.stats.inobjs.Load() }
func (xctn *Base) InBytes() int64 { return xctn.stats.inbytes.Load() }

func (xctn *Base) InObjsAdd(cnt int, size int64) {
	xctn.stats.inobjs.Add(int64(cnt))
	if size > 0 { // not unsized
		xctn.stats.inbytes.Add(size)
	}
}

func (xctn *Base) Snap() cluster.XactSnap {
	snap := &Snap{}
	xctn.ToSnap(snap)
	return snap
}

func (xctn *Base) ToSnap(snap *Snap) {
	snap.ID = xctn.ID()
	snap.Kind = xctn.Kind()
	snap.Bck = xctn.origBck
	snap.StartTime = xctn.StartTime()
	snap.EndTime = xctn.EndTime()
	snap.AbortedX = xctn.Aborted() != nil

	xctn.ToStats(&snap.Stats)
}

func (xctn *Base) GetStats() (stats *Stats) {
	stats = &Stats{}
	xctn.ToStats(stats)
	return
}

func (xctn *Base) ToStats(stats *Stats) {
	stats.Objs = xctn.Objs()         // locally processed
	stats.Bytes = xctn.Bytes()       //
	stats.OutObjs = xctn.OutObjs()   // transmit
	stats.OutBytes = xctn.OutBytes() //
	stats.InObjs = xctn.InObjs()     // receive
	stats.InBytes = xctn.InBytes()
}

// RebID helpers

func RebID2S(id int64) string          { return fmt.Sprintf("g%d", id) }
func S2RebID(id string) (int64, error) { return strconv.ParseInt(id[1:], 10, 64) }
func IsValidRebID(id string) bool      { _, err := S2RebID(id); return err == nil }

func CompareRebIDs(a, b string) int {
	if ai, err := S2RebID(a); err != nil {
		debug.AssertMsg(false, a)
	} else if bi, err := S2RebID(b); err != nil {
		debug.AssertMsg(false, b)
	} else {
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
	}
	return 0
}
