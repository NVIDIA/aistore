// Package xaction provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

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
	XactBase struct {
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
	XactMarked struct {
		Xact        cluster.Xact
		Interrupted bool
	}
)

var IncInactive func()

// common helper to go-run and wait until it actually starts running
func GoRunW(xact cluster.Xact) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go xact.Run(wg)
	wg.Wait()
}

//////////////
// XactBase - partially implements Xact interface
//////////////

func (xact *XactBase) InitBase(id, kind string, bck *cluster.Bck) {
	debug.AssertMsg(kind == cmn.ActETLInline || cos.IsValidUUID(id) || IsValidRebID(id), id)
	debug.AssertMsg(IsValidKind(kind), kind)
	xact.id, xact.kind = id, kind
	xact.abort.ch = make(chan error, 1)
	xact.bck = bck
	if xact.bck != nil {
		xact.origBck = bck.Bck
	}
	xact.setStartTime(time.Now())
}

func (xact *XactBase) ID() string        { return xact.id }
func (xact *XactBase) Kind() string      { return xact.kind }
func (xact *XactBase) Bck() *cluster.Bck { return xact.bck }
func (xact *XactBase) Finished() bool    { return xact.eutime.Load() != 0 }

//
// aborting
//
func (xact *XactBase) ChanAbort() <-chan error { return xact.abort.ch }

func (xact *XactBase) IsAborted() bool { return xact.Aborted() != nil }

func (xact *XactBase) Aborted() (err error) {
	xact.abort.mu.RLock()
	err = xact.abort.err
	xact.abort.mu.RUnlock()
	return
}

func (xact *XactBase) AbortedAfter(d time.Duration) (err error) {
	sleep := cos.CalcProbeFreq(d)
	err = xact.Aborted()
	for elapsed := time.Duration(0); elapsed < d && err == nil; elapsed += sleep {
		time.Sleep(sleep)
		err = xact.Aborted()
	}
	return
}

func (xact *XactBase) Abort(err error) (ok bool) {
	if err == nil {
		err = cmn.ErrXactNoErrAbort
	} else if errAborted := cmn.AsErrAborted(err); errAborted != nil {
		err = errAborted.Unwrap()
	}
	xact.abort.mu.Lock()
	if err := xact.abort.err; err != nil {
		xact.abort.mu.Unlock()
		glog.Warningf("%s already aborted(%v)", xact.Name(), err)
		return
	}
	xact.abort.err = err
	xact.abort.ch <- err
	close(xact.abort.ch)
	xact.abort.mu.Unlock()
	if xact.Kind() != cmn.ActList {
		glog.Infof("%s aborted(%v)", xact.Name(), err)
	}
	return true
}

// count all the way to duration; reset and adjust every time activity is detected
func (xact *XactBase) Quiesce(d time.Duration, cb cluster.QuiCB) cluster.QuiRes {
	var (
		idle, total time.Duration
		sleep       = cos.CalcProbeFreq(d)
		dur         = d
	)
	if xact.Aborted() != nil {
		return cluster.QuiAborted
	}
	for idle < dur {
		time.Sleep(sleep)
		if xact.Aborted() != nil {
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

func (xact *XactBase) Name() (s string) {
	var b string
	if xact.bck != nil {
		b = "-" + xact.origBck.String()
	}
	s = "x-" + xact.Kind() + "[" + xact.ID() + "]" + b
	return
}

func (xact *XactBase) String() string {
	var (
		name = xact.Name()
		s    = name + "-" + cos.FormatTimestamp(xact.StartTime())
	)
	if !xact.Finished() { // ok to (rarely) miss _aborted_ state as this is purely informational
		return s
	}
	etime := cos.FormatTimestamp(xact.EndTime())
	if err := xact.Aborted(); err != nil {
		s += "-[abrt: " + err.Error() + "]"
	}
	return s + "-" + etime
}

func (xact *XactBase) StartTime() time.Time {
	u := xact.sutime.Load()
	if u != 0 {
		return time.Unix(0, u)
	}
	return time.Time{}
}

func (xact *XactBase) setStartTime(s time.Time) { xact.sutime.Store(s.UnixNano()) }

func (xact *XactBase) EndTime() time.Time {
	u := xact.eutime.Load()
	if u != 0 {
		return time.Unix(0, u)
	}
	return time.Time{}
}

// upon completion, all xactions:
// - atomically set end-time
// - optionally, notify listener(s)
// - optionally, refresh local capacity stats, etc.
func (xact *XactBase) notifyRefresh(err error) {
	// notifications
	if n := xact.Notif(); n != nil {
		nl.OnFinished(n, err)
	}
	xactRecord := Table[xact.kind]
	if xactRecord.RefreshCap {
		if cs, _ := fs.RefreshCapStatus(nil, nil); cs.Err != nil {
			glog.Error(cs.Err) // log warning
		}
	}

	IncInactive() // in re: HK cleanup long-time finished
}

func (xact *XactBase) Notif() (n cluster.Notif) {
	if xact.notif == nil {
		return
	}
	return xact.notif
}

func (xact *XactBase) AddNotif(n cluster.Notif) {
	xact.notif = n.(*NotifXact)
	debug.Assert(xact.notif.Xact != nil && xact.notif.F != nil)
	debug.Assert(!n.Upon(cluster.UponProgress) || xact.notif.P != nil)
}

func (xact *XactBase) Finish(err error) {
	if xact.eutime.CAS(0, 1) {
		xact.eutime.Store(time.Now().UnixNano())
		xact.notifyRefresh(err)
		if xact.Kind() != cmn.ActList {
			glog.Infof("%s finished(%v)", xact, err)
		}
	}
}

func (*XactBase) Result() (interface{}, error) {
	return nil, errors.New("getting result is not implemented")
}

// base stats: locally processed
func (xact *XactBase) Objs() int64  { return xact.stats.objs.Load() }
func (xact *XactBase) Bytes() int64 { return xact.stats.bytes.Load() }

func (xact *XactBase) ObjsAdd(cnt int, size int64) {
	xact.stats.objs.Add(int64(cnt))
	xact.stats.bytes.Add(size)
}

// base stats: transmit
func (xact *XactBase) OutObjs() int64  { return xact.stats.outobjs.Load() }
func (xact *XactBase) OutBytes() int64 { return xact.stats.outbytes.Load() }

func (xact *XactBase) OutObjsAdd(cnt int, size int64) {
	xact.stats.outobjs.Add(int64(cnt))
	if size > 0 { // not unsized
		xact.stats.outbytes.Add(size)
	}
}

// base stats: receive
func (xact *XactBase) InObjs() int64  { return xact.stats.inobjs.Load() }
func (xact *XactBase) InBytes() int64 { return xact.stats.inbytes.Load() }

func (xact *XactBase) InObjsAdd(cnt int, size int64) {
	xact.stats.inobjs.Add(int64(cnt))
	if size > 0 { // not unsized
		xact.stats.inbytes.Add(size)
	}
}

func (xact *XactBase) Snap() cluster.XactionSnap {
	snap := &Snap{}
	xact.ToSnap(snap)
	return snap
}

func (xact *XactBase) ToSnap(snap *Snap) {
	snap.ID = xact.ID()
	snap.Kind = xact.Kind()
	snap.Bck = xact.origBck
	snap.StartTime = xact.StartTime()
	snap.EndTime = xact.EndTime()
	snap.AbortedX = xact.Aborted() != nil

	xact.ToStats(&snap.Stats)
}

func (xact *XactBase) GetStats() (stats *Stats) {
	stats = &Stats{}
	xact.ToStats(stats)
	return
}

func (xact *XactBase) ToStats(stats *Stats) {
	stats.Objs = xact.Objs()         // locally processed
	stats.Bytes = xact.Bytes()       //
	stats.OutObjs = xact.OutObjs()   // transmit
	stats.OutBytes = xact.OutBytes() //
	stats.InObjs = xact.InObjs()     // receive
	stats.InBytes = xact.InBytes()
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
