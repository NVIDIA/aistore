// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
)

type (
	Base struct {
		notif  *NotifXact
		bck    meta.Bck
		id     string
		kind   string
		sutime atomic.Int64
		eutime atomic.Int64
		abort  struct {
			ch   chan error
			err  error
			mu   sync.RWMutex
			done atomic.Bool
		}
		stats struct {
			objs     atomic.Int64 // locally processed
			bytes    atomic.Int64
			outobjs  atomic.Int64 // transmit
			outbytes atomic.Int64
			inobjs   atomic.Int64 // receive
			inbytes  atomic.Int64
		}
		err cos.Errs
	}
	Marked struct {
		Xact        cluster.Xact
		Interrupted bool // (rebalance | resilver) interrupted
		Restarted   bool // node restarted
	}
)

var IncFinished func()

// common helper to go-run and wait until it actually starts running
func GoRunW(xctn cluster.Xact) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go xctn.Run(wg)
	wg.Wait()
}

func IsValidUUID(id string) bool { return cos.IsValidUUID(id) || IsValidRebID(id) }

//////////////
// Base - partially implements `cluster.Xact` interface
//////////////

func (xctn *Base) InitBase(id, kind string, bck *meta.Bck) {
	debug.Assert(kind == apc.ActETLInline || cos.IsValidUUID(id) || IsValidRebID(id), id)
	debug.Assert(IsValidKind(kind), kind)
	xctn.id, xctn.kind = id, kind
	xctn.abort.ch = make(chan error, 1)
	if bck != nil {
		xctn.bck = *bck
	}
	xctn.setStartTime(time.Now())
}

func (xctn *Base) ID() string   { return xctn.id }
func (xctn *Base) Kind() string { return xctn.kind }

func (xctn *Base) Bck() *meta.Bck { return &xctn.bck }

func (xctn *Base) Finished() bool { return xctn.eutime.Load() != 0 }

func (xctn *Base) Running() (yes bool) {
	yes = xctn.sutime.Load() != 0 && !xctn.Finished() && !xctn.IsAborted()
	debug.Assert(!yes || xctn.ID() != "", xctn.String())
	return
}

func (xctn *Base) IsIdle() bool { return !xctn.Running() }

func (*Base) FromTo() (*meta.Bck, *meta.Bck) { return nil, nil }

//
// aborting
//

func (xctn *Base) ChanAbort() <-chan error { return xctn.abort.ch }

func (xctn *Base) IsAborted() bool { return xctn.abort.done.Load() }

func (xctn *Base) AbortErr() (err error) {
	if !xctn.IsAborted() {
		return
	}
	// (is aborted)
	// normally, is expected to return `abort.err` without any sleep
	// but may also poll up to 4 times for 1s total
	const wait = time.Second
	sleep := cos.ProbingFrequency(wait)
	for elapsed := time.Duration(0); elapsed < wait; elapsed += sleep {
		xctn.abort.mu.RLock()
		err = xctn.abort.err
		xctn.abort.mu.RUnlock()
		if err != nil {
			return
		}
		time.Sleep(sleep)
	}
	return
}

func (xctn *Base) AbortedAfter(d time.Duration) (err error) {
	sleep := cos.ProbingFrequency(d)
	for elapsed := time.Duration(0); elapsed < d; elapsed += sleep {
		if err = xctn.AbortErr(); err != nil {
			break
		}
		time.Sleep(sleep)
	}
	return
}

func (xctn *Base) Abort(err error) (ok bool) {
	if xctn.Finished() || !xctn.abort.done.CAS(false, true) {
		return
	}

	if err == nil {
		err = cmn.ErrXactUserAbort // NOTE: only user can cause no-errors abort
	} else if errAborted := cmn.AsErrAborted(err); errAborted != nil {
		if errCause := errAborted.Unwrap(); errCause != nil {
			err = errCause
		}
	}
	xctn.abort.mu.Lock()
	debug.Assert(xctn.abort.err == nil, xctn.String())
	xctn.abort.err = err
	xctn.abort.ch <- err
	close(xctn.abort.ch)
	xctn.abort.mu.Unlock()

	if xctn.Kind() != apc.ActList {
		nlog.Infof("%s aborted(%v)", xctn.Name(), err)
	}
	return true
}

//
// multi-error
//

func (xctn *Base) AddErr(err error) {
	if err != nil && !xctn.IsAborted() { // no more errors once aborted
		fs.CleanPathErr(err)
		xctn.err.Add(err)
	}
}

func (xctn *Base) Err() error {
	if xctn.ErrCnt() == 0 {
		return nil
	}
	return &xctn.err
}

func (xctn *Base) JoinErr() (int, error) { return xctn.err.JoinErr() }
func (xctn *Base) ErrCnt() int           { return xctn.err.Cnt() }

// count all the way to duration; reset and adjust every time activity is detected
func (xctn *Base) Quiesce(d time.Duration, cb cluster.QuiCB) cluster.QuiRes {
	var (
		idle, total time.Duration
		sleep       = cos.ProbingFrequency(d)
		dur         = d
	)
	if xctn.IsAborted() {
		return cluster.QuiAborted
	}
	for idle < dur {
		time.Sleep(sleep)
		if xctn.IsAborted() {
			return cluster.QuiAborted
		}
		total += sleep
		switch res := cb(total); res {
		case cluster.QuiInactiveCB: // NOTE: used by callbacks, converts to one of the returned codes
			idle += sleep
		case cluster.QuiActive:
			idle = 0                  // reset
			dur = min(dur+sleep, 2*d) // bump up to 2x initial
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
	if !xctn.bck.IsEmpty() {
		b = "-" + xctn.bck.String()
	}
	s = "x-" + xctn.Kind() + "[" + xctn.ID() + "]" + b
	return
}

func (xctn *Base) String() string {
	var (
		name = xctn.Name()
		s    = name + "-" + cos.FormatTime(xctn.StartTime(), cos.StampMicro)
	)
	if !xctn.Finished() { // ok to (rarely) miss _aborted_ state as this is purely informational
		return s
	}
	etime := cos.FormatTime(xctn.EndTime(), cos.StampMicro)
	if xctn.IsAborted() {
		s = fmt.Sprintf("%s-[abrt: %v]", s, xctn.AbortErr())
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

// upon completion, all xactions optionally notify listener(s) and refresh local capacity stats
func (xctn *Base) onFinished(err error) {
	// notifications
	if xctn.notif != nil {
		nl.OnFinished(xctn.notif, err)
	}
	xactRecord := Table[xctn.kind]
	if xactRecord.RefreshCap {
		if cs, _ := fs.CapRefresh(nil, nil); cs.Err != nil {
			nlog.Errorln(cs.Err) // log warning
		}
	}

	IncFinished() // in re: HK cleanup long-time finished
}

func (xctn *Base) AddNotif(n cluster.Notif) {
	xctn.notif = n.(*NotifXact)
	debug.Assert(xctn.notif.Xact != nil && xctn.notif.F != nil)        // always fin-notif and points to self
	debug.Assert(!n.Upon(cluster.UponProgress) || xctn.notif.P != nil) // progress notification is optional
}

// atomically set end-time
func (xctn *Base) Finish() {
	var (
		err, infoErr error
		aborted      bool
	)
	if !xctn.eutime.CAS(0, 1) {
		return
	}
	xctn.eutime.Store(time.Now().UnixNano())
	if aborted = xctn.IsAborted(); aborted {
		xctn.abort.mu.RLock()
		err = xctn.abort.err
		xctn.abort.mu.RUnlock()
	}
	if xctn.ErrCnt() > 0 {
		if err == nil {
			debug.Assert(!aborted)
			err = xctn.Err()
		} else {
			infoErr = xctn.Err() // abort takes precedence
		}
	}
	xctn.onFinished(err)
	// log
	if xctn.Kind() == apc.ActList {
		return
	}
	if err == nil {
		nlog.Infof("%s finished", xctn)
	} else if aborted {
		nlog.Warningf("%s aborted: %v (%v)", xctn, err, infoErr)
	} else {
		nlog.Warningf("%s finished w/err: %v", xctn, infoErr)
	}
}

func (*Base) Result() (any, error) {
	return nil, errors.New("getting result is not implemented")
}

// base stats: locally processed
func (xctn *Base) Objs() int64  { return xctn.stats.objs.Load() }
func (xctn *Base) Bytes() int64 { return xctn.stats.bytes.Load() }

func (xctn *Base) ObjsAdd(cnt int, size int64) {
	xctn.stats.objs.Add(int64(cnt))
	xctn.stats.bytes.Add(size)
}

// oft. used
func (xctn *Base) LomAdd(lom *cluster.LOM) { xctn.ObjsAdd(1, lom.SizeBytes(true)) }

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
	debug.Assert(size >= 0, xctn.String()) // "unsized" is caller's responsibility
	xctn.stats.inobjs.Add(int64(cnt))
	xctn.stats.inbytes.Add(size)
}

// provided for external use to fill-in xaction-specific `SnapExt` part
func (xctn *Base) ToSnap(snap *cluster.Snap) {
	snap.ID = xctn.ID()
	snap.Kind = xctn.Kind()
	snap.StartTime = xctn.StartTime()
	snap.EndTime = xctn.EndTime()
	if err := xctn.AbortErr(); err != nil {
		snap.AbortErr = err.Error()
		snap.AbortedX = true
	}
	snap.Err = xctn.err.Error() // TODO: a (verbose) option to respond with xctn.err.JoinErr() :NOTE
	if b := xctn.Bck(); b != nil {
		snap.Bck = b.Clone()
	}

	// counters
	xctn.ToStats(&snap.Stats)
}

func (xctn *Base) ToStats(stats *cluster.Stats) {
	stats.Objs = xctn.Objs()         // locally processed
	stats.Bytes = xctn.Bytes()       //
	stats.OutObjs = xctn.OutObjs()   // transmit
	stats.OutBytes = xctn.OutBytes() //
	stats.InObjs = xctn.InObjs()     // receive
	stats.InBytes = xctn.InBytes()
}

func (xctn *Base) InMaintOrDecomm(smap *meta.Smap, tsi *meta.Snode) (err error) {
	if smap.InMaintOrDecomm(tsi) {
		err = cmn.NewErrXactTgtInMaint(xctn.String(), tsi.String())
	}
	return
}

// RebID helpers

func RebID2S(id int64) string          { return fmt.Sprintf("g%d", id) }
func S2RebID(id string) (int64, error) { return strconv.ParseInt(id[1:], 10, 64) }

func IsValidRebID(id string) (valid bool) {
	if len(id) > 1 {
		_, err := S2RebID(id)
		valid = err == nil
	}
	return
}

func CompareRebIDs(someID, fltID string) int {
	ai, err := S2RebID(someID)
	if err != nil {
		return -1 // m.b. less than
	}
	bi, err := S2RebID(fltID)
	debug.Assert(err == nil, fltID)
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}
