// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
)

type (
	Base struct {
		notif *NotifXact
		bck   meta.Bck
		abort struct {
			ch     chan error
			err    ratomic.Pointer[error]
			done   atomic.Bool
			closed atomic.Bool
		}
		id     string
		kind   string
		_nam   string
		ctlmsg string // via InitBase, SetCtlMsg
		err    cos.Errs
		stats  struct {
			objs     atomic.Int64 // locally processed
			bytes    atomic.Int64
			outobjs  atomic.Int64 // transmit
			outbytes atomic.Int64
			inobjs   atomic.Int64 // receive
			inbytes  atomic.Int64
		}
		sutime atomic.Int64
		eutime atomic.Int64
	}
	Marked struct {
		Xact        core.Xact
		Interrupted bool // (rebalance | resilver) interrupted
		Restarted   bool // node restarted
	}
)

var IncFinished func()

// common helper to go-run and wait until it actually starts running
func GoRunW(xctn core.Xact) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go xctn.Run(wg)
	wg.Wait()
}

//////////////
// Base - partially implements `core.Xact` interface
//////////////

func (xctn *Base) InitBase(id, kind, ctlmsg string, bck *meta.Bck) {
	debug.Assert(kind == apc.ActETLInline || cos.IsValidUUID(id) || IsValidRebID(id), id)
	debug.Assert(IsValidKind(kind), kind)

	xctn.id, xctn.kind = id, kind
	xctn.ctlmsg = ctlmsg

	xctn.abort.ch = make(chan error, 1)
	if bck != nil {
		xctn.bck = *bck
	}
	xctn.setStartTime(time.Now())

	// name never changes
	xctn._nam = "x-" + xctn.Kind() + LeftID + xctn.ID() + RightID
	if !xctn.bck.IsEmpty() {
		xctn._nam += "-" + xctn.bck.Cname("")
	}
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

func (xctn *Base) AbortErr() error {
	if !xctn.IsAborted() {
		return nil
	}
	// (is aborted)
	// normally, is expected to return `abort.err` without any sleep
	// but may also poll up to 4 times for 1s total
	const wait = time.Second
	sleep := cos.ProbingFrequency(wait)
	for elapsed := time.Duration(0); elapsed < wait; elapsed += sleep {
		perr := xctn.abort.err.Load()
		if perr != nil {
			return *perr
		}
		time.Sleep(sleep)
	}
	return cmn.NewErrAborted(xctn.Name(), "base.abort-err.timeout", nil)
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

func (xctn *Base) Abort(err error) bool {
	if xctn.Finished() || !xctn.abort.done.CAS(false, true) {
		return false
	}

	if err == nil {
		err = cmn.ErrXactUserAbort // NOTE: only user can cause no-errors abort
	} else if errAborted := cmn.AsErrAborted(err); errAborted != nil {
		if errCause := errAborted.Unwrap(); errCause != nil {
			err = errCause
		}
	}
	perr := xctn.abort.err.Swap(&err)
	debug.Assert(perr == nil, xctn.String())
	debug.Assert(len(xctn.abort.ch) == 0, xctn.String()) // CAS above

	xctn.abort.ch <- err
	if xctn.abort.closed.CAS(false, true) {
		close(xctn.abort.ch)
	}

	if xctn.Kind() != apc.ActList {
		nlog.InfoDepth(1, xctn.Name(), err)
	}
	return true
}

// atomically set end-time
func (xctn *Base) Finish() {
	var (
		err     error
		info    string
		aborted bool
	)
	if !xctn.eutime.CAS(0, 1) {
		return
	}
	xctn.eutime.Store(time.Now().UnixNano())
	if aborted = xctn.IsAborted(); aborted {
		if perr := xctn.abort.err.Load(); perr != nil {
			err = *perr
		}
	}

	if xctn.abort.closed.CAS(false, true) {
		close(xctn.abort.ch)
	}

	if xctn.ErrCnt() > 0 {
		if err == nil {
			debug.Assert(!aborted)
			err = xctn.Err()
		} else {
			// abort takes precedence
			info = "(" + xctn.Err().Error() + ")"
		}
	}
	xctn.onFinished(err, aborted)
	// log
	switch {
	case xctn.Kind() == apc.ActList:
	case err == nil:
		nlog.Infoln(xctn.String(), "finished")
	case aborted:
		nlog.Warningln(xctn.String(), "aborted:", err, info)
	default:
		nlog.Warningln(xctn.String(), "finished w/err:", err)
	}
}

//
// multi-error
//

func (xctn *Base) AddErr(err error, logExtra ...int) {
	if xctn.IsAborted() { // no more errors once aborted
		return
	}
	debug.Assert(err != nil)

	xctn.err.Add(err)
	// just add
	if len(logExtra) == 0 {
		return
	}
	// log error
	level := logExtra[0]
	if level == 0 {
		nlog.ErrorDepth(1, err)
		return
	}
	// finally, FastV
	module := logExtra[1]
	if cmn.Rom.FastV(level, module) {
		nlog.InfoDepth(1, "Warning:", err)
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
func (xctn *Base) Quiesce(d time.Duration, cb core.QuiCB) core.QuiRes {
	var (
		idle, total time.Duration
		sleep       = cos.ProbingFrequency(d)
		dur         = d
	)
	if xctn.IsAborted() {
		return core.QuiAborted
	}
	for idle < dur {
		time.Sleep(sleep)
		if xctn.IsAborted() {
			return core.QuiAborted
		}
		total += sleep
		switch qui := cb(total); qui {
		case core.QuiInactiveCB: // used by callbacks, converts to one of the returned codes
			idle += sleep
		case core.QuiActive:
			idle = 0                   // reset
			dur = min(dur+sleep, d<<1) // bump inactivity duration (cannot increase beyond 2x initial)
		case core.QuiActiveDontBump: //       reset, don't bump
			idle = 0
		case core.QuiActiveRet:
			return core.QuiActiveRet
		case core.QuiDone:
			return core.QuiDone
		case core.QuiTimeout:
			return core.QuiTimeout
		case core.QuiAborted:
			return core.QuiAborted
		}
	}
	return core.Quiescent
}

func (xctn *Base) Cname() string { return Cname(xctn.Kind(), xctn.ID()) }

func (xctn *Base) Name() (s string) { return xctn._nam }

func (xctn *Base) String() string {
	var (
		sb strings.Builder
		l  = 256
	)
	sb.Grow(l)

	sb.WriteString(xctn._nam)
	sb.WriteByte('-')
	sb.WriteString(cos.FormatTime(xctn.StartTime(), cos.StampMicro))

	if !xctn.Finished() { // ok to (rarely) miss _aborted_ state as this is purely informational
		return sb.String()
	}
	etime := cos.FormatTime(xctn.EndTime(), cos.StampMicro)
	if xctn.IsAborted() {
		sb.WriteString(fmt.Sprintf("-[abrt: %v]", xctn.AbortErr()))
	}
	sb.WriteByte('-')
	sb.WriteString(etime)

	return sb.String()
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
func (xctn *Base) onFinished(err error, aborted bool) {
	// notifications
	if xctn.notif != nil {
		nl.OnFinished(xctn.notif, err, aborted)
	}
	xactRecord := Table[xctn.kind]
	if xactRecord.RefreshCap {
		// currently, ignoring returned err-cap and not calling t.OOS()
		// both (conditions) handled by periodic stats
		fs.CapRefresh(cmn.GCO.Get(), nil /*tcdf*/)
	}

	IncFinished() // in re: HK cleanup long-time finished
}

func (xctn *Base) AddNotif(n core.Notif) {
	xctn.notif = n.(*NotifXact)
	debug.Assert(xctn.notif.Xact != nil && xctn.notif.F != nil)     // always fin-notif and points to self
	debug.Assert(!n.Upon(core.UponProgress) || xctn.notif.P != nil) // progress notification is optional
}

// base stats: locally processed
func (xctn *Base) Objs() int64  { return xctn.stats.objs.Load() }
func (xctn *Base) Bytes() int64 { return xctn.stats.bytes.Load() }

func (xctn *Base) ObjsAdd(cnt int, size int64) {
	xctn.stats.objs.Add(int64(cnt))
	xctn.stats.bytes.Add(size)
}

// oft. used
func (xctn *Base) LomAdd(lom *core.LOM) { xctn.ObjsAdd(1, lom.Lsize(true)) }

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
func (xctn *Base) ToSnap(snap *core.Snap) {
	snap.ID = xctn.ID()
	snap.Kind = xctn.Kind()
	snap.CtlMsg = xctn.ctlmsg
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

func (xctn *Base) ToStats(stats *core.Stats) {
	stats.Objs = xctn.Objs()         // locally processed
	stats.Bytes = xctn.Bytes()       //
	stats.OutObjs = xctn.OutObjs()   // transmit
	stats.OutBytes = xctn.OutBytes() //
	stats.InObjs = xctn.InObjs()     // receive
	stats.InBytes = xctn.InBytes()
}

func (xctn *Base) SetCtlMsg(s string) { xctn.ctlmsg = s } // see InitBase

//
// RebID helpers
//

func RebID2S(id int64) string          { return "g" + strconv.FormatInt(id, 10) }
func S2RebID(id string) (int64, error) { return strconv.ParseInt(id[1:], 10, 64) }

func IsValidRebID(id string) (valid bool) {
	if len(id) > 1 {
		_, err := S2RebID(id)
		valid = err == nil
	}
	return valid
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
