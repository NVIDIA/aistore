// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	Args struct {
		ID   cluster.XactID
		Kind string
		Bck  *cmn.Bck
	}
	XactBase struct {
		id      cluster.XactID
		sutime  atomic.Int64
		eutime  atomic.Int64
		objects atomic.Int64
		bytes   atomic.Int64
		kind    string
		bck     cmn.Bck
		abrt    chan struct{}
		aborted atomic.Bool
		notif   *NotifXact
	}
	XactMarked struct {
		Xact        cluster.Xact
		Interrupted bool
	}
	ErrXactExpired struct { // return it if called (right) after self-termination
		msg string
	}

	BaseID string
	RebID  int64
)

var (
	_ cluster.XactID = BaseID("")
	_ cluster.XactID = RebID(0)
)

var IncInactive func()

///////////
// RebID //
///////////

func (id RebID) String() string { return fmt.Sprintf("g%d", id) }
func (id RebID) Int() int64     { return int64(id) }

func (id RebID) Compare(other string) int {
	var (
		o   int64
		err error
	)
	if o, err = strconv.ParseInt(other, 10, 64); err != nil {
		if o, err = strconv.ParseInt(other[1:], 10, 64); err != nil {
			return -1
		}
	}
	if int64(id) < o {
		return -1
	}
	if int64(id) > o {
		return 1
	}
	return 0
}

//////////////
// XactBase - partially implements Xact interface
//////////////

func NewXactBase(args Args) (xact *XactBase) {
	debug.Assert(args.Kind != "")
	debug.Assertf(args.ID != BaseID(""), "%v", args)
	xact = &XactBase{id: args.ID, kind: args.Kind, abrt: make(chan struct{})}
	if args.Bck != nil {
		xact.bck = *args.Bck
	}
	xact.setStartTime(time.Now())
	return
}

func (xact *XactBase) ID() cluster.XactID         { return xact.id }
func (xact *XactBase) Kind() string               { return xact.kind }
func (xact *XactBase) Bck() cmn.Bck               { return xact.bck }
func (xact *XactBase) Finished() bool             { return xact.eutime.Load() != 0 }
func (xact *XactBase) ChanAbort() <-chan struct{} { return xact.abrt }
func (xact *XactBase) Aborted() bool              { return xact.aborted.Load() }

func (xact *XactBase) AbortedAfter(d time.Duration) (aborted bool) {
	sleep := cos.CalcProbeFreq(d)
	aborted = xact.Aborted()
	for elapsed := time.Duration(0); elapsed < d && !aborted; elapsed += sleep {
		time.Sleep(sleep)
		aborted = xact.Aborted()
	}
	return
}

// count all the way to duration; reset and adjust every time activity is detected
func (xact *XactBase) Quiesce(d time.Duration, cb cluster.QuiCB) cluster.QuiRes {
	var (
		idle, total time.Duration
		sleep       = cos.CalcProbeFreq(d)
		dur         = d
	)
	if xact.Aborted() {
		return cluster.QuiAborted
	}
wait:
	for idle < dur {
		time.Sleep(sleep)
		if xact.Aborted() {
			return cluster.QuiAborted
		}
		total += sleep
		switch res := cb(total); res {
		case cluster.QuiInactive:
			idle += sleep
		case cluster.QuiActive:
			idle = 0                              // reset
			dur = cos.MinDuration(dur+sleep, 2*d) // bump up to 2x initial
		case cluster.QuiDone:
			return cluster.QuiDone
		case cluster.QuiTimeout:
			break wait
		}
	}
	return cluster.QuiTimeout
}

func (xact *XactBase) String() string {
	prefix := xact.Kind()
	if xact.bck.Name != "" {
		prefix += "@" + xact.bck.Name
	}
	if !xact.Finished() {
		return fmt.Sprintf("%s(%q)", prefix, xact.ID())
	}
	var (
		stime    = xact.StartTime()
		stimestr = cos.FormatTimestamp(stime)
		etime    = xact.EndTime()
		d        = etime.Sub(stime)
	)
	return fmt.Sprintf("%s(%q) started %s ended %s (%v)",
		prefix, xact.ID(), stimestr, cos.FormatTimestamp(etime), d)
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
func (xact *XactBase) _setEndTime(err error) {
	xact.eutime.Store(time.Now().UnixNano())

	// notifications
	if n := xact.Notif(); n != nil {
		nl.OnFinished(n, err)
	}
	xactDtor := XactsDtor[xact.kind]
	if xactDtor.RefreshCap {
		if cs, _ := fs.RefreshCapStatus(nil, nil); cs.Err != nil {
			glog.Error(cs.Err) // log warning
		}
	}

	IncInactive()
}

func (xact *XactBase) Notif() (n cluster.Notif) {
	if xact.notif == nil {
		return
	}
	return xact.notif
}

func (xact *XactBase) AddNotif(n cluster.Notif) {
	debug.Assert(xact.notif == nil) // currently, "add" means "set"
	xact.notif = n.(*NotifXact)
	debug.Assert(xact.notif.Xact != nil && xact.notif.F != nil)
	debug.Assert(!n.Upon(cluster.UponProgress) || xact.notif.P != nil)
}

func (*XactBase) Renew() {}

func (xact *XactBase) Abort() {
	if !xact.aborted.CAS(false, true) {
		glog.Infof("already aborted: " + xact.String())
		return
	}
	close(xact.abrt)
	glog.Infof("ABORT: " + xact.String())
}

func (xact *XactBase) Finish(err error) {
	if xact.eutime.Load() == 0 {
		xact._setEndTime(err)
	}
}

func (*XactBase) Result() (interface{}, error) {
	return nil, errors.New("getting result is not implemented")
}

func (xact *XactBase) ObjCount() int64            { return xact.objects.Load() }
func (xact *XactBase) ObjectsInc() int64          { return xact.objects.Inc() }
func (xact *XactBase) ObjectsAdd(cnt int64) int64 { return xact.objects.Add(cnt) }
func (xact *XactBase) BytesCount() int64          { return xact.bytes.Load() }
func (xact *XactBase) BytesAdd(size int64) int64  { return xact.bytes.Add(size) }

func (xact *XactBase) Stats() cluster.XactStats {
	return &BaseXactStats{
		IDX:         xact.ID().String(),
		KindX:       xact.Kind(),
		StartTimeX:  xact.StartTime(),
		EndTimeX:    xact.EndTime(),
		BckX:        xact.Bck(),
		ObjCountX:   xact.ObjCount(),
		BytesCountX: xact.BytesCount(),
		AbortedX:    xact.Aborted(),
	}
}

func (id BaseID) String() string           { return string(id) }
func (BaseID) Int() int64                  { cos.Assert(false); return 0 }
func (id BaseID) Compare(other string) int { return strings.Compare(string(id), other) }

// errors

func NewErrXactExpired(msg string) *ErrXactExpired { return &ErrXactExpired{msg: msg} }
func (e *ErrXactExpired) Error() string            { return e.msg }
func IsErrXactExpired(err error) bool              { _, ok := err.(*ErrXactExpired); return ok }
