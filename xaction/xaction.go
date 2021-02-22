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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
)

type (
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

	XactBaseID string

	RebID int64

	XactMarked struct {
		Xact        cluster.Xact
		Interrupted bool
	}

	ErrXactExpired struct { // return it if called (right) after self-termination
		msg string
	}
)

var (
	_ cluster.XactID = XactBaseID("")
	_ cluster.XactID = RebID(0)
)

func (id RebID) String() string { return fmt.Sprintf("g%d", id) }

func (id RebID) Int() int64 { return int64(id) }
func (id RebID) Compare(other string) int {
	var (
		o   int64
		err error
	)
	if o, err = strconv.ParseInt(other, 10, 64); err == nil {
		goto compare
	} else if o, err = strconv.ParseInt(other[1:], 10, 64); err == nil {
		goto compare
	} else {
		return -1
	}
compare:
	if int64(id) < o {
		return -1
	}
	if int64(id) > o {
		return 1
	}
	return 0
}

func NewXactBase(id cluster.XactID, kind string) *XactBase {
	cmn.Assert(kind != "")
	xact := &XactBase{id: id, kind: kind, abrt: make(chan struct{})}
	xact.setStartTime(time.Now())
	return xact
}

func NewXactBaseBck(id, kind string, bck cmn.Bck) *XactBase {
	xact := NewXactBase(XactBaseID(id), kind)
	xact.bck = bck
	return xact
}

//
// XactBase - partially implements Xact interface
//

func (xact *XactBase) ID() cluster.XactID         { return xact.id }
func (xact *XactBase) Kind() string               { return xact.kind }
func (xact *XactBase) Bck() cmn.Bck               { return xact.bck }
func (xact *XactBase) Finished() bool             { return xact.eutime.Load() != 0 }
func (xact *XactBase) ChanAbort() <-chan struct{} { return xact.abrt }
func (xact *XactBase) Aborted() bool              { return xact.aborted.Load() }

func (xact *XactBase) AbortedAfter(d time.Duration) (aborted bool) {
	sleep := cmn.CalcProbeFreq(d)
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
		sleep       = cmn.CalcProbeFreq(d)
		dur         = d
	)
	if xact.Aborted() {
		return cluster.QuiAborted
	}
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
			dur = cmn.MinDuration(dur+sleep, 2*d) // bump up to 2x initial
		case cluster.QuiDone:
			return cluster.QuiDone
		case cluster.QuiTimeout:
			break
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
		stimestr = cmn.FormatTimestamp(stime)
		etime    = xact.EndTime()
		d        = etime.Sub(stime)
	)
	return fmt.Sprintf("%s(%q) started %s ended %s (%v)",
		prefix, xact.ID(), stimestr, cmn.FormatTimestamp(etime), d)
}

func (xact *XactBase) StartTime() time.Time {
	u := xact.sutime.Load()
	if u != 0 {
		return time.Unix(0, u)
	}
	return time.Time{}
}

func (xact *XactBase) setStartTime(s time.Time) {
	xact.sutime.Store(s.UnixNano())
}

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

	if xact.Kind() != cmn.ActListObjects || !cmn.IsErrAborted(err) {
		debug.Infof("%s finished, err: %v", xact, err)
		if err != nil {
			glog.Errorf("%s finished with err: %v", xact, err)
		}
	}

	xactDtor := XactsDtor[xact.kind]
	if xactDtor.RefreshCap {
		if cs, _ := fs.RefreshCapStatus(nil, nil); cs.Err != nil {
			glog.Error(cs.Err)
		}
	}
}

func (xact *XactBase) Notif() (n cluster.Notif) {
	if xact.notif == nil {
		return
	}
	return xact.notif
}

func (xact *XactBase) AddNotif(n cluster.Notif) {
	var ok bool
	cmn.Assert(xact.notif == nil) // currently, "add" means "set"
	xact.notif, ok = n.(*NotifXact)
	cmn.Assert(ok)
	cmn.Assert(xact.notif.Xact != nil)
	cmn.Assert(xact.notif.F != nil)
	if n.Upon(cluster.UponProgress) {
		cmn.Assert(xact.notif.P != nil)
	}
}

// TODO: Consider moving it to separate interface.
func (xact *XactBase) Renew() {}

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

func (xact *XactBase) Result() (interface{}, error) {
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

func (id XactBaseID) String() string           { return string(id) }
func (id XactBaseID) Int() int64               { cmn.Assert(false); return 0 }
func (id XactBaseID) Compare(other string) int { return strings.Compare(string(id), other) }

// errors

func NewErrXactExpired(msg string) *ErrXactExpired { return &ErrXactExpired{msg: msg} }
func (e *ErrXactExpired) Error() string            { return e.msg }
func IsErrXactExpired(err error) bool              { _, ok := err.(*ErrXactExpired); return ok }
