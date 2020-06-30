// Package cmn provides common API constants and types, and low-level utilities for all aistore projects
/*
 * Copyright (c) 2018 - 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	timestampFormat = "15:04:05.000000"

	// Default demand xaction idle timeout: how long the xaction must live after
	// the end of the last request.
	xactIdleTimeout = 2 * time.Minute
)

type (
	XactID interface {
		String() string
		Int() int64
		Compare(string) int // -1 = less, 0 = equal, +1 = greater
	}

	Xact interface {
		Run() error
		ID() XactID
		Kind() string
		Bck() Bck
		StartTime() time.Time
		EndTime() time.Time
		ObjCount() int64
		BytesCount() int64
		String() string
		Finished() bool
		Aborted() bool
		ChanAbort() <-chan struct{}
		IsMountpathXact() bool
		Result() (interface{}, error)
		Stats() XactStats

		// modifiers
		Abort()
		AddNotif(n Notif)
	}

	XactStats interface {
		ID() string
		Kind() string
		Bck() Bck
		StartTime() time.Time
		EndTime() time.Time
		ObjCount() int64
		BytesCount() int64
		Aborted() bool
		Running() bool
		Finished() bool
	}

	XactBase struct {
		id      XactID
		sutime  atomic.Int64
		eutime  atomic.Int64
		objects atomic.Int64
		bytes   atomic.Int64
		kind    string
		bck     Bck
		abrt    chan struct{}
		aborted atomic.Bool
		notif   *NotifXact
	}

	XactBaseID string

	//
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	//
	XactDemand interface {
		Xact
		IdleTimer() <-chan time.Time
		Renew()
		IncPending()
		DecPending()
		SubPending(n int)
	}
	XactDemandBase struct {
		XactBase
		idleTime time.Duration
		timer    *time.Timer
		pending  atomic.Int64
	}
	ErrXactExpired struct { // return it if called (right) after self-termination
		msg string
	}

	// xaction notification
	NotifXact struct {
		NotifBase
		Xact Xact
	}
)

var (
	// interface guards
	_ Xact       = &XactBase{}
	_ XactStats  = &BaseXactStats{}
	_ XactDemand = &XactDemandBase{}
)

func NewErrXactExpired(msg string) *ErrXactExpired { return &ErrXactExpired{msg: msg} }
func (e *ErrXactExpired) Error() string            { return e.msg }
func IsErrXactExpired(err error) bool              { _, ok := err.(*ErrXactExpired); return ok }

func (id XactBaseID) String() string           { return string(id) }
func (id XactBaseID) Int() int64               { Assert(false); return 0 }
func (id XactBaseID) Compare(other string) int { return strings.Compare(string(id), other) }

//
// BaseXactStats
//

func (b *BaseXactStats) ID() string           { return b.IDX }
func (b *BaseXactStats) Kind() string         { return b.KindX }
func (b *BaseXactStats) Bck() Bck             { return b.BckX }
func (b *BaseXactStats) StartTime() time.Time { return b.StartTimeX }
func (b *BaseXactStats) EndTime() time.Time   { return b.EndTimeX }
func (b *BaseXactStats) ObjCount() int64      { return b.ObjCountX }
func (b *BaseXactStats) BytesCount() int64    { return b.BytesCountX }
func (b *BaseXactStats) Aborted() bool        { return b.AbortedX }
func (b *BaseXactStats) Running() bool        { return b.EndTimeX.IsZero() }
func (b *BaseXactStats) Finished() bool       { return !b.EndTimeX.IsZero() }

//
// XactBase - partially implements Xact interface
//

func NewXactBase(id XactID, kind string) *XactBase {
	Assert(kind != "")
	xact := &XactBase{id: id, kind: kind, abrt: make(chan struct{})}
	xact.setStartTime(time.Now())
	return xact
}
func NewXactBaseWithBucket(id, kind string, bck Bck) *XactBase {
	xact := NewXactBase(XactBaseID(id), kind)
	xact.bck = bck
	return xact
}

func (xact *XactBase) Run() error                 { Assert(false); return nil }
func (xact *XactBase) ID() XactID                 { return xact.id }
func (xact *XactBase) Kind() string               { return xact.kind }
func (xact *XactBase) Bck() Bck                   { return xact.bck }
func (xact *XactBase) Finished() bool             { return xact.eutime.Load() != 0 }
func (xact *XactBase) ChanAbort() <-chan struct{} { return xact.abrt }
func (xact *XactBase) Aborted() bool              { return xact.aborted.Load() }

func (xact *XactBase) String() string {
	var (
		prefix = xact.Kind()
	)
	if xact.bck.Name != "" {
		prefix += "@" + xact.bck.Name
	}
	if !xact.Finished() {
		return fmt.Sprintf("%s(%q)", prefix, xact.ID())
	}
	var (
		stime    = xact.StartTime()
		stimestr = stime.Format(timestampFormat)
		etime    = xact.EndTime()
		d        = etime.Sub(stime)
	)
	return fmt.Sprintf("%s(%q) started %s ended %s (%v)",
		prefix, xact.ID(), stimestr, etime.Format(timestampFormat), d)
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
func (xact *XactBase) setEndTime() {
	xact.eutime.Store(time.Now().UnixNano())
	if xact.Kind() != ActListObjects {
		glog.Infoln(xact.String())
	}
}

func (xact *XactBase) Notif() (n Notif) {
	if xact.notif == nil {
		return
	}
	return xact.notif
}

func (xact *XactBase) AddNotif(n Notif) {
	var ok bool
	Assert(xact.notif == nil) // currently, "add" means "set"
	xact.notif, ok = n.(*NotifXact)
	Assert(ok)
	xact.notif.Xact = xact
	Assert(xact.notif.F != nil)
}

func (xact *XactBase) Abort() {
	if !xact.aborted.CAS(false, true) {
		glog.Infof("already aborted: " + xact.String())
		return
	}
	xact.setEndTime()
	close(xact.abrt)
	glog.Infof("ABORT: " + xact.String())
}

func (xact *XactBase) Finish(errs ...error) {
	xact.setEndTime()

	// notifications
	if n := xact.Notif(); n != nil && n.Upon(UponTerm) {
		var err error
		if len(errs) > 0 {
			err = errs[0]
		}
		n.Callback(n, err)
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

func (xact *XactBase) IsMountpathXact() bool { Assert(false); return true } // must implement

func (xact *XactBase) Stats() XactStats {
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

//
// XactDemandBase - partially implements XactDemand interface
//

func NewXactDemandBase(kind string, bck Bck, idleTimes ...time.Duration) *XactDemandBase {
	idleTime := xactIdleTimeout
	if len(idleTimes) != 0 {
		idleTime = idleTimes[0]
	}
	return &XactDemandBase{
		XactBase: *NewXactBaseWithBucket("", kind, bck),
		idleTime: idleTime,
		timer:    time.NewTimer(idleTime),
	}
}

func (r *XactDemandBase) IdleTimer() <-chan time.Time { return r.timer.C }
func (r *XactDemandBase) Renew() {
	pending := r.Pending()
	debug.Assert(pending >= 0)
	if pending == 0 {
		// If there are no requests yet and renew was issued then we will wait
		// `r.idleTime` for some request to come.
		r.timer.Reset(r.idleTime)
	}
}
func (r *XactDemandBase) IncPending() {
	if pending := r.pending.Inc(); pending == 1 {
		// Stop the timer on the first request. It will be restarted once all
		// jobs finish (see: `SubPending`).
		r.timer.Stop()
	}
}
func (r *XactDemandBase) DecPending() { r.SubPending(1) }
func (r *XactDemandBase) SubPending(n int) {
	pending := r.pending.Sub(int64(n))
	debug.Assert(pending >= 0)
	r.Renew()
}
func (r *XactDemandBase) Pending() int64 { return r.pending.Load() }
func (r *XactDemandBase) Stop() {
	r.timer.Stop()
}

func IsValidXaction(kind string) bool {
	_, ok := XactsMeta[kind]
	return ok
}

func IsXactTypeBck(kind string) bool {
	return XactsMeta[kind].Type == XactTypeBck
}
