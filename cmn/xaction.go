// Package cmn provides common API constants and types, and low-level utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const timeStampFormat = "15:04:05.000000"

const xactIdleTimeout = time.Minute * 3

type (
	Xact interface {
		XactCountStats
		ID() int64
		Kind() string
		Bucket() string
		StartTime(s ...time.Time) time.Time
		EndTime(e ...time.Time) time.Time
		String() string
		Abort()
		ChanAbort() <-chan struct{}
		Finished() bool
		Aborted() bool
		IsMountpathXact() bool
		Description() string
	}
	XactBase struct {
		XactBaseCountStats
		id       int64
		sutime   atomic.Int64
		eutime   atomic.Int64
		kind     string
		bucket   string
		abrt     chan struct{}
		aborted  atomic.Bool
		bckIsAIS bool
	}
	//
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	//
	XactDemand interface {
		Xact
		ChanCheckTimeout() <-chan time.Time
		Renew()
		Timeout() bool
		IncPending()
		DecPending()
	}
	XactDemandBase struct {
		XactBase
		ticker  *time.Ticker
		renew   atomic.Int64
		pending atomic.Int64
	}
	ErrXpired struct { // return it if called (right) after self-termination
		msg string
	}

	MountpathXact    struct{}
	NonmountpathXact struct{}
)

func (e *ErrXpired) Error() string       { return e.msg }
func NewErrXpired(msg string) *ErrXpired { return &ErrXpired{msg: msg} }

//
// XactBase - partially implements Xact interface
//

func NewXactBase(id int64, kind string) *XactBase {
	stime := time.Now()
	xact := &XactBase{id: id, kind: kind, abrt: make(chan struct{})}
	xact.sutime.Store(stime.UnixNano())
	return xact
}
func NewXactBaseWithBucket(id int64, kind string, bucket string, bckIsAIS bool) *XactBase {
	xact := NewXactBase(id, kind)
	xact.bucket, xact.bckIsAIS = bucket, bckIsAIS
	return xact
}

func (xact *XactBase) ID() int64                  { return xact.id }
func (xact *XactBase) ShortID() uint32            { return ShortID(xact.id) }
func (xact *XactBase) Kind() string               { return xact.kind }
func (xact *XactBase) Bucket() string             { return xact.bucket }
func (xact *XactBase) BckIsAIS() bool             { return xact.bckIsAIS }
func (xact *XactBase) Finished() bool             { return xact.eutime.Load() != 0 }
func (xact *XactBase) ChanAbort() <-chan struct{} { return xact.abrt }
func (xact *XactBase) Aborted() bool              { return xact.aborted.Load() }

func (xact *XactBase) String() string {
	stime := xact.StartTime()
	stimestr := stime.Format(timeStampFormat)
	if !xact.Finished() {
		return fmt.Sprintf("%s(%d) started %s", xact.Kind(), xact.ShortID(), stimestr)
	}
	etime := xact.EndTime()
	d := etime.Sub(stime)
	return fmt.Sprintf("%s(%d) started %s ended %s (%v)", xact.Kind(), xact.ShortID(), stimestr, etime.Format(timeStampFormat), d)
}

func (xact *XactBase) StartTime(s ...time.Time) time.Time {
	if len(s) == 0 {
		u := xact.sutime.Load()
		if u == 0 {
			return time.Time{}
		}
		return time.Unix(0, u)
	}
	stime := s[0]
	xact.sutime.Store(stime.UnixNano())
	return stime
}

func (xact *XactBase) EndTime(e ...time.Time) time.Time {
	if len(e) == 0 {
		u := xact.eutime.Load()
		if u == 0 {
			return time.Time{}
		}
		return time.Unix(0, u)
	}
	etime := e[0]
	xact.eutime.Store(etime.UnixNano())
	if xact.Kind() != ActAsyncTask {
		glog.Infoln(xact.String())
	}
	return etime
}

func (xact *XactBase) Abort() {
	if !xact.aborted.CAS(false, true) {
		glog.Infof("already aborted: " + xact.String())
		return
	}
	xact.eutime.Store(time.Now().UnixNano())
	close(xact.abrt)
	glog.Infof("ABORT: " + xact.String())
}

//
// XactDemandBase - partially implements XactDemand interface
//

func NewXactDemandBase(id int64, kind string, bucket string, bckIsAIS bool, idleTime ...time.Duration) *XactDemandBase {
	tickTime := xactIdleTimeout
	if len(idleTime) != 0 {
		tickTime = idleTime[0]
	}
	ticker := time.NewTicker(tickTime)
	return &XactDemandBase{
		XactBase: *NewXactBaseWithBucket(id, kind, bucket, bckIsAIS),
		ticker:   ticker,
	}
}

func (r *XactDemandBase) ChanCheckTimeout() <-chan time.Time { return r.ticker.C }
func (r *XactDemandBase) Renew()                             { r.renew.Store(1) } // see Timeout()
func (r *XactDemandBase) IncPending()                        { r.pending.Inc() }
func (r *XactDemandBase) DecPending()                        { r.pending.Dec() }
func (r *XactDemandBase) SubPending(n int64)                 { r.pending.Sub(n) }
func (r *XactDemandBase) Pending() int64                     { return r.pending.Load() }

func (r *XactDemandBase) Timeout() bool {
	if r.pending.Load() > 0 {
		return false
	}
	return r.renew.Dec() < 0
}

func (r *XactDemandBase) Stop() { r.ticker.Stop() }

func ValidXact(xact string) (bool, bool) {
	meta, ok := XactKind[xact]
	return meta.IsGlobal, ok
}

func (*MountpathXact) IsMountpathXact() bool    { return true }
func (*NonmountpathXact) IsMountpathXact() bool { return false }

type (
	XactCountStats interface {
		ObjectsCnt() int64
		BytesCnt() int64
	}

	XactBaseCountStats struct {
		objects atomic.Int64
		bytes   atomic.Int64
	}
)

func (s *XactBaseCountStats) ObjectsCnt() int64          { return s.objects.Load() }
func (s *XactBaseCountStats) ObjectsInc() int64          { return s.objects.Inc() }
func (s *XactBaseCountStats) ObjectsAdd(cnt int64) int64 { return s.objects.Add(cnt) }

func (s *XactBaseCountStats) BytesCnt() int64           { return s.bytes.Load() }
func (s *XactBaseCountStats) BytesAdd(size int64) int64 { return s.bytes.Add(size) }
