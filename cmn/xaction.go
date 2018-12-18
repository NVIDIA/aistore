// Package cmn provides common API constants and types, and low-level utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const timeStampFormat = "15:04:05.000000"

const xactIdleTimeout = time.Minute * 3

type (
	Xact interface {
		ID() int64
		Kind() string
		StartTime(s ...time.Time) time.Time
		EndTime(e ...time.Time) time.Time
		String() string
		Abort()
		ChanAbort() <-chan struct{}
		Finished() bool
	}
	XactBase struct {
		id     int64
		sutime int64
		eutime int64
		kind   string
		abrt   chan struct{}
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
		renew   int64
		pending int64
	}
	ErrXpired struct { // return it if called (right) after self-termination
		errstr string
	}
)

func (e *ErrXpired) Error() string     { return e.errstr }
func NewErrXpired(s string) *ErrXpired { return &ErrXpired{errstr: s} }

//
// XactBase - implements Xact interface
//

var _ Xact = &XactBase{}

func NewXactBase(id int64, kind string) *XactBase {
	stime := time.Now()
	xact := &XactBase{id: id, kind: kind, abrt: make(chan struct{}, 1)}
	atomic.StoreInt64(&xact.sutime, stime.UnixNano())
	return xact
}

func (xact *XactBase) ID() int64                  { return xact.id }
func (xact *XactBase) Kind() string               { return xact.kind }
func (xact *XactBase) Finished() bool             { return atomic.LoadInt64(&xact.eutime) != 0 }
func (xact *XactBase) ChanAbort() <-chan struct{} { return xact.abrt }

func (xact *XactBase) String() string {
	stime := xact.StartTime()
	stimestr := stime.Format(timeStampFormat)
	if !xact.Finished() {
		return fmt.Sprintf("xaction %s:%d started %s", xact.Kind(), xact.ID(), stimestr)
	}
	etime := xact.EndTime()
	d := etime.Sub(stime)
	return fmt.Sprintf("xaction %s:%d started %s ended %s (duration %v)", xact.Kind(), xact.ID(), stimestr, etime.Format(timeStampFormat), d)
}

func (xact *XactBase) StartTime(s ...time.Time) time.Time {
	if len(s) == 0 {
		u := atomic.LoadInt64(&xact.sutime)
		if u == 0 {
			return time.Time{}
		}
		return time.Unix(0, u)
	}
	stime := s[0]
	atomic.StoreInt64(&xact.sutime, stime.UnixNano())
	return stime
}

func (xact *XactBase) EndTime(e ...time.Time) time.Time {
	if len(e) == 0 {
		u := atomic.LoadInt64(&xact.eutime)
		if u == 0 {
			return time.Time{}
		}
		return time.Unix(0, u)
	}
	etime := e[0]
	atomic.StoreInt64(&xact.eutime, etime.UnixNano())
	glog.Infoln(xact.String())
	return etime
}

func (xact *XactBase) Abort() {
	atomic.StoreInt64(&xact.eutime, time.Now().UnixNano())
	xact.abrt <- struct{}{} // NOTE: to get this, xactions must be receiving on the ChanAbort() channel - examples in the code
	close(xact.abrt)
	glog.Infof("ABORT: " + xact.String())
}

//
// XactDemandBase - implements XactDemand interface
//

var _ XactDemand = &XactDemandBase{}

func NewXactDemandBase(id int64, kind string) *XactDemandBase {
	base := NewXactBase(id, kind)
	ticker := time.NewTicker(xactIdleTimeout)
	return &XactDemandBase{XactBase: *base, ticker: ticker}
}

func (r *XactDemandBase) ChanCheckTimeout() <-chan time.Time { return r.ticker.C }
func (r *XactDemandBase) Renew()                             { atomic.StoreInt64(&r.renew, 1) } // see Timeout()
func (r *XactDemandBase) IncPending()                        { atomic.AddInt64(&r.pending, 1) }
func (r *XactDemandBase) DecPending()                        { atomic.AddInt64(&r.pending, -1) }

func (r *XactDemandBase) Timeout() bool {
	if atomic.LoadInt64(&r.pending) > 0 {
		return false
	}
	return atomic.AddInt64(&r.renew, -1) < 0
}

func (r *XactDemandBase) Stop() { r.ticker.Stop() }
