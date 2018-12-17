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

type (
	XactInterface interface {
		ID() int64
		Kind() string
		StartTime(s ...time.Time) time.Time
		EndTime(e ...time.Time) time.Time
		String() string
		Abort()
		ChanAbort() chan struct{}
		Finished() bool
	}
	XactBase struct {
		id     int64
		sutime int64
		eutime int64
		kind   string
		abrt   chan struct{}
	}
)

var _ XactInterface = &XactBase{}

func NewXactBase(id int64, kind string) *XactBase {
	stime := time.Now()
	xact := &XactBase{id: id, kind: kind, abrt: make(chan struct{}, 1)}
	atomic.StoreInt64(&xact.sutime, stime.UnixNano())
	return xact
}

func (xact *XactBase) ID() int64                { return xact.id }
func (xact *XactBase) Kind() string             { return xact.kind }
func (xact *XactBase) Finished() bool           { return atomic.LoadInt64(&xact.eutime) != 0 }
func (xact *XactBase) ChanAbort() chan struct{} { return xact.abrt }

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
	xact.abrt <- struct{}{}
	close(xact.abrt)
	glog.Infof("ABORT: " + xact.String())
}
