/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package cmn provides common API constants and types, and low-level utilities for all dfcpub projects
package cmn

import "time"

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
		id    int64
		stime time.Time
		etime time.Time
		kind  string
		abrt  chan struct{}
	}
)

var _ XactInterface = &XactBase{}

func NewXactBase(id int64, kind string) *XactBase {
	return &XactBase{id: id, stime: time.Now(), kind: kind, abrt: make(chan struct{}, 1)}
}

func (xact *XactBase) ID() int64                { return xact.id }
func (xact *XactBase) Kind() string             { return xact.kind }
func (xact *XactBase) String() string           { Assert(false, "must be implemented"); return "" }
func (xact *XactBase) Finished() bool           { return !xact.etime.IsZero() }
func (xact *XactBase) ChanAbort() chan struct{} { return xact.abrt }

func (xact *XactBase) StartTime(s ...time.Time) time.Time {
	if len(s) == 0 {
		return xact.stime
	}
	xact.stime = s[0]
	return xact.stime
}

func (xact *XactBase) EndTime(e ...time.Time) time.Time {
	if len(e) == 0 {
		return xact.etime
	}
	xact.etime = e[0]
	return xact.etime
}

func (xact *XactBase) Abort() {
	xact.etime = time.Now()
	xact.abrt <- struct{}{}
	close(xact.abrt)
}
