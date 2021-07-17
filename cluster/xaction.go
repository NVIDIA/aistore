// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type QuiRes int

const (
	QuiInactive = QuiRes(iota) // e.g., no pending requests, no messages received, etc.
	QuiActive                  // active (e.g., receiving data)
	QuiDone                    // all done
	QuiAborted                 // aborted
	QuiTimeout                 // timeout
	Quiescent                  // idle => quiescent
)

type (
	QuiCB func(elapsed time.Duration) QuiRes // see enum below

	Xact interface {
		Run()
		ID() string
		Kind() string
		Bck() cmn.Bck
		StartTime() time.Time
		EndTime() time.Time
		ObjCount() int64
		BytesCount() int64
		String() string
		Finished() bool
		Aborted() bool
		AbortedAfter(time.Duration) bool
		Quiesce(time.Duration, QuiCB) QuiRes
		ChanAbort() <-chan struct{}
		Result() (interface{}, error)
		Stats() XactStats

		// modifiers
		Renew()
		Finish(err error)
		Abort()
		AddNotif(n Notif)

		BytesAdd(cnt int64) int64
		ObjectsInc() int64
		ObjectsAdd(cnt int64) int64
	}

	XactStats interface {
		ID() string
		Kind() string
		Bck() cmn.Bck
		StartTime() time.Time
		EndTime() time.Time
		ObjCount() int64
		BytesCount() int64
		Aborted() bool
		Running() bool
		Finished() bool
	}
)
