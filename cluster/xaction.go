// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"
)

type QuiRes int

const (
	QuiInactiveCB = QuiRes(iota) // e.g., no pending requests (NOTE: used exclusively by `quicb` callbacks)
	QuiActive                    // active (e.g., receiving data)
	QuiActiveRet                 // active that immediately breaks waiting for quiecscence
	QuiDone                      // all done
	QuiAborted                   // aborted
	QuiTimeout                   // timeout
	Quiescent                    // idle => quiescent
)

type (
	QuiCB func(elapsed time.Duration) QuiRes // see enum below

	Xact interface {
		Run(*sync.WaitGroup)
		ID() string
		Kind() string
		Bck() *Bck
		StartTime() time.Time
		EndTime() time.Time
		Objs() int64     // locally processed
		Bytes() int64    //
		OutObjs() int64  // transmit
		OutBytes() int64 //
		InObjs() int64   // receive
		InBytes() int64
		Finished() bool
		Aborted() bool
		AbortedAfter(time.Duration) bool
		Quiesce(time.Duration, QuiCB) QuiRes
		ChanAbort() <-chan struct{}
		Result() (interface{}, error)
		Snap() XactionSnap

		// reporting: log, err
		String() string
		Name() string

		// modifiers
		Finish(error)
		Abort(error) bool
		AddNotif(n Notif)

		// stats
		BytesAdd(int64) int64    // locally processed
		ObjsInc() int64          //
		ObjsAdd(int64) int64     //
		OutBytesAdd(int64) int64 // transmit
		OutObjsInc() int64       //
		OutObjsAdd(int64) int64  //
		InBytesAdd(int64) int64  // receive
		InObjsInc() int64
		InObjsAdd(int64) int64
	}

	XactionSnap interface {
		Aborted() bool
		Running() bool
		Finished() bool
	}
)
