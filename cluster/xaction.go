// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
		Finished() bool
		Aborted() bool
		AbortedAfter(time.Duration) bool
		Quiesce(time.Duration, QuiCB) QuiRes
		ChanAbort() <-chan error
		Result() (interface{}, error)
		Snap() XactionSnap

		// reporting: log, err
		String() string
		Name() string

		// modifiers
		Finish(error)
		Abort(error) bool
		AddNotif(n Notif)

		// common stats
		Objs() int64
		ObjsAdd(int, int64)    // locally processed
		OutObjsAdd(int, int64) // transmit
		InObjsAdd(int, int64)  // receive
		InBytes() int64
		OutBytes() int64
	}

	XactionSnap interface {
		Aborted() bool
		Running() bool
		Finished() bool
	}
)
