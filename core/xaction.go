// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"math"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
)

type QuiRes int

const (
	QuiInactiveCB     = QuiRes(iota) // e.g., no pending requests (NOTE: used exclusively by `quicb` callbacks)
	QuiActive                        // active (e.g., receiving data)
	QuiActiveRet                     // active that immediately breaks waiting for quiecscence
	QuiActiveDontBump                // active that does not increase inactivity duration (ie., keeps initial setting)
	QuiDone                          // all done
	QuiAborted                       // aborted
	QuiTimeout                       // timeout
	Quiescent                        // idle => quiescent
)

type (
	QuiCB func(elapsed time.Duration) QuiRes // see enum below

	GetStats interface {
		Objs() int64
		ObjsAdd(int, int64)    // locally processed
		OutObjsAdd(int, int64) // transmit
		InObjsAdd(int, int64)  // receive
		InBytes() int64
		OutBytes() int64
	}

	Xact interface {
		Run(*sync.WaitGroup)
		ID() string
		Kind() string
		Bck() *meta.Bck
		FromTo() (*meta.Bck, *meta.Bck)
		StartTime() time.Time
		EndTime() time.Time
		IsDone() bool
		IsRunning() bool
		IsIdle() bool
		Quiesce(time.Duration, QuiCB) QuiRes

		// abrt
		IsAborted() bool
		AbortErr() error
		AbortedAfter(time.Duration) error
		ChanAbort() <-chan error
		// err (info)
		AddErr(error, ...int)

		// to support api.QueryXactionSnaps
		CtlMsg() string
		Snap() *Snap // (struct below)

		// reporting: log, err
		String() string
		Name() string
		Cname() string

		// modifiers
		Finish()
		Abort(error) bool
		AddNotif(n Notif)

		// common stats
		GetStats
	}
)

type (
	Stats struct {
		Objs     int64 `json:"loc-objs,string"`  // locally processed
		Bytes    int64 `json:"loc-bytes,string"` //
		OutObjs  int64 `json:"out-objs,string"`  // transmit
		OutBytes int64 `json:"out-bytes,string"` //
		InObjs   int64 `json:"in-objs,string"`   // receive
		InBytes  int64 `json:"in-bytes,string"`
	}
	Snap struct {
		// xaction-specific stats counters
		Ext any `json:"ext"`

		// common static props
		StartTime time.Time `json:"start-time"`
		EndTime   time.Time `json:"end-time"`
		Bck       cmn.Bck   `json:"bck"`
		SrcBck    cmn.Bck   `json:"src-bck"`
		DstBck    cmn.Bck   `json:"dst-bck"`
		ID        string    `json:"id"`
		Kind      string    `json:"kind"`
		CtlMsg    string    `json:"ctlmsg,omitempty"` // initiating control msg (a.k.a. "run options"; added v3.26)

		// extended error info
		AbortErr string `json:"abort-err"`
		Err      string `json:"err"`

		// packed field: number of workers, et al.
		Packed int64 `json:"glob.id,string"`

		// common runtime: stats counters (above) and state
		Stats    Stats `json:"stats"`
		AbortedX bool  `json:"aborted"`
		IdleX    bool  `json:"is_idle"`
	}
	AllRunningInOut struct {
		Kind    string
		Running []string
		Idle    []string // NOTE: returning only when not nil
	}
)

//////////
// Snap (see related MultiSnap in xact/api)
//////////

func (xsnap *Snap) IsAborted() bool { return xsnap.AbortedX }
func (xsnap *Snap) IsIdle() bool    { return xsnap.IdleX }
func (xsnap *Snap) Started() bool   { return !xsnap.StartTime.IsZero() }

func (xsnap *Snap) IsRunning() bool {
	return xsnap.Started() && !xsnap.IsAborted() && xsnap.EndTime.IsZero()
}

func (xsnap *Snap) IsFinished() bool { return xsnap.Started() && !xsnap.EndTime.IsZero() }

// snap.Packed layout:
//
// [[ --------- bits 20 through 63 ----------] [--- bits 10 through 19 ---] [------ bits 0 through 9 ------]]
//             channel-full counter                 number of workers          number of mountpath joggers

const (
	gorBits = 10
	chShift = 2 * gorBits
	gorMask = (1 << gorBits) - 1
)

func (xsnap *Snap) Pack(njoggers, nworkers int, chanFull int64) {
	chfull := int(min(chanFull, math.MaxInt))
	xsnap.Packed = int64((njoggers & gorMask) | (nworkers&gorMask)<<gorBits | chfull<<chShift)
}

func (xsnap *Snap) Unpack() (njoggers, nworkers, chanFull int) {
	njoggers = int(xsnap.Packed & gorMask)
	nworkers = int(xsnap.Packed>>gorBits) & gorMask
	chanFull = int(xsnap.Packed >> chShift)
	return njoggers, nworkers, chanFull
}
