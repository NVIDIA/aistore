// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
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
		FromTo() (*Bck, *Bck)
		StartTime() time.Time
		EndTime() time.Time
		Finished() bool
		Running() bool
		IsAborted() bool
		AbortErr() error
		AbortedAfter(time.Duration) error
		ChanAbort() <-chan error
		Quiesce(time.Duration, QuiCB) QuiRes
		Result() (any, error)

		Snap() *Snap // (struct below)

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

		// rebalance-only
		RebID int64 `json:"glob.id,string"`

		// common runtime: stats counters (above) and state
		Stats    Stats `json:"stats"`
		AbortedX bool  `json:"aborted"`
		IdleX    bool  `json:"is_idle"`
	}
)

//////////
// Snap //
//////////

func (snp *Snap) IsAborted() bool { return snp.AbortedX }
func (snp *Snap) IsIdle() bool    { return snp.IdleX }
func (snp *Snap) Started() bool   { return !snp.StartTime.IsZero() }
func (snp *Snap) Running() bool   { return snp.Started() && !snp.IsAborted() && snp.EndTime.IsZero() }
func (snp *Snap) Finished() bool  { return snp.Started() && !snp.EndTime.IsZero() }
