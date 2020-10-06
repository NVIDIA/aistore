// Package notifications provides interfaces for AIStore notifications
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package nl

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
)

type NotifListener interface {
	Callback(n NotifListener, err error, timestamp int64)
	UnmarshalStats(rawMsg []byte) (interface{}, bool, bool, error)
	Lock()
	Unlock()
	RLock()
	RUnlock()
	Notifiers() cluster.NodeMap
	FinNotifiers() cmn.StringSet
	NotifTy() int
	Kind() string
	Bcks() []cmn.Bck
	SetErr(error)
	Err() error
	UUID() string
	SetAborted()
	Aborted() bool
	Status() *NotifStatus
	SetStats(daeID string, stats interface{})
	NodeStats() *sync.Map
	QueryArgs() cmn.ReqArgs
	AbortArgs() cmn.ReqArgs
	EndTime() int64
	HasFinished(node *cluster.Snode) bool
	MarkFinished(node *cluster.Snode)
	AllFinished() bool
	FinCount() int
	Finished() bool
	String() string
	GetOwner() string
	SetOwner(string)
	SetHrwOwner(smap *cluster.Smap)
	LastUpdated(si *cluster.Snode) int64
	ProgressInterval() time.Duration
	NodesUptoDate(dur ...time.Duration) (cmn.StringSet, bool)
}

type (
	NotifCallback func(n NotifListener, err error)

	NotifListenerBase struct {
		sync.RWMutex
		Common struct {
			UUID   string
			Action string // async operation kind (see cmn/api_const.go)

			// ownership
			Owned       string // "": not owned | equalIC: IC | otherwise, pid + IC
			SmapVersion int64  // smap version in which NL is added

			Bck []cmn.Bck
			Ty  int // notification category (above)
		}

		Srcs              cluster.NodeMap  // expected Notifiers
		FinSrcs           cmn.StringSet    // daeID of Notifiers finished
		F                 NotifCallback    `json:"-"` // optional listening-side callback
		Stats             *sync.Map        // [daeID => Stats (e.g. cmn.BaseXactStatsExt)]
		LastUpdatedX      map[string]int64 // [daeID => last update time(nanoseconds)]
		ProgressIntervalX time.Duration    // time interval to monitor the progress

		ErrMsg   string        // ErrMsg
		FinTime  atomic.Int64  // timestamp when finished
		AbortedX atomic.Bool   // sets if the xaction is Aborted
		ErrCount atomic.Uint32 // number of error encountered
	}

	NotifStatus struct {
		UUID     string `json:"uuid"` // UUID of the xaction
		ErrMsg   string `json:"err"`
		FinTime  int64  `json:"end_time"` // time when xaction ended
		AbortedX bool   `json:"aborted"`
	}
)

func NewNLB(uuid string, smap *cluster.Smap, srcs cluster.NodeMap, ty int, action string,
	progressInterval time.Duration, bck ...cmn.Bck) *NotifListenerBase {
	cmn.Assert(ty > cmn.NotifInvalid)
	cmn.Assert(len(srcs) != 0)
	nlb := &NotifListenerBase{
		Srcs:              srcs,
		Stats:             &sync.Map{},
		ProgressIntervalX: progressInterval,
		LastUpdatedX:      make(map[string]int64, len(srcs)),
	}
	nlb.Common.UUID = uuid
	nlb.Common.Action = action
	nlb.Common.SmapVersion = smap.Version
	nlb.Common.Bck = bck
	nlb.Common.Ty = ty
	nlb.FinSrcs = make(cmn.StringSet, len(srcs))
	return nlb
}

///////////////////////
// notifListenerBase //
///////////////////////

func (nlb *NotifListenerBase) Notifiers() cluster.NodeMap      { return nlb.Srcs }
func (nlb *NotifListenerBase) FinNotifiers() cmn.StringSet     { return nlb.FinSrcs }
func (nlb *NotifListenerBase) NotifTy() int                    { return nlb.Common.Ty }
func (nlb *NotifListenerBase) UUID() string                    { return nlb.Common.UUID }
func (nlb *NotifListenerBase) Aborted() bool                   { return nlb.AbortedX.Load() }
func (nlb *NotifListenerBase) SetAborted()                     { nlb.AbortedX.CAS(false, true) }
func (nlb *NotifListenerBase) EndTime() int64                  { return nlb.FinTime.Load() }
func (nlb *NotifListenerBase) Finished() bool                  { return nlb.EndTime() > 0 }
func (nlb *NotifListenerBase) ProgressInterval() time.Duration { return nlb.ProgressIntervalX }
func (nlb *NotifListenerBase) NodeStats() *sync.Map            { return nlb.Stats }
func (nlb *NotifListenerBase) GetOwner() string                { return nlb.Common.Owned }
func (nlb *NotifListenerBase) SetOwner(o string)               { nlb.Common.Owned = o }
func (nlb *NotifListenerBase) Kind() string                    { return nlb.Common.Action }
func (nlb *NotifListenerBase) Bcks() []cmn.Bck                 { return nlb.Common.Bck }
func (nlb *NotifListenerBase) FinCount() int                   { return len(nlb.FinSrcs) }

// is called after all Notifiers will have notified OR on failure (err != nil)
func (nlb *NotifListenerBase) Callback(nl NotifListener, err error, timestamp int64) {
	if nlb.FinTime.CAS(0, 1) {
		nlb.FinTime.Store(timestamp)
		// is optional
		if nlb.F != nil {
			nlb.F(nl, err) // invoke user-supplied callback and pass user-supplied NotifListener
		}
	}
}

func (nlb *NotifListenerBase) SetErr(err error) {
	if nlb.ErrCount.Inc() == 1 {
		nlb.ErrMsg = err.Error()
	}
}

func (nlb *NotifListenerBase) Err() error {
	if nlb.ErrMsg == "" {
		return nil
	}
	if l := nlb.ErrCount.Load(); l > 1 {
		return fmt.Errorf("%s... (error-count=%d)", nlb.ErrMsg, l)
	}
	return errors.New(nlb.ErrMsg)
}

func (nlb *NotifListenerBase) SetStats(daeID string, stats interface{}) {
	_, ok := nlb.Srcs[daeID]
	cmn.Assert(ok)
	nlb.Stats.Store(daeID, stats)
	nlb.LastUpdatedX[daeID] = mono.NanoTime()
}

func (nlb *NotifListenerBase) LastUpdated(si *cluster.Snode) int64 {
	return nlb.LastUpdatedX[si.DaemonID]
}

func (nlb *NotifListenerBase) NodesUptoDate(durs ...time.Duration) (uptoDate cmn.StringSet, tardy bool) {
	dur := cmn.GCO.Get().Periodic.NotifTime
	uptoDate = cmn.NewStringSet()

	if len(durs) > 0 {
		dur = durs[0]
	} else if nlb.ProgressInterval() != 0 {
		dur = nlb.ProgressInterval()
	}
	for _, si := range nlb.Notifiers() {
		ts := nlb.LastUpdated(si)
		diff := mono.Since(ts)
		if _, ok := nlb.Stats.Load(si.ID()); ok && (nlb.HasFinished(si) || diff < dur) {
			uptoDate.Add(si.DaemonID)
			continue
		}
		tardy = true
	}
	return
}

func (nlb *NotifListenerBase) Status() *NotifStatus {
	status := &NotifStatus{
		UUID:     nlb.UUID(),
		FinTime:  nlb.FinTime.Load(),
		AbortedX: nlb.Aborted(),
	}
	if err := nlb.Err(); err != nil {
		status.ErrMsg = err.Error()
	}
	return status
}

func (nlb *NotifListenerBase) String() string {
	var (
		tm, res  string
		hdr      = fmt.Sprintf("nl-%s[%s]", nlb.Kind(), nlb.UUID())
		finCount = nlb.FinCount()
	)
	if tfin := nlb.FinTime.Load(); tfin > 0 {
		if l := nlb.ErrCount.Load(); l > 0 {
			res = fmt.Sprintf("-fail(#errs=%d)", l)
		} else {
			res = "-done"
		}
		tm = cmn.FormatTimestamp(time.Unix(0, tfin))
		return fmt.Sprintf("%s-%s%s", hdr, tm, res)
	}
	if finCount > 0 {
		return fmt.Sprintf("%s(cnt=%d/%d)", hdr, finCount, len(nlb.Srcs))
	}
	return hdr
}

// effectively, cache owner
func (nlb *NotifListenerBase) SetHrwOwner(smap *cluster.Smap) {
	psiOwner, err := cluster.HrwIC(smap, nlb.UUID())
	cmn.AssertNoErr(err) // TODO -- FIXME: handle
	nlb.SetOwner(psiOwner.ID())
}

func (nlb *NotifListenerBase) HasFinished(node *cluster.Snode) bool {
	return nlb.FinSrcs.Contains(node.ID())
}

func (nlb *NotifListenerBase) MarkFinished(node *cluster.Snode) {
	if nlb.FinSrcs == nil {
		nlb.FinSrcs = make(cmn.StringSet)
	}
	nlb.FinSrcs.Add(node.ID())
}

func (nlb *NotifListenerBase) AllFinished() bool {
	for _, node := range nlb.Notifiers() {
		if !nlb.HasFinished(node) {
			return false
		}
	}
	return true
}

/////////////////
// NotifStatus //
/////////////////

func (xs *NotifStatus) Finished() bool { return xs.FinTime > 0 }
func (xs *NotifStatus) Aborted() bool  { return xs.AbortedX }
