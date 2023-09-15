// Package notifications provides interfaces for AIStore notifications
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package nl

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	jsoniter "github.com/json-iterator/go"
)

type Listener interface {
	Callback(nl Listener, ts int64)
	UnmarshalStats(rawMsg []byte) (any, bool, bool, error)
	Lock()
	Unlock()
	RLock()
	RUnlock()
	Notifiers() meta.NodeMap
	Kind() string
	Cause() string
	Bcks() []*cmn.Bck
	AddErr(error)
	Err() error
	ErrCnt() int
	UUID() string
	SetAborted()
	Aborted() bool
	Status() *Status
	SetStats(daeID string, stats any)
	NodeStats() *NodeStats
	QueryArgs() cmn.HreqArgs
	EndTime() int64
	SetAddedTime()
	AddedTime() int64
	Finished() bool
	String() string
	GetOwner() string
	SetOwner(string)
	LastUpdated(si *meta.Snode) int64
	ProgressInterval() time.Duration

	// detailed ref-counting
	ActiveNotifiers() meta.NodeMap
	FinCount() int
	ActiveCount() int
	HasFinished(node *meta.Snode) bool
	MarkFinished(node *meta.Snode)
	NodesTardy(periodicNotifTime time.Duration) (nodes meta.NodeMap, tardy bool)
}

type (
	Callback func(n Listener)

	NodeStats struct {
		sync.RWMutex
		stats map[string]any // daeID => Stats (e.g. cmn.SnapExt)
	}

	ListenerBase struct {
		mu     sync.RWMutex
		Common struct {
			UUID  string
			Kind  string // async operation kind (see api/apc/actmsg.go)
			Cause string // causal action (e.g. decommission => rebalance)
			Owned string // "": not owned | equalIC: IC | otherwise, pid + IC
			Bck   []*cmn.Bck
		}
		// construction
		Srcs        meta.NodeMap     // all notifiers
		ActiveSrcs  meta.NodeMap     // running notifiers
		F           Callback         `json:"-"` // optional listening-side callback
		Stats       *NodeStats       // [daeID => Stats (e.g. cmn.SnapExt)]
		lastUpdated map[string]int64 // [daeID => last update time(nanoseconds)]
		progress    time.Duration    // time interval to monitor the progress
		addedTime   atomic.Int64     // Time when `nl` is added

		// runtime
		EndTimeX atomic.Int64 // timestamp when finished
		AbortedX atomic.Bool  // sets if the xaction is Aborted
		Errs     cos.Errs     // reported error and count
	}

	Status struct {
		Kind     string `json:"kind"`     // xaction kind
		UUID     string `json:"uuid"`     // xaction UUID
		ErrMsg   string `json:"err"`      // error
		EndTimeX int64  `json:"end_time"` // time xaction ended
		AbortedX bool   `json:"aborted"`  // true if aborted
	}
	StatusVec []Status
)

//////////////////
// ListenerBase //
//////////////////

func NewNLB(uuid, action, cause string, srcs meta.NodeMap, progress time.Duration, bck ...*cmn.Bck) *ListenerBase {
	nlb := &ListenerBase{
		Srcs:        srcs,
		Stats:       NewNodeStats(len(srcs)),
		progress:    progress,
		lastUpdated: make(map[string]int64, len(srcs)),
	}
	nlb.Common.UUID = uuid
	nlb.Common.Kind = action
	nlb.Common.Cause = cause
	nlb.Common.Bck = bck
	nlb.ActiveSrcs = srcs.ActiveMap()
	return nlb
}

func (nlb *ListenerBase) Lock()    { nlb.mu.Lock() }
func (nlb *ListenerBase) Unlock()  { nlb.mu.Unlock() }
func (nlb *ListenerBase) RLock()   { nlb.mu.RLock() }
func (nlb *ListenerBase) RUnlock() { nlb.mu.RUnlock() }

func (nlb *ListenerBase) Notifiers() meta.NodeMap         { return nlb.Srcs }
func (nlb *ListenerBase) UUID() string                    { return nlb.Common.UUID }
func (nlb *ListenerBase) Aborted() bool                   { return nlb.AbortedX.Load() }
func (nlb *ListenerBase) SetAborted()                     { nlb.AbortedX.CAS(false, true) }
func (nlb *ListenerBase) EndTime() int64                  { return nlb.EndTimeX.Load() }
func (nlb *ListenerBase) Finished() bool                  { return nlb.EndTime() > 0 }
func (nlb *ListenerBase) ProgressInterval() time.Duration { return nlb.progress }
func (nlb *ListenerBase) NodeStats() *NodeStats           { return nlb.Stats }
func (nlb *ListenerBase) GetOwner() string                { return nlb.Common.Owned }
func (nlb *ListenerBase) SetOwner(o string)               { nlb.Common.Owned = o }
func (nlb *ListenerBase) Kind() string                    { return nlb.Common.Kind }
func (nlb *ListenerBase) Cause() string                   { return nlb.Common.Cause }
func (nlb *ListenerBase) Bcks() []*cmn.Bck                { return nlb.Common.Bck }
func (nlb *ListenerBase) AddedTime() int64                { return nlb.addedTime.Load() }
func (nlb *ListenerBase) SetAddedTime()                   { nlb.addedTime.Store(mono.NanoTime()) }

func (nlb *ListenerBase) ActiveNotifiers() meta.NodeMap { return nlb.ActiveSrcs }
func (nlb *ListenerBase) ActiveCount() int              { return len(nlb.ActiveSrcs) }
func (nlb *ListenerBase) FinCount() int                 { return len(nlb.Srcs) - nlb.ActiveCount() }

func (nlb *ListenerBase) MarkFinished(node *meta.Snode) {
	delete(nlb.ActiveSrcs, node.ID())
}

func (nlb *ListenerBase) HasFinished(node *meta.Snode) bool {
	return !nlb.ActiveSrcs.Contains(node.ID())
}

// is called after all Notifiers will have notified OR on failure (err != nil)
func (nlb *ListenerBase) Callback(nl Listener, ts int64) {
	if nlb.EndTimeX.CAS(0, 1) {
		nlb.EndTimeX.Store(ts)
		if nlb.F != nil {
			nlb.F(nl)
		}
	}
}

func (nlb *ListenerBase) AddErr(err error) { nlb.Errs.Add(err) }
func (nlb *ListenerBase) ErrCnt() int      { return nlb.Errs.Cnt() }

func (nlb *ListenerBase) Err() error {
	if nlb.ErrCnt() == 0 {
		return nil
	}
	return &nlb.Errs
}

func (nlb *ListenerBase) SetStats(daeID string, stats any) {
	debug.AssertRWMutexLocked(&nlb.mu)

	_, ok := nlb.Srcs[daeID]
	debug.Assert(ok)
	nlb.Stats.Store(daeID, stats)
	if nlb.lastUpdated == nil {
		nlb.lastUpdated = make(map[string]int64, len(nlb.Srcs))
	}
	nlb.lastUpdated[daeID] = mono.NanoTime()
}

func (nlb *ListenerBase) LastUpdated(si *meta.Snode) int64 {
	if nlb.lastUpdated == nil {
		return 0
	}
	return nlb.lastUpdated[si.ID()]
}

// under rlock
func (nlb *ListenerBase) NodesTardy(periodicNotifTime time.Duration) (nodes meta.NodeMap, tardy bool) {
	if nlb.ProgressInterval() != 0 {
		periodicNotifTime = nlb.ProgressInterval()
	}
	nodes = make(meta.NodeMap, nlb.ActiveCount())
	now := mono.NanoTime()
	for _, si := range nlb.ActiveSrcs {
		ts := nlb.LastUpdated(si)
		diff := time.Duration(now - ts)
		if _, ok := nlb.Stats.Load(si.ID()); ok && diff < periodicNotifTime {
			continue
		}
		nodes.Add(si)
		tardy = true
	}
	return
}

func (nlb *ListenerBase) Status() *Status {
	return &Status{Kind: nlb.Kind(), UUID: nlb.UUID(), EndTimeX: nlb.EndTimeX.Load(), AbortedX: nlb.Aborted()}
}

func (nlb *ListenerBase) String() string {
	var (
		tm, res  string
		hdr      = fmt.Sprintf("nl-%s[%s]", nlb.Kind(), nlb.UUID())
		finCount = nlb.FinCount()
	)
	if nlb.Cause() != "" {
		hdr += "-caused-by-" + nlb.Cause()
	}
	if bcks := nlb.Bcks(); len(bcks) > 0 {
		if len(bcks) == 1 {
			hdr += "-" + bcks[0].String()
		} else {
			hdr += "-" + bcks[0].String() + "-" + bcks[1].String()
		}
	}
	if tfin := nlb.EndTimeX.Load(); tfin > 0 {
		if cnt := nlb.ErrCnt(); cnt > 0 {
			res = "-" + nlb.Err().Error()
		} else {
			res = "-done"
		}
		tm = cos.FormatNanoTime(tfin, cos.StampMicro)
		return fmt.Sprintf("%s-%s%s", hdr, tm, res)
	}
	if finCount > 0 {
		return fmt.Sprintf("%s(cnt=%d/%d)", hdr, finCount, len(nlb.Srcs))
	}
	return hdr
}

////////////
// Status //
////////////

func (ns *Status) Finished() bool { return ns.EndTimeX > 0 }
func (ns *Status) Aborted() bool  { return ns.AbortedX }

func (ns *Status) String() (s string) {
	s = ns.Kind + "[" + ns.UUID + "]"
	switch {
	case ns.Aborted():
		s += "-abrt"
	case ns.Finished():
		if ns.ErrMsg != "" {
			s += "-" + ns.ErrMsg
		} else {
			s += "-done"
		}
	}
	return
}

func (nsv StatusVec) String() (s string) {
	for _, ns := range nsv {
		s += ns.String() + ", "
	}
	return s[:cos.Max(0, len(s)-2)]
}

///////////////
// NodeStats //
///////////////

func NewNodeStats(sizes ...int) *NodeStats {
	size := 0
	if len(sizes) > 0 {
		size = sizes[0]
	}
	return &NodeStats{
		stats: make(map[string]any, size),
	}
}

func (ns *NodeStats) Store(key string, stats any) {
	ns.Lock()
	if ns.stats == nil {
		ns.stats = make(map[string]any)
	}
	ns.stats[key] = stats
	ns.Unlock()
}

func (ns *NodeStats) Range(f func(string, any) bool) {
	ns.RLock()
	defer ns.RUnlock()

	for key, val := range ns.stats {
		if !f(key, val) {
			return
		}
	}
}

func (ns *NodeStats) Load(key string) (val any, ok bool) {
	ns.RLock()
	val, ok = ns.stats[key]
	ns.RUnlock()
	return
}

func (ns *NodeStats) Len() (l int) {
	ns.RLock()
	l = len(ns.stats)
	ns.RUnlock()
	return
}

func (ns *NodeStats) MarshalJSON() (data []byte, err error) {
	ns.RLock()
	data, err = jsoniter.Marshal(ns.stats)
	ns.RUnlock()
	return
}

func (ns *NodeStats) UnmarshalJSON(data []byte) (err error) {
	if len(data) == 0 {
		return nil
	}
	return jsoniter.Unmarshal(data, &ns.stats)
}
