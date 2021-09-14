// Package notifications provides interfaces for AIStore notifications
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package nl

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	jsoniter "github.com/json-iterator/go"
)

type NotifListener interface {
	Callback(nl NotifListener, ts int64)
	UnmarshalStats(rawMsg []byte) (interface{}, bool, bool, error)
	Lock()
	Unlock()
	RLock()
	RUnlock()
	Notifiers() cluster.NodeMap
	Kind() string
	Bcks() []cmn.Bck
	SetErr(error)
	Err() error
	UUID() string
	SetAborted()
	Aborted() bool
	Status() *NotifStatus
	SetStats(daeID string, stats interface{})
	NodeStats() *NodeStats
	QueryArgs() cmn.ReqArgs
	AbortArgs() cmn.ReqArgs
	EndTime() int64
	SetAddedTime()
	AddedTime() int64
	Finished() bool
	String() string
	GetOwner() string
	SetOwner(string)
	SetHrwOwner(smap *cluster.Smap)
	LastUpdated(si *cluster.Snode) int64
	ProgressInterval() time.Duration

	// detailed ref-counting
	ActiveNotifiers() cluster.NodeMap
	FinCount() int
	ActiveCount() int
	HasFinished(node *cluster.Snode) bool
	MarkFinished(node *cluster.Snode)
	NodesTardy(durs ...time.Duration) (nodes cluster.NodeMap, tardy bool)
}

type (
	NotifCallback func(n NotifListener)

	NodeStats struct {
		sync.RWMutex
		stats map[string]interface{} // daeID => Stats (e.g. cmn.BaseStatsExt)
	}

	NotifListenerBase struct {
		mu     sync.RWMutex
		Common struct {
			UUID        string
			Action      string // async operation kind (see cmn/api_const.go)
			Owned       string // "": not owned | equalIC: IC | otherwise, pid + IC
			SmapVersion int64  // smap version in which NL is added
			Bck         []cmn.Bck
		}
		// construction
		Srcs        cluster.NodeMap  // all notifiers
		ActiveSrcs  cluster.NodeMap  // running notifiers
		F           NotifCallback    `json:"-"` // optional listening-side callback
		Stats       *NodeStats       // [daeID => Stats (e.g. cmn.BaseStatsExt)]
		lastUpdated map[string]int64 // [daeID => last update time(nanoseconds)]
		progress    time.Duration    // time interval to monitor the progress
		addedTime   atomic.Int64     // Time when `nl` is added

		// runtime
		FinTime  atomic.Int64 // timestamp when finished
		AbortedX atomic.Bool  // sets if the xaction is Aborted
		ErrValue cos.ErrValue // reported error and count
	}

	NotifStatus struct {
		UUID     string `json:"uuid"`     // UUID of the xaction
		ErrMsg   string `json:"err"`      // error
		FinTime  int64  `json:"end_time"` // time xaction ended
		AbortedX bool   `json:"aborted"`  // true if aborted
	}
)

///////////////////////
// notifListenerBase //
///////////////////////

func NewNLB(uuid, action string, smap *cluster.Smap, srcs cluster.NodeMap, progress time.Duration, bck ...cmn.Bck) *NotifListenerBase {
	nlb := &NotifListenerBase{
		Srcs:        srcs,
		Stats:       NewNodeStats(len(srcs)),
		progress:    progress,
		lastUpdated: make(map[string]int64, len(srcs)),
	}
	nlb.Common.UUID = uuid
	nlb.Common.Action = action
	nlb.Common.SmapVersion = smap.Version
	nlb.Common.Bck = bck
	nlb.ActiveSrcs = srcs.ActiveMap()
	return nlb
}

func (nlb *NotifListenerBase) Lock()    { nlb.mu.Lock() }
func (nlb *NotifListenerBase) Unlock()  { nlb.mu.Unlock() }
func (nlb *NotifListenerBase) RLock()   { nlb.mu.RLock() }
func (nlb *NotifListenerBase) RUnlock() { nlb.mu.RUnlock() }

func (nlb *NotifListenerBase) Notifiers() cluster.NodeMap      { return nlb.Srcs }
func (nlb *NotifListenerBase) UUID() string                    { return nlb.Common.UUID }
func (nlb *NotifListenerBase) Aborted() bool                   { return nlb.AbortedX.Load() }
func (nlb *NotifListenerBase) SetAborted()                     { nlb.AbortedX.CAS(false, true) }
func (nlb *NotifListenerBase) EndTime() int64                  { return nlb.FinTime.Load() }
func (nlb *NotifListenerBase) Finished() bool                  { return nlb.EndTime() > 0 }
func (nlb *NotifListenerBase) ProgressInterval() time.Duration { return nlb.progress }
func (nlb *NotifListenerBase) NodeStats() *NodeStats           { return nlb.Stats }
func (nlb *NotifListenerBase) GetOwner() string                { return nlb.Common.Owned }
func (nlb *NotifListenerBase) SetOwner(o string)               { nlb.Common.Owned = o }
func (nlb *NotifListenerBase) Kind() string                    { return nlb.Common.Action }
func (nlb *NotifListenerBase) Bcks() []cmn.Bck                 { return nlb.Common.Bck }
func (nlb *NotifListenerBase) AddedTime() int64                { return nlb.addedTime.Load() }
func (nlb *NotifListenerBase) SetAddedTime()                   { nlb.addedTime.Store(mono.NanoTime()) }

func (nlb *NotifListenerBase) ActiveNotifiers() cluster.NodeMap { return nlb.ActiveSrcs }
func (nlb *NotifListenerBase) ActiveCount() int                 { return len(nlb.ActiveSrcs) }
func (nlb *NotifListenerBase) FinCount() int                    { return len(nlb.Srcs) - nlb.ActiveCount() }

func (nlb *NotifListenerBase) MarkFinished(node *cluster.Snode) {
	delete(nlb.ActiveSrcs, node.ID())
}

func (nlb *NotifListenerBase) HasFinished(node *cluster.Snode) bool {
	return !nlb.ActiveSrcs.Contains(node.ID())
}

// is called after all Notifiers will have notified OR on failure (err != nil)
func (nlb *NotifListenerBase) Callback(nl NotifListener, ts int64) {
	if nlb.FinTime.CAS(0, 1) {
		nlb.FinTime.Store(ts)
		if nlb.F != nil {
			nlb.F(nl)
		}
	}
}

func (nlb *NotifListenerBase) SetErr(err error) { nlb.ErrValue.Store(err) }
func (nlb *NotifListenerBase) Err() error       { return nlb.ErrValue.Err() }

func (nlb *NotifListenerBase) SetStats(daeID string, stats interface{}) {
	debug.AssertRWMutexLocked(&nlb.mu)

	_, ok := nlb.Srcs[daeID]
	debug.Assert(ok)
	nlb.Stats.Store(daeID, stats)
	if nlb.lastUpdated == nil {
		nlb.lastUpdated = make(map[string]int64, len(nlb.Srcs))
	}
	nlb.lastUpdated[daeID] = mono.NanoTime()
}

func (nlb *NotifListenerBase) LastUpdated(si *cluster.Snode) int64 {
	if nlb.lastUpdated == nil {
		return 0
	}
	return nlb.lastUpdated[si.DaemonID]
}

func (nlb *NotifListenerBase) NodesTardy(durs ...time.Duration) (nodes cluster.NodeMap, tardy bool) {
	dur := cmn.GCO.Get().Periodic.NotifTime.D()
	if len(durs) > 0 {
		dur = durs[0]
	} else if nlb.ProgressInterval() != 0 {
		dur = nlb.ProgressInterval()
	}
	nodes = make(cluster.NodeMap, nlb.ActiveCount())
	now := mono.NanoTime()
	for _, si := range nlb.ActiveSrcs {
		ts := nlb.LastUpdated(si)
		diff := time.Duration(now - ts)
		if _, ok := nlb.Stats.Load(si.ID()); ok && diff < dur {
			continue
		}
		nodes.Add(si)
		tardy = true
	}
	return
}

func (nlb *NotifListenerBase) Status() *NotifStatus {
	return &NotifStatus{UUID: nlb.UUID(), FinTime: nlb.FinTime.Load(), AbortedX: nlb.Aborted()}
}

func (nlb *NotifListenerBase) String() string {
	var (
		tm, res  string
		hdr      = fmt.Sprintf("nl-%s[%s]", nlb.Kind(), nlb.UUID())
		finCount = nlb.FinCount()
	)
	if tfin := nlb.FinTime.Load(); tfin > 0 {
		if err := nlb.ErrValue.Err(); err != nil {
			res = "-" + err.Error()
		} else {
			res = "-done"
		}
		tm = cos.FormatTimestamp(time.Unix(0, tfin))
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
	if err != nil {
		debug.AssertNoErr(err)
		return
	}
	nlb.SetOwner(psiOwner.ID())
}

/////////////////
// NotifStatus //
/////////////////

func (xs *NotifStatus) Finished() bool { return xs.FinTime > 0 }
func (xs *NotifStatus) Aborted() bool  { return xs.AbortedX }

/////////////////
//  NodeStats  //
/////////////////

func NewNodeStats(sizes ...int) *NodeStats {
	size := 0
	if len(sizes) > 0 {
		size = sizes[0]
	}
	return &NodeStats{
		stats: make(map[string]interface{}, size),
	}
}

func (ns *NodeStats) Store(key string, stats interface{}) {
	ns.Lock()
	if ns.stats == nil {
		ns.stats = make(map[string]interface{})
	}
	ns.stats[key] = stats
	ns.Unlock()
}

func (ns *NodeStats) Range(f func(string, interface{}) bool) {
	ns.RLock()
	defer ns.RUnlock()

	for key, val := range ns.stats {
		if !f(key, val) {
			return
		}
	}
}

func (ns *NodeStats) Load(key string) (val interface{}, ok bool) {
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
