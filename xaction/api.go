// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	ScopeG   = "global"
	ScopeT   = "target"
	ScopeBck = "bucket"
	ScopeO   = "other"
)

type (
	XactDescriptor struct {
		Scope      string          // ScopeG (global), etc. - the enum above
		Access     cmn.AccessAttrs // Access required by xact (see: cmn.Access*)
		Startable  bool            // determines if this xaction can be started via API
		Metasync   bool            // true: changes and metasyncs cluster-wide meta
		Owned      bool            // true: JTX-owned
		RefreshCap bool            // true: refresh capacity stats upon completion
		Mountpath  bool            // true: mountpath-traversing (jogger-based) xaction
	}

	XactReqMsg struct {
		ID          string    `json:"id"`
		Kind        string    `json:"kind"`
		Bck         cmn.Bck   `json:"bck"`
		OnlyRunning *bool     `json:"show_active"`
		Force       *bool     `json:"force"`             // true: force LRU
		Buckets     []cmn.Bck `json:"buckets,omitempty"` // list of buckets on which LRU should run
		Node        string    `json:"node,omitempty"`
	}

	BaseXactStats struct {
		IDX         string    `json:"id"`
		KindX       string    `json:"kind"`
		BckX        cmn.Bck   `json:"bck"`
		StartTimeX  time.Time `json:"start_time"`
		EndTimeX    time.Time `json:"end_time"`
		ObjCountX   int64     `json:"obj_count,string"`
		BytesCountX int64     `json:"bytes_count,string"`
		AbortedX    bool      `json:"aborted"`
	}

	BaseXactStatsExt struct {
		BaseXactStats
		Ext interface{} `json:"ext"`
	}

	BaseXactDemandStatsExt struct {
		IsIdle bool `json:"is_idle"`
	}
)

// interface guard
var _ cluster.XactStats = (*BaseXactStats)(nil)

// XactsDtor is a static Kind=>[Xaction Descriptor] map that contains
// static properties of a given xaction type (aka `kind`), such as:
// `Startable`, `Owned`, etc.
var XactsDtor = map[string]XactDescriptor{
	// bucket-less xactions that will typically have a 'cluster' scope (with resilver being a notable exception)
	cmn.ActLRU:       {Scope: ScopeG, Startable: true, Mountpath: true},
	cmn.ActElection:  {Scope: ScopeG, Startable: false},
	cmn.ActResilver:  {Scope: ScopeT, Startable: true, Mountpath: true},
	cmn.ActRebalance: {Scope: ScopeG, Startable: true, Metasync: true, Owned: false, Mountpath: true},
	cmn.ActDownload:  {Scope: ScopeG, Startable: false, Mountpath: true},

	// xactions that run on a given bucket or buckets
	cmn.ActECGet:          {Scope: ScopeBck, Startable: false},
	cmn.ActECPut:          {Scope: ScopeBck, Startable: false, Mountpath: true, RefreshCap: true},
	cmn.ActECRespond:      {Scope: ScopeBck, Startable: false},
	cmn.ActMakeNCopies:    {Scope: ScopeBck, Access: cmn.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActPutCopies:      {Scope: ScopeBck, Startable: false, Mountpath: true, RefreshCap: true},
	cmn.ActArchive:        {Scope: ScopeBck, Startable: false, RefreshCap: true},
	cmn.ActMoveBck:        {Scope: ScopeBck, Access: cmn.AccessMoveBucket, Startable: false, Metasync: true, Owned: false, Mountpath: true},
	cmn.ActCopyBck:        {Scope: ScopeBck, Access: cmn.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActETLBck:         {Scope: ScopeBck, Access: cmn.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActECEncode:       {Scope: ScopeBck, Access: cmn.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActEvictObjects:   {Scope: ScopeBck, Access: cmn.AccessObjDELETE, Startable: false, RefreshCap: true, Mountpath: true},
	cmn.ActDelete:         {Scope: ScopeBck, Access: cmn.AccessObjDELETE, Startable: false, RefreshCap: true, Mountpath: true},
	cmn.ActLoadLomCache:   {Scope: ScopeBck, Startable: true, Mountpath: true},
	cmn.ActPrefetch:       {Scope: ScopeBck, Access: cmn.AccessRW, RefreshCap: true, Startable: true},
	cmn.ActPromote:        {Scope: ScopeBck, Access: cmn.AccessPROMOTE, Startable: false, RefreshCap: true},
	cmn.ActQueryObjects:   {Scope: ScopeBck, Access: cmn.AccessObjLIST, Startable: false, Metasync: false, Owned: true},
	cmn.ActList:           {Scope: ScopeBck, Access: cmn.AccessObjLIST, Startable: false, Metasync: false, Owned: true},
	cmn.ActInvalListCache: {Scope: ScopeBck, Access: cmn.AccessObjLIST, Startable: false},

	// other
	cmn.ActSummary: {Scope: ScopeO, Access: cmn.AccessObjLIST | cmn.AccessBckHEAD, Startable: false, Metasync: false, Owned: true, Mountpath: true},
}

func IsValidKind(kind string) bool { _, ok := XactsDtor[kind]; return ok }
func IsBckScope(kind string) bool  { return XactsDtor[kind].Scope == ScopeBck }
func IsMountpath(kind string) bool { return XactsDtor[kind].Mountpath }

///////////////////
// BaseXactStats //
///////////////////

func (b *BaseXactStats) ID() string           { return b.IDX }
func (b *BaseXactStats) Kind() string         { return b.KindX }
func (b *BaseXactStats) Bck() cmn.Bck         { return b.BckX }
func (b *BaseXactStats) StartTime() time.Time { return b.StartTimeX }
func (b *BaseXactStats) EndTime() time.Time   { return b.EndTimeX }
func (b *BaseXactStats) ObjCount() int64      { return b.ObjCountX }
func (b *BaseXactStats) BytesCount() int64    { return b.BytesCountX }
func (b *BaseXactStats) Aborted() bool        { return b.AbortedX }
func (b *BaseXactStats) Running() bool        { return b.EndTimeX.IsZero() }
func (b *BaseXactStats) Finished() bool       { return !b.EndTimeX.IsZero() }

////////////////
// XactReqMsg //
////////////////
func (msg *XactReqMsg) String() (s string) {
	if msg.ID == "" {
		s = fmt.Sprintf("xmsg-%s", msg.Kind)
	} else {
		s = fmt.Sprintf("xmsg-%s[%s]", msg.Kind, msg.ID)
	}
	if msg.Bck.IsEmpty() {
		return
	}
	return fmt.Sprintf("%s, bucket %s", s, msg.Bck)
}
