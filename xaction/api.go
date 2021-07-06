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
	XactTypeGlobal = "global"
	XactTypeBck    = "bucket"
	XactTypeTask   = "task"
)

type (
	XactDescriptor struct {
		Type       string          // XactTypeGlobal, etc. - enum above
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
	// bucket-less (aka "global") xactions with scope = (target | cluster)
	cmn.ActLRU:       {Type: XactTypeGlobal, Startable: true, Mountpath: true},
	cmn.ActElection:  {Type: XactTypeGlobal, Startable: false},
	cmn.ActResilver:  {Type: XactTypeGlobal, Startable: true, Mountpath: true},
	cmn.ActRebalance: {Type: XactTypeGlobal, Startable: true, Metasync: true, Owned: false, Mountpath: true},
	cmn.ActDownload:  {Type: XactTypeGlobal, Startable: false, Mountpath: true},

	// xactions that run on a given bucket or buckets
	cmn.ActECGet:          {Type: XactTypeBck, Startable: false},
	cmn.ActECPut:          {Type: XactTypeBck, Startable: false},
	cmn.ActECRespond:      {Type: XactTypeBck, Startable: false},
	cmn.ActMakeNCopies:    {Type: XactTypeBck, Access: cmn.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActPutCopies:      {Type: XactTypeBck, Startable: false},
	cmn.ActArchive:        {Type: XactTypeBck, Startable: false},
	cmn.ActMoveBck:        {Type: XactTypeBck, Access: cmn.AccessMoveBucket, Startable: false, Metasync: true, Owned: false, Mountpath: true},
	cmn.ActCopyBck:        {Type: XactTypeBck, Access: cmn.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActETLBck:         {Type: XactTypeBck, Access: cmn.AccessRW, Startable: false, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActECEncode:       {Type: XactTypeBck, Access: cmn.AccessRW, Startable: true, Metasync: true, Owned: false, RefreshCap: true, Mountpath: true},
	cmn.ActEvictObjects:   {Type: XactTypeBck, Access: cmn.AccessObjDELETE, Startable: false, Mountpath: true},
	cmn.ActDelete:         {Type: XactTypeBck, Access: cmn.AccessObjDELETE, Startable: false, Mountpath: true},
	cmn.ActLoadLomCache:   {Type: XactTypeBck, Startable: true, Mountpath: true},
	cmn.ActPrefetch:       {Type: XactTypeBck, Access: cmn.AccessRW, Startable: true},
	cmn.ActPromote:        {Type: XactTypeBck, Access: cmn.AccessPROMOTE, Startable: false, RefreshCap: true},
	cmn.ActQueryObjects:   {Type: XactTypeBck, Access: cmn.AccessObjLIST, Startable: false, Metasync: false, Owned: true},
	cmn.ActList:           {Type: XactTypeBck, Access: cmn.AccessObjLIST, Startable: false, Metasync: false, Owned: true},
	cmn.ActSummary:        {Type: XactTypeTask, Access: cmn.AccessObjLIST | cmn.AccessBckHEAD, Startable: false, Metasync: false, Owned: true, Mountpath: true},
	cmn.ActInvalListCache: {Type: XactTypeBck, Access: cmn.AccessObjLIST, Startable: false},
}

func IsValidKind(kind string) bool { _, ok := XactsDtor[kind]; return ok }
func IsTypeBck(kind string) bool   { return XactsDtor[kind].Type == XactTypeBck }
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
