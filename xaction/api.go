// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
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
		Type      string // XactTypeGlobal, etc. - enum above
		Startable bool   // determines if this xaction can be started via API
		Metasync  bool   // true: changes and metasyncs cluster-wide meta
		Owned     bool   // true: JTX-owned
	}

	XactReqMsg struct {
		Target      string    `json:"target,omitempty"`
		ID          string    `json:"id"`
		Kind        string    `json:"kind"`
		Bck         cmn.Bck   `json:"bck"`
		OnlyRunning *bool     `json:"show_active"`
		Force       *bool     `json:"force"`             // true: force LRU
		Buckets     []cmn.Bck `json:"buckets,omitempty"` // list of buckets on which LRU should run
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
)

var (
	// interface guards
	_ cluster.XactStats = &BaseXactStats{}
)

//
// BaseXactStats
//

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

// XactsDtor is a statically declared table of the form: [xaction-kind => xaction-descriptor]
// TODO add progress-bar-supported and  limited-coexistence(#791)
//      consider adding on-demand column as well
var XactsDtor = map[string]XactDescriptor{
	// bucket-less (aka "global") xactions with scope = (target | cluster)
	cmn.ActLRU:       {Type: XactTypeGlobal, Startable: true},
	cmn.ActElection:  {Type: XactTypeGlobal, Startable: false},
	cmn.ActResilver:  {Type: XactTypeGlobal, Startable: true},
	cmn.ActRebalance: {Type: XactTypeGlobal, Startable: true, Metasync: true, Owned: false},
	cmn.ActDownload:  {Type: XactTypeGlobal, Startable: false},

	// xactions that run on a given bucket or buckets
	cmn.ActECGet:         {Type: XactTypeBck, Startable: false},
	cmn.ActECPut:         {Type: XactTypeBck, Startable: false},
	cmn.ActECRespond:     {Type: XactTypeBck, Startable: false},
	cmn.ActMakeNCopies:   {Type: XactTypeBck, Startable: true, Metasync: true, Owned: false},
	cmn.ActPutCopies:     {Type: XactTypeBck, Startable: false},
	cmn.ActRenameLB:      {Type: XactTypeBck, Startable: false, Metasync: true, Owned: false},
	cmn.ActCopyBucket:    {Type: XactTypeBck, Startable: false, Metasync: true, Owned: false},
	cmn.ActETLBucket:     {Type: XactTypeBck, Startable: false, Metasync: true, Owned: false},
	cmn.ActECEncode:      {Type: XactTypeBck, Startable: true, Metasync: true, Owned: false},
	cmn.ActEvictObjects:  {Type: XactTypeBck, Startable: false},
	cmn.ActDelete:        {Type: XactTypeBck, Startable: false},
	cmn.ActLoadLomCache:  {Type: XactTypeBck, Startable: false},
	cmn.ActPrefetch:      {Type: XactTypeBck, Startable: true},
	cmn.ActPromote:       {Type: XactTypeBck, Startable: false},
	cmn.ActQueryObjects:  {Type: XactTypeBck, Startable: false, Metasync: false, Owned: true},
	cmn.ActListObjects:   {Type: XactTypeTask, Startable: false, Metasync: false, Owned: true},
	cmn.ActSummaryBucket: {Type: XactTypeTask, Startable: false, Metasync: false, Owned: true},
}

func IsValidXaction(kind string) bool { _, ok := XactsDtor[kind]; return ok }
func IsXactTypeBck(kind string) bool  { return XactsDtor[kind].Type == XactTypeBck }
