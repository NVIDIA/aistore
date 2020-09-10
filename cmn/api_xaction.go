// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "time"

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
		Target      string `json:"target,omitempty"`
		ID          string `json:"id"`
		Kind        string `json:"kind"`
		Bck         Bck    `json:"bck"`
		OnlyRunning *bool  `json:"show_active"`
		Force       *bool  `json:"force"`             // true: force LRU
		Buckets     []Bck  `json:"buckets,omitempty"` // list of buckets on which LRU should run
	}
	BaseXactStats struct {
		IDX         string    `json:"id"`
		KindX       string    `json:"kind"`
		BckX        Bck       `json:"bck"`
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

	XactStatus struct {
		UUID     string `json:"uuid"` // UUID of the xaction
		ErrMsg   string `json:"err"`
		FinTime  int64  `json:"end_time"` // time when xaction ended
		AbortedX bool   `json:"aborted"`
	}
)

// XactsDtor is a statically declared table of the form: [xaction-kind => xaction-descriptor]
// TODO add progress-bar-supported and  limited-coexistence(#791)
//      consider adding on-demand column as well
var XactsDtor = map[string]XactDescriptor{
	// bucket-less (aka "global") xactions with scope = (target | cluster)
	ActLRU:       {Type: XactTypeGlobal, Startable: true},
	ActElection:  {Type: XactTypeGlobal, Startable: false},
	ActResilver:  {Type: XactTypeGlobal, Startable: true},
	ActRebalance: {Type: XactTypeGlobal, Startable: true, Metasync: true, Owned: false},
	ActDownload:  {Type: XactTypeGlobal, Startable: false},

	// xactions that run on a given bucket or buckets
	ActECGet:         {Type: XactTypeBck, Startable: false},
	ActECPut:         {Type: XactTypeBck, Startable: false},
	ActECRespond:     {Type: XactTypeBck, Startable: false},
	ActMakeNCopies:   {Type: XactTypeBck, Startable: true, Metasync: true, Owned: false},
	ActPutCopies:     {Type: XactTypeBck, Startable: false},
	ActRenameLB:      {Type: XactTypeBck, Startable: false, Metasync: true, Owned: false},
	ActCopyBucket:    {Type: XactTypeBck, Startable: false, Metasync: true, Owned: false},
	ActETLBucket:     {Type: XactTypeBck, Startable: false, Metasync: true, Owned: false},
	ActECEncode:      {Type: XactTypeBck, Startable: true, Metasync: true, Owned: false},
	ActEvictObjects:  {Type: XactTypeBck, Startable: false},
	ActDelete:        {Type: XactTypeBck, Startable: false},
	ActLoadLomCache:  {Type: XactTypeBck, Startable: false},
	ActPrefetch:      {Type: XactTypeBck, Startable: true},
	ActPromote:       {Type: XactTypeBck, Startable: false},
	ActQueryObjects:  {Type: XactTypeBck, Startable: false, Metasync: false, Owned: true},
	ActListObjects:   {Type: XactTypeTask, Startable: false, Metasync: false, Owned: true},
	ActSummaryBucket: {Type: XactTypeTask, Startable: false, Metasync: false, Owned: true},
}

func IsValidXaction(kind string) bool { _, ok := XactsDtor[kind]; return ok }
func IsXactTypeBck(kind string) bool  { return XactsDtor[kind].Type == XactTypeBck }
