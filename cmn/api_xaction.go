// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "time"

const (
	XactTypeGlobal = "global"
	XactTypeBck    = "bucket"
	XactTypeTask   = "task"
)

type (
	XactMetadata struct {
		Type      string
		Startable bool // determines if can be started via API
	}
	XactReqMsg struct {
		Target   string `json:"target,omitempty"`
		ID       string `json:"id"`
		Kind     string `json:"kind"`
		Bck      Bck    `json:"bck"`
		All      bool   `json:"all,omitempty"`
		Finished bool   `json:"finished"`
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
)

var XactsMeta = map[string]XactMetadata{
	// NOTE -- TODO: extend to include: run-by-primary-only | progress-bar-supported | limited-coexistence #791
	// global kinds
	ActLRU:       {Type: XactTypeGlobal, Startable: true},
	ActElection:  {Type: XactTypeGlobal, Startable: false},
	ActResilver:  {Type: XactTypeGlobal, Startable: true},
	ActRebalance: {Type: XactTypeGlobal, Startable: true},
	ActDownload:  {Type: XactTypeGlobal, Startable: false},

	// bucket's kinds
	ActECGet:        {Type: XactTypeBck, Startable: false},
	ActECPut:        {Type: XactTypeBck, Startable: false},
	ActECRespond:    {Type: XactTypeBck, Startable: false},
	ActMakeNCopies:  {Type: XactTypeBck, Startable: false},
	ActPutCopies:    {Type: XactTypeBck, Startable: false},
	ActRenameLB:     {Type: XactTypeBck, Startable: false},
	ActCopyBucket:   {Type: XactTypeBck, Startable: false},
	ActECEncode:     {Type: XactTypeBck, Startable: false},
	ActEvictObjects: {Type: XactTypeBck, Startable: false},
	ActDelete:       {Type: XactTypeBck, Startable: false},
	ActLoadLomCache: {Type: XactTypeBck, Startable: false},
	ActPrefetch:     {Type: XactTypeBck, Startable: true},
	ActPromote:      {Type: XactTypeBck, Startable: false},

	ActListObjects:   {Type: XactTypeTask, Startable: false},
	ActSummaryBucket: {Type: XactTypeTask, Startable: false},
	ActTar2Tf:        {Type: XactTypeTask, Startable: false},
}
