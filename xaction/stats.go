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

type (
	BaseStats struct {
		IDX         string    `json:"id"`
		KindX       string    `json:"kind"`
		BckX        cmn.Bck   `json:"bck"`
		StartTimeX  time.Time `json:"start_time"`
		EndTimeX    time.Time `json:"end_time"`
		ObjCountX   int64     `json:"obj_count,string"`
		BytesCountX int64     `json:"bytes_count,string"`
		AbortedX    bool      `json:"aborted"`
	}
	BaseStatsExt struct {
		BaseStats
		Ext interface{} `json:"ext"`
	}
	BaseDemandStatsExt struct {
		IsIdle bool `json:"is_idle"`
	}

	QueryMsg struct {
		ID          string      `json:"id"`
		Kind        string      `json:"kind"`
		Bck         cmn.Bck     `json:"bck"`
		OnlyRunning *bool       `json:"show_active"`
		Buckets     []cmn.Bck   `json:"buckets,omitempty"` // list of buckets on which LRU should run
		Node        string      `json:"node,omitempty"`
		Ext         interface{} `json:"ext"`
	}

	QueryMsgLRU struct {
		Force bool `json:"force"`
	}
)

// interface guard
var _ cluster.XactStats = (*BaseStats)(nil)

///////////////
// BaseStats //
///////////////

func (b *BaseStats) ID() string           { return b.IDX }
func (b *BaseStats) Kind() string         { return b.KindX }
func (b *BaseStats) Bck() cmn.Bck         { return b.BckX }
func (b *BaseStats) StartTime() time.Time { return b.StartTimeX }
func (b *BaseStats) EndTime() time.Time   { return b.EndTimeX }
func (b *BaseStats) ObjCount() int64      { return b.ObjCountX }
func (b *BaseStats) BytesCount() int64    { return b.BytesCountX }
func (b *BaseStats) Aborted() bool        { return b.AbortedX }
func (b *BaseStats) Running() bool        { return b.EndTimeX.IsZero() }
func (b *BaseStats) Finished() bool       { return !b.EndTimeX.IsZero() }

//////////////
// QueryMsg //
//////////////
func (msg *QueryMsg) String() (s string) {
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
