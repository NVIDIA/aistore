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
		ID         string    `json:"id"`
		Kind       string    `json:"kind"`
		Bck        cmn.Bck   `json:"bck"`
		StartTime  time.Time `json:"start_time"`
		EndTime    time.Time `json:"end_time"`
		ObjCount   int64     `json:"obj_count,string"`
		BytesCount int64     `json:"bytes_count,string"`
		AbortedX   bool      `json:"aborted"`
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

func (b *BaseStats) Aborted() bool  { return b.AbortedX }
func (b *BaseStats) Running() bool  { return b.EndTime.IsZero() }
func (b *BaseStats) Finished() bool { return !b.EndTime.IsZero() }

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
