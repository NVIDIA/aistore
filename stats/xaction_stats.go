// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type XactStats interface {
	Kind() string
	Bck() cmn.Bck
	StartTime() time.Time
	EndTime() time.Time
	ObjCount() int64
	BytesCount() int64
	Aborted() bool
	Running() bool
	Finished() bool
}

type BaseXactStats struct {
	KindX       string    `json:"kind"`
	BckX        cmn.Bck   `json:"bck"`
	StartTimeX  time.Time `json:"start_time"`
	EndTimeX    time.Time `json:"end_time"`
	ObjCountX   int64     `json:"obj_count,string"`
	BytesCountX int64     `json:"bytes_count,string"`
	AbortedX    bool      `json:"aborted"`
}

// Used to cast to generic stats type, with some more information in ext
type BaseXactStatsExt struct {
	BaseXactStats
	Ext interface{} `json:"ext"`
}

func NewXactStats(xact cmn.Xact) *BaseXactStats {
	return &BaseXactStats{
		KindX:       xact.Kind(),
		StartTimeX:  xact.StartTime(),
		EndTimeX:    xact.EndTime(),
		BckX:        xact.Bck(),
		ObjCountX:   xact.ObjectsCnt(),
		BytesCountX: xact.BytesCnt(),
		AbortedX:    xact.Aborted(),
	}
}

func (b *BaseXactStats) Kind() string         { return b.KindX }
func (b *BaseXactStats) Bck() cmn.Bck         { return b.BckX }
func (b *BaseXactStats) StartTime() time.Time { return b.StartTimeX }
func (b *BaseXactStats) EndTime() time.Time   { return b.EndTimeX }
func (b *BaseXactStats) ObjCount() int64      { return b.ObjCountX }
func (b *BaseXactStats) BytesCount() int64    { return b.BytesCountX }
func (b *BaseXactStats) Aborted() bool        { return b.AbortedX }
func (b *BaseXactStats) Running() bool        { return b.EndTimeX.IsZero() }
func (b *BaseXactStats) Finished() bool       { return !b.EndTimeX.IsZero() }

type RebalanceTargetStats struct {
	BaseXactStats
	Ext ExtRebalanceStats `json:"ext"`
}

type ExtRebalanceStats struct {
	TxRebCount int64 `json:"tx.reb.n,string"`
	TxRebSize  int64 `json:"tx.reb.size,string"`
	RxRebCount int64 `json:"rx.reb.n,string"`
	RxRebSize  int64 `json:"rx.reb.size,string"`
	RebID      int64 `json:"glob.id,string"`
}

func (s *RebalanceTargetStats) FillFromTrunner(r *Trunner) {
	s.Ext.RxRebSize = r.Core.get(RxRebSize)
	s.Ext.RxRebCount = r.Core.get(RxRebCount)
	s.Ext.TxRebSize = r.Core.get(TxRebSize)
	s.Ext.TxRebCount = r.Core.get(TxRebCount)
	s.Ext.RebID = r.T.RebalanceInfo().RebID

	s.ObjCountX = s.Ext.RxRebCount + s.Ext.TxRebCount
	s.BytesCountX = s.Ext.RxRebSize + s.Ext.TxRebSize
}
