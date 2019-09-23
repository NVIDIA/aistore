// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type XactStats interface {
	ID() int64
	Kind() string
	Bucket() string
	StartTime() time.Time
	EndTime() time.Time
	ObjCount() int64
	BytesCount() int64
	Aborted() bool
	Running() bool
	Finished() bool
}

type BaseXactStats struct {
	IDX         int64     `json:"id"`
	KindX       string    `json:"kind"`
	BucketX     string    `json:"bucket"`
	ProviderX   string    `json:"provider"`
	StartTimeX  time.Time `json:"start_time"`
	EndTimeX    time.Time `json:"end_time"`
	ObjCountX   int64     `json:"obj_count"`
	BytesCountX int64     `json:"bytes_count"`
	AbortedX    bool      `json:"aborted"`
}

// Used to cast to generic stats type, with some more information in ext
type BaseXactStatsExt struct {
	BaseXactStats
	Ext interface{} `json:"ext"`
}

func (b *BaseXactStats) ID() int64            { return b.IDX }
func (b *BaseXactStats) ShortID() uint32      { return cmn.ShortID(b.IDX) }
func (b *BaseXactStats) Kind() string         { return b.KindX }
func (b *BaseXactStats) Bucket() string       { return b.BucketX }
func (b *BaseXactStats) StartTime() time.Time { return b.StartTimeX }
func (b *BaseXactStats) EndTime() time.Time   { return b.EndTimeX }
func (b *BaseXactStats) ObjCount() int64      { return b.ObjCountX }
func (b *BaseXactStats) BytesCount() int64    { return b.BytesCountX }
func (b *BaseXactStats) Aborted() bool        { return b.AbortedX }
func (b *BaseXactStats) Running() bool        { return b.EndTimeX.IsZero() }
func (b *BaseXactStats) Finished() bool       { return !b.EndTimeX.IsZero() }
func (b *BaseXactStats) FillFromXact(xact cmn.Xact, bcks ...*cluster.Bck) *BaseXactStats {
	b.IDX = xact.ID()
	b.KindX = xact.Kind()
	b.StartTimeX = xact.StartTime()
	b.EndTimeX = xact.EndTime()
	if len(bcks) > 0 {
		bck := bcks[0]
		b.BucketX, b.ProviderX = bck.Name, bck.Provider
	}
	b.ObjCountX = xact.ObjectsCnt()
	b.BytesCountX = xact.BytesCnt()
	b.AbortedX = xact.Aborted()
	return b
}

type RebalanceTargetStats struct {
	BaseXactStats
	Ext ExtRebalanceStats `json:"ext"`
}

type ExtRebalanceStats struct {
	TxRebCount  int64 `json:"tx.reb.n"`
	TxRebSize   int64 `json:"tx.reb.size"`
	RxRebCount  int64 `json:"rx.reb.n"`
	RxRebSize   int64 `json:"rx.reb.size"`
	GlobalRebID int64 `json:"reb.glob.id"`
}

func (s *RebalanceTargetStats) FillFromTrunner(r *Trunner) {
	s.Ext.RxRebSize = r.Core.get(RxRebSize)
	s.Ext.RxRebCount = r.Core.get(RxRebCount)
	s.Ext.TxRebSize = r.Core.get(TxRebSize)
	s.Ext.TxRebCount = r.Core.get(TxRebCount)
	s.Ext.GlobalRebID = r.T.RebalanceInfo().GlobalRebID

	s.ObjCountX = s.Ext.RxRebCount + s.Ext.TxRebCount
	s.BytesCountX = s.Ext.RxRebSize + s.Ext.TxRebSize
}
