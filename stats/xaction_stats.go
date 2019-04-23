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
	Count() int64
	ID() int64
	Kind() string
	Bucket() string
	StartTime() time.Time
	EndTime() time.Time
	Status() string
}

type BaseXactStats struct {
	IDX        int64     `json:"id"`
	KindX      string    `json:"kind"`
	BucketX    string    `json:"bucket"`
	StartTimeX time.Time `json:"start_time"`
	EndTimeX   time.Time `json:"end_time"`
	StatusX    string    `json:"status"`
	XactCountX int64     `json:"count"`
}

// Used to cast to generic stats type, with some more information in ext
//nolint:unused
type BaseXactStatsExt struct {
	BaseXactStats
	Ext interface{} `json:"ext"`
}

func (b *BaseXactStats) Count() int64 {
	return b.XactCountX
}
func (b *BaseXactStats) ID() int64            { return b.IDX }
func (b *BaseXactStats) Kind() string         { return b.KindX }
func (b *BaseXactStats) Bucket() string       { return b.BucketX }
func (b *BaseXactStats) StartTime() time.Time { return b.StartTimeX }
func (b *BaseXactStats) EndTime() time.Time   { return b.EndTimeX }
func (b *BaseXactStats) Status() string       { return b.StatusX }
func (b *BaseXactStats) FromXact(xact cmn.Xact, bucket string) *BaseXactStats {
	b.StatusX = cmn.XactionStatusInProgress
	if xact.Finished() {
		b.StatusX = cmn.XactionStatusCompleted
	}
	b.IDX = xact.ID()
	b.KindX = xact.Kind()
	b.StartTimeX = xact.StartTime()
	b.EndTimeX = xact.EndTime()
	b.BucketX = bucket
	return b
}

type RebalanceTargetStats struct {
	BaseXactStats
	Ext ExtRebalanceStats `json:"ext"`
}

type ExtRebalanceStats struct {
	NumSentFiles int64 `json:"num_sent_files"`
	NumSentBytes int64 `json:"num_sent_bytes"`
	NumRecvFiles int64 `json:"num_recv_files"`
	NumRecvBytes int64 `json:"num_recv_bytes"`
}

func (s *RebalanceTargetStats) FillFromTrunner(r *Trunner) {
	vr := r.Core.Tracker[RxCount]
	vt := r.Core.Tracker[TxCount]
	vr.RLock()
	vt.RLock()

	s.Ext.NumRecvBytes = r.Core.Tracker[RxSize].Value
	s.Ext.NumRecvFiles = r.Core.Tracker[RxCount].Value
	s.Ext.NumSentBytes = r.Core.Tracker[TxSize].Value
	s.Ext.NumSentFiles = r.Core.Tracker[TxCount].Value

	vt.RUnlock()
	vr.RUnlock()
}

type PrefetchTargetStats struct {
	BaseXactStats
	Ext ExtPrefetchStats `json:"ext"`
}

type ExtPrefetchStats struct {
	NumFilesPrefetched int64 `json:"num_files_prefetched"`
	NumBytesPrefetched int64 `json:"num_bytes_prefetched"`
}

func (s *PrefetchTargetStats) FillFromStatsRunner(r *Trunner) {
	v := r.Core.Tracker[PrefetchCount]
	v.RLock()

	s.Ext.NumBytesPrefetched = r.Core.Tracker[PrefetchSize].Value
	s.Ext.NumFilesPrefetched = r.Core.Tracker[PrefetchCount].Value
	v.RUnlock()
}
