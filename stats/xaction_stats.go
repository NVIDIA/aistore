// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018 - 2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"github.com/NVIDIA/aistore/cmn"
)

type RebalanceTargetStats struct {
	cmn.BaseXactStats
	Ext ExtRebalanceStats `json:"ext"`
}

type ExtRebalanceStats struct {
	RebTxCount int64 `json:"reb.tx.n,string"`
	RebTxSize  int64 `json:"reb.tx.size,string"`
	RebRxCount int64 `json:"reb.rx.n,string"`
	RebRxSize  int64 `json:"reb.rx.size,string"`
	RebID      int64 `json:"glob.id,string"`
}

var (
	// interface guard
	_ cmn.XactStats = &RebalanceTargetStats{}
)

func (s *RebalanceTargetStats) FillFromTrunner(r *Trunner) {
	s.Ext.RebTxCount = r.Core.get(RebTxCount)
	s.Ext.RebTxSize = r.Core.get(RebTxSize)
	s.Ext.RebRxCount = r.Core.get(RebRxCount)
	s.Ext.RebRxSize = r.Core.get(RebRxSize)
	s.Ext.RebID = r.T.RebalanceInfo().RebID

	s.ObjCountX = s.Ext.RebTxCount + s.Ext.RebRxCount
	s.BytesCountX = s.Ext.RebTxSize + s.Ext.RebRxSize
}
