// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cluster"
)

type quiArgs struct {
	md   *rebArgs
	reb  *Reb
	done func(md *rebArgs) bool
}

func (q *quiArgs) quicb(_ time.Duration /*accum. wait time*/) cluster.QuiRes {
	if q.done(q.md) {
		return cluster.QuiDone
	}
	if q.reb.laterx.CAS(true, false) {
		return cluster.QuiActive
	}
	return cluster.QuiInactiveCB
}

// Uses generic xact.Quiesce to make sure that no objects are received
// during a given `maxWait` interval of time.
func (reb *Reb) quiesce(md *rebArgs, maxWait time.Duration, cb func(md *rebArgs) bool) cluster.QuiRes {
	q := &quiArgs{md, reb, cb}
	return reb.xctn().Quiesce(maxWait, q.quicb)
}

// Returns true if all transport queues are empty
func (reb *Reb) nodesQuiescent(md *rebArgs) (quiescent bool) {
	locStage := reb.stages.stage.Load()
	for _, si := range md.smap.Tmap {
		if si.ID() == reb.t.SID() && !reb.isQuiescent() {
			return
		}
		status, ok := reb.checkGlobStatus(si, locStage, md)
		if !ok || !status.Quiescent {
			return
		}
	}
	return true
}
