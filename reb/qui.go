// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/fs/glob"
)

type quiArgs struct {
	rargs *rebArgs
	reb   *Reb
	done  func(rargs *rebArgs) bool
}

func (q *quiArgs) quicb(_ time.Duration /*accum. wait time*/) cluster.QuiRes {
	if q.done(q.rargs) {
		return cluster.QuiDone
	}
	if q.reb.laterx.CAS(true, false) {
		return cluster.QuiActive
	}
	return cluster.QuiInactiveCB
}

// Uses generic xact.Quiesce to make sure that no objects are received
// during a given `maxWait` interval of time.
func (reb *Reb) quiesce(rargs *rebArgs, maxWait time.Duration, cb func(rargs *rebArgs) bool) cluster.QuiRes {
	q := &quiArgs{rargs, reb, cb}
	return reb.xctn().Quiesce(maxWait, q.quicb)
}

// Returns true if all transport queues are empty
func (reb *Reb) nodesQuiescent(rargs *rebArgs) (quiescent bool) {
	locStage := reb.stages.stage.Load()
	for _, si := range rargs.smap.Tmap {
		if si.ID() == glob.T.SID() && !reb.isQuiescent() {
			return
		}
		status, ok := reb.checkStage(si, rargs, locStage)
		if !ok || !status.Quiescent {
			return
		}
	}
	return true
}
