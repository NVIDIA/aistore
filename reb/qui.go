// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cluster"
)

type quiArgs struct {
	md   *rebArgs
	reb  *Manager
	done func(md *rebArgs) bool
}

func (q *quiArgs) quicb(_ time.Duration /*accum. wait time*/) cluster.QuiRes {
	if q.done(q.md) {
		return cluster.QuiDone
	}
	if !q.reb.laterx.CAS(true, false) {
		return cluster.QuiInactive
	}
	return cluster.QuiActive
}

// Uses generic xaction.Quiesce to make sure that no objects are received
// during a given `maxWait` interval of time.
// Callback `cb` is used, e.g., to wait for EC batch to finish - no need
// to wait if all targets have sent push notifications.
func (reb *Manager) quiesce(md *rebArgs, maxWait time.Duration, cb func(md *rebArgs) bool) cluster.QuiRes {
	q := &quiArgs{md, reb, cb}
	return reb.xact().Quiesce(maxWait, q.quicb)
}

// Returns true if all transport queues are empty
func (reb *Manager) nodesQuiescent(md *rebArgs) (quiescent bool) {
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

// Ensures that no objects are stuck waiting for a slice from a remote
// target. For each object marked waiting it re-requests the slices
// from other targets, and uses those slices to rebuild the main replica.
func (reb *Manager) allCTReceived(md *rebArgs) bool {
	toWait, toRebuild := reb.toWait(md.config.EC.BatchSize)
	if toWait == 0 && toRebuild == 0 {
		return true
	}
	if toWait != 0 {
		reb.reRequest(md)
	}
	reb.rebuildReceived(md)
	return true
}
