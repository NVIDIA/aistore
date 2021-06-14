// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
)

////////////////
// nodeStages //
////////////////

type (
	nodeStages struct {
		// Info about remote targets. It needs mutex for it can be
		// updated from different goroutines
		mtx     sync.Mutex
		targets map[string]*stageStatus // daemonID <-> stageStatus
		// Info about this target rebalance status. This info is used oftener
		// than remote target ones, and updated more frequently locally.
		// That is why it uses atomics instead of global mutex
		currBatch atomic.Int64  // EC rebalance: current batch ID
		lastBatch atomic.Int64  // EC rebalance: ID of the last batch
		stage     atomic.Uint32 // rebStage* enum: this target current stage
	}
)

func newNodeStages() *nodeStages {
	return &nodeStages{targets: make(map[string]*stageStatus)}
}

// Returns true if the target is in `newStage` or in any next stage
func (*nodeStages) stageReached(status *stageStatus, newStage uint32, newBatchID int64) bool {
	// for simple stages: just check the stage
	if newBatchID == 0 {
		return status.stage >= newStage
	}
	// for cyclic stage (used in EC): check both batch ID and stage
	return status.batchID > newBatchID ||
		(status.batchID == newBatchID && status.stage >= newStage) ||
		(status.stage >= rebStageECCleanup && status.stage > newStage)
}

// Mark a 'node' that it has reached the 'stage'. Do nothing if the target
// is already in this stage or has finished it already
func (ns *nodeStages) setStage(daemonID string, stage uint32, batchID int64) {
	ns.mtx.Lock()
	status, ok := ns.targets[daemonID]
	if !ok {
		status = &stageStatus{}
		ns.targets[daemonID] = status
	}

	if !ns.stageReached(status, stage, batchID) {
		status.stage = stage
		status.batchID = batchID
	}
	ns.mtx.Unlock()
}

// Returns true if the target is in `newStage` or in any next stage.
func (ns *nodeStages) isInStage(si *cluster.Snode, stage uint32) bool {
	ns.mtx.Lock()
	inStage := ns.isInStageBatchUnlocked(si, stage, 0)
	ns.mtx.Unlock()
	return inStage
}

// Returns true if the target is in `newStage` and has reached the given
// batch ID or it is in any next stage
func (ns *nodeStages) isInStageBatchUnlocked(si *cluster.Snode, stage uint32, batchID int64) bool {
	status, ok := ns.targets[si.ID()]
	if !ok {
		return false
	}
	return ns.stageReached(status, stage, batchID)
}

func (ns *nodeStages) cleanup() {
	ns.mtx.Lock()
	for k := range ns.targets {
		delete(ns.targets, k)
	}
	ns.mtx.Unlock()
}
