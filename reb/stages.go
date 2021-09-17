// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
		targets map[string]uint32 // daemonID <-> stage
		// Info about this target rebalance status. This info is used oftener
		// than remote target ones, and updated more frequently locally.
		// That is why it uses atomics instead of global mutex
		stage atomic.Uint32 // rebStage* enum: this target current stage
	}
)

func newNodeStages() *nodeStages {
	return &nodeStages{targets: make(map[string]uint32)}
}

// Returns true if the target is in `newStage` or in any next stage
func (*nodeStages) stageReached(stage, newStage uint32) bool {
	return stage > newStage
}

// Mark a 'node' that it has reached the 'stage'. Do nothing if the target
// is already in this stage or has finished it already
func (ns *nodeStages) setStage(daemonID string, stage uint32) {
	ns.mtx.Lock()
	status, ok := ns.targets[daemonID]
	if !ok {
		ns.targets[daemonID] = status
	}

	if !ns.stageReached(status, stage) {
		ns.targets[daemonID] = stage
	}
	ns.mtx.Unlock()
}

// Returns true if the target is in `newStage` or in any next stage.
func (ns *nodeStages) isInStage(si *cluster.Snode, stage uint32) bool {
	ns.mtx.Lock()
	inStage := ns.isInStageUnlocked(si, stage)
	ns.mtx.Unlock()
	return inStage
}

// Returns true if the target is in `newStage` or in any next stage
func (ns *nodeStages) isInStageUnlocked(si *cluster.Snode, stage uint32) bool {
	status, ok := ns.targets[si.ID()]
	if !ok {
		return false
	}
	return ns.stageReached(status, stage)
}

func (ns *nodeStages) cleanup() {
	ns.mtx.Lock()
	for k := range ns.targets {
		delete(ns.targets, k)
	}
	ns.mtx.Unlock()
}
