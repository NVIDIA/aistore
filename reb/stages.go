// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/atomic"
)

////////////////
// nodeStages //
////////////////

type nodeStages struct {
	targets map[string]uint32 // remote tid <-> stage
	stage   atomic.Uint32     // rebStage* enum: my own current stage
	mtx     sync.Mutex        // updated from different goroutines
}

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
