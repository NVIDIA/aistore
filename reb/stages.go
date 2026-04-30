// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/core/meta"
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

// Returns true if the target is at `newStage` or any next stage
func (*nodeStages) stageReached(stage, newStage uint32) bool {
	return stage >= newStage
}

// Record that daemonID has reached `stage`. No-op if already at this stage or beyond
// (forward-only transition).
func (ns *nodeStages) setStage(daemonID string, stage uint32) {
	ns.mtx.Lock()
	cur, ok := ns.targets[daemonID]
	if !ok || cur < stage {
		ns.targets[daemonID] = stage
	}
	ns.mtx.Unlock()
}

// Returns true if the target is at `stage` or any later stage.
func (ns *nodeStages) isInStage(tsi *meta.Snode, stage uint32) bool {
	ns.mtx.Lock()
	cur, ok := ns.targets[tsi.ID()]
	ns.mtx.Unlock()

	return ok && ns.stageReached(cur, stage)
}

func (ns *nodeStages) cleanup() {
	ns.mtx.Lock()
	clear(ns.targets)
	ns.mtx.Unlock()
}
