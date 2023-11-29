// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// via GET /v1/health (apc.Health)
func (reb *Reb) RebStatus(status *Status) {
	var (
		tsmap  = reb.t.Sowner().Get()
		marked = xreg.GetRebMarked()
	)
	status.Aborted = marked.Interrupted
	status.Running = marked.Xact != nil && marked.Xact.Running()

	// rlock
	reb.mu.RLock()
	status.Stage = reb.stages.stage.Load()
	status.RebID = reb.rebID.Load()
	status.Quiescent = reb.isQuiescent()
	status.SmapVersion = tsmap.Version
	smap := reb.smap.Load()
	if smap != nil {
		status.RebVersion = smap.Version
	}
	reb.mu.RUnlock()

	// xreb, ?running
	xreb := reb.xctn()
	if xreb != nil {
		status.Aborted = xreb.IsAborted()
		status.Running = xreb.Running()
		xreb.ToStats(&status.Stats)
		if status.Running {
			if marked.Xact != nil && marked.Xact.ID() != xreb.ID() {
				id, _ := xact.S2RebID(marked.Xact.ID())
				debug.Assert(id > xreb.RebID(), marked.Xact.String()+" vs "+xreb.String())
				nlog.Warningf("%s: must be transitioning (renewing) from %s (stage %s) to %s",
					reb.t, xreb, stages[status.Stage], marked.Xact)
				status.Running = false // not yet
			} else {
				debug.Assertf(reb.RebID() == xreb.RebID(), "rebID[%d] vs %s", reb.RebID(), xreb)
			}
		}
	} else if status.Running {
		nlog.Warningln(reb.t.String()+": transitioning (renewing) to", marked.Xact.String())
		status.Running = false
	}

	// wack status
	if smap == nil || status.Stage != rebStageWaitAck {
		return
	}
	if status.SmapVersion != status.RebVersion {
		nlog.Warningf("%s: Smap v%d != %d", reb.t, status.SmapVersion, status.RebVersion)
		return
	}
	reb.awaiting.mtx.Lock()
	reb.wackStatus(status, smap)
	reb.awaiting.mtx.Unlock()
}

// extended info when stage is <wack>
func (reb *Reb) wackStatus(status *Status, rsmap *meta.Smap) {
	var (
		config     = cmn.GCO.Get()
		sleepRetry = cmn.KeepaliveRetryDuration(config)
	)
	now := mono.NanoTime()
	if time.Duration(now-reb.awaiting.ts) < sleepRetry {
		status.Targets = reb.awaiting.targets
		return
	}
	reb.awaiting.ts = now
	reb.awaiting.targets = reb.awaiting.targets[:0]
	for _, lomAcks := range reb.lomAcks() {
		lomAcks.mu.Lock()
		reb.awaiting.targets = _wackStatusLom(lomAcks, reb.awaiting.targets, rsmap)
		lomAcks.mu.Unlock()
	}
	status.Targets = reb.awaiting.targets
}

func _wackStatusLom(lomAcks *lomAcks, targets meta.Nodes, rsmap *meta.Smap) meta.Nodes {
outer:
	for _, lom := range lomAcks.q {
		tsi, err := rsmap.HrwHash2T(lom.Digest())
		if err != nil {
			continue
		}
		for _, si := range targets {
			if si.ID() == tsi.ID() {
				continue outer
			}
		}
		targets = append(targets, tsi)
		if len(targets) >= maxWackTargets { // limit reporting
			break
		}
	}
	return targets
}
