// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact/xs"
)

func (reb *Reb) rebID() int64           { return reb.id.Load() }
func (reb *Reb) FilterAdd(uname []byte) { reb.filterGFN.Insert(uname) }

// (limited usage; compare with `abortAll` below)
func (reb *Reb) AbortLocal(olderSmapV int64, err error) {
	xreb := reb.xctn()
	if xreb == nil {
		return
	}
	// double-check
	smap := reb.smap.Load()
	if smap == nil {
		return
	}
	if smap.Version == olderSmapV {
		if xreb.Abort(err) {
			nlog.Warningf("%v - aborted", err)
			reb.lazydel.stop()
		}
	}
}

func (reb *Reb) xctn() *xs.Rebalance        { return reb.xreb.Load() }
func (reb *Reb) setXact(xctn *xs.Rebalance) { reb.xreb.Store(xctn) }

func (reb *Reb) logHdr(rebID int64, smap *meta.Smap, initializing ...bool) string {
	var (
		sb cos.SB
		l  = 64
	)
	sb.Init(l)

	sb.WriteString(core.T.String())
	sb.WriteString("[g")
	sb.WriteString(strconv.FormatInt(rebID, 10)) // (compare with `xact.RebID2S`)
	sb.WriteUint8(',')
	if smap != nil {
		sb.WriteUint8('v')
		sb.WriteString(strconv.FormatInt(smap.Version, 10))
	} else {
		sb.WriteString("v<???>")
	}
	if len(initializing) > 0 {
		sb.WriteUint8(']')
		return sb.String() // "%s[g%d,%s]"
	}
	sb.WriteUint8(',')
	sb.WriteString(stages[reb.stages.stage.Load()])
	sb.WriteUint8(']')

	return sb.String() // "%s[g%d,%s,%s]"
}

func (reb *Reb) warnID(remoteID int64, tid string) (s string) {
	const warn = "t[%s] runs %s g[%d] (local g[%d])"
	if id := reb.rebID(); id < remoteID {
		s = fmt.Sprintf(warn, tid, "newer", remoteID, id)
	} else {
		s = fmt.Sprintf(warn, tid, "older", remoteID, id)
	}
	return s
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *Reb) changeStage(newStage uint32) {
	// set our own stage
	reb.stages.stage.Store(newStage)

	// notify all
	var (
		ntfn = &stageNtfn{daemonID: core.T.SID(), stage: newStage, rebID: reb.rebID()}
		hdr  = transport.ObjHdr{}
	)
	hdr.Opaque = ntfn.NewPack(rebMsgNtfn)

	if err := reb.dm.Notif(&hdr); err != nil {
		nlog.Warningln("failed to bcast new-stage notif: [", ntfn.rebID, stages[newStage], err, "]")
	}
}

// Aborts global rebalance and notifies all other targets.
func (reb *Reb) abortAll(err error, xreb *xs.Rebalance) {
	if xreb == nil || !xreb.Abort(err) {
		return
	}
	reb.lazydel.stop()
	nlog.InfoDepth(1, xreb.Name(), "abort-and-bcast", err)

	var (
		ntfn = &stageNtfn{daemonID: core.T.SID(), rebID: reb.rebID(), stage: rebStageAbort}
		hdr  = transport.ObjHdr{}
	)
	hdr.Opaque = ntfn.NewPack(rebMsgNtfn)

	if err := reb.dm.Notif(&hdr); err != nil {
		nlog.Errorln("failed to bcast abort notif: [", ntfn.rebID, err, "]")
	}
}

/////////////
// lomAcks //
/////////////

// transaction: addLomAck => (cleanupLomAck | ackLomAck)

func (reb *Reb) lomAcks() *[cos.MultiHashMapCount]*lomAcks { return &reb.lomacks }

func (reb *Reb) addLomAck(lom *core.LOM) {
	lomAck := reb.lomAcks()[lom.CacheIdx()]
	lomAck.mu.Lock()
	lomAck.q[lom.Uname()] = lom.LIF()
	lomAck.mu.Unlock()
}

// called upon failure to send
func (reb *Reb) cleanupLomAck(lom *core.LOM) {
	lomAck := reb.lomAcks()[lom.CacheIdx()]

	lomAck.mu.Lock()
	delete(lomAck.q, lom.Uname())
	lomAck.mu.Unlock()
}

// called by recvRegularAck
func (reb *Reb) ackLomAck(lom *core.LOM, rebID int64, xreb *xs.Rebalance) {
	lomAck := reb.lomAcks()[lom.CacheIdx()]

	lomAck.mu.Lock()
	uname := lom.Uname()
	lif, ok := lomAck.q[uname] // via addLomAck() above
	if !ok {
		lomAck.mu.Unlock()
		return
	}
	debug.Assert(uname == lif.Uname)
	delete(lomAck.q, uname)
	lomAck.mu.Unlock()

	if lif.BID != lom.Bprops().BID {
		err := cmn.NewErrObjDefunct(lom.String(), lif.BID, lom.Bprops().BID)
		nlog.Warningln(err)
		return
	}
	debug.Assert(lif.Digest != 0)

	size := lif.Size

	// counting acknowledged migrations (as initiator)
	xreb.ObjsAdd(1, size)

	// NOTE: rm migrated object (and local copies, if any) right away
	// TODO [feature]: mark "deleted" instead
	if !cmn.Rom.Features().IsSet(feat.DontDeleteWhenRebalancing) {
		lom.UncacheDel()
		reb.lazydel.enqueue(lif, xreb.Name(), rebID)
	}
}
