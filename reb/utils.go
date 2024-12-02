// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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

func (reb *Reb) RebID() int64           { return reb.rebID.Load() }
func (reb *Reb) FilterAdd(uname []byte) { reb.filterGFN.Insert(uname) }

// (limited usage; compare with `abortAndBroadcast` below)
func (reb *Reb) AbortLocal(olderSmapV int64, err error) {
	if xreb := reb.xctn(); xreb != nil {
		// double-check
		smap := reb.smap.Load()
		if smap.Version == olderSmapV {
			if xreb.Abort(err) {
				nlog.Warningf("%v - aborted", err)
			}
		}
	}
}

func (reb *Reb) xctn() *xs.Rebalance        { return reb.xreb.Load() }
func (reb *Reb) setXact(xctn *xs.Rebalance) { reb.xreb.Store(xctn) }

func (reb *Reb) logHdr(rebID int64, smap *meta.Smap, initializing ...bool) string {
	var (
		sb strings.Builder
		l  = 64
	)
	sb.Grow(l)

	sb.WriteString(core.T.String())
	sb.WriteString("[g")
	sb.WriteString(strconv.FormatInt(rebID, 10)) // (compare with `xact.RebID2S`)
	sb.WriteByte(',')
	if smap != nil {
		sb.WriteByte('v')
		sb.WriteString(strconv.FormatInt(smap.Version, 10))
	} else {
		sb.WriteString("v<???>")
	}
	if len(initializing) > 0 {
		sb.WriteByte(']')
		return sb.String() // "%s[g%d,%s]"
	}
	sb.WriteByte(',')
	sb.WriteString(stages[reb.stages.stage.Load()])
	sb.WriteByte(']')

	return sb.String() // "%s[g%d,%s,%s]"
}

func (reb *Reb) warnID(remoteID int64, tid string) (s string) {
	const warn = "t[%s] runs %s g[%d] (local g[%d])"
	if id := reb.RebID(); id < remoteID {
		s = fmt.Sprintf(warn, tid, "newer", remoteID, id)
	} else {
		s = fmt.Sprintf(warn, tid, "older", remoteID, id)
	}
	return s
}

func (reb *Reb) _waitForSmap() (smap *meta.Smap, err error) {
	smap = reb.smap.Load()
	if smap != nil {
		return
	}
	var (
		config = cmn.GCO.Get()
		sleep  = cmn.Rom.CplaneOperation()
		maxwt  = config.Rebalance.DestRetryTime.D()
		curwt  time.Duration
	)
	maxwt = min(maxwt, config.Timeout.SendFile.D()/3)
	nlog.Warningf("%s: waiting to start...", core.T)
	time.Sleep(sleep)
	for curwt < maxwt {
		smap = reb.smap.Load()
		if smap != nil {
			return
		}
		time.Sleep(sleep)
		curwt += sleep
	}
	return nil, fmt.Errorf("%s: timed out waiting for usable Smap", core.T)
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *Reb) changeStage(newStage uint32) {
	// first, set own stage
	reb.stages.stage.Store(newStage)
	var (
		req = stageNtfn{
			daemonID: core.T.SID(), stage: newStage, rebID: reb.RebID(),
		}
		hdr = transport.ObjHdr{}
	)
	hdr.Opaque = reb.encodeStageNtfn(&req)
	// second, notify all
	if err := reb.pushes.Send(&transport.Obj{Hdr: hdr}, nil); err != nil {
		nlog.Warningln("failed to push new-stage notif: [", req.rebID, stages[newStage], err, "]")
	}
}

// Aborts global rebalance and notifies all other targets.
// (compare with `Abort` above)
func (reb *Reb) abortAndBroadcast(err error) {
	xreb := reb.xctn()
	if xreb == nil || !xreb.Abort(err) {
		return
	}
	nlog.InfoDepth(1, xreb.Name(), "abort-and-bcast", err)

	var (
		req = stageNtfn{
			daemonID: core.T.SID(),
			rebID:    reb.RebID(),
			stage:    rebStageAbort,
		}
		hdr = transport.ObjHdr{}
	)
	hdr.Opaque = reb.encodeStageNtfn(&req)
	if err := reb.pushes.Send(&transport.Obj{Hdr: hdr}, nil); err != nil {
		nlog.Errorln("failed to broadcast abort notif: [", req.rebID, err, "]")
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
	lomAck.q[lom.Uname()] = lom
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
func (reb *Reb) ackLomAck(lom *core.LOM) {
	lomAck := reb.lomAcks()[lom.CacheIdx()]

	lomAck.mu.Lock()
	uname := lom.Uname()
	lomOrig, ok := lomAck.q[uname] // via addLomAck() above
	if !ok {
		lomAck.mu.Unlock()
		return
	}
	delete(lomAck.q, uname)
	lomAck.mu.Unlock()

	debug.Assert(uname == lomOrig.Uname())
	size := lomOrig.Lsize()
	core.FreeLOM(lomOrig)

	// counting acknowledged migrations (as initiator)
	xreb := reb.xctn()
	xreb.ObjsAdd(1, size)

	// NOTE: rm migrated object (and local copies, if any) right away
	// TODO [feature]: mark "deleted" instead
	if !cmn.Rom.Features().IsSet(feat.DontDeleteWhenRebalancing) {
		lom.Lock(true)
		err := lom.RemoveObj()
		lom.Unlock(true)
		debug.AssertNoErr(err)
	}
}
