// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xs"
)

func (reb *Reb) RebID() int64           { return reb.rebID.Load() }
func (reb *Reb) FilterAdd(uname []byte) { reb.filterGFN.Insert(uname) }

// (limited usage; compare with `abortAndBroadcast` below)
func (reb *Reb) AbortLocal(olderSmapV int64, err error) {
	if xreb := reb.xctn(); xreb != nil {
		// double-check
		smap := (*cluster.Smap)(reb.smap.Load())
		if smap.Version == olderSmapV {
			if xreb.Abort(err) {
				glog.Warningf("%v - aborted", err)
			}
		}
	}
}

func (reb *Reb) xctn() *xs.Rebalance        { return (*xs.Rebalance)(reb.xreb.Load()) }
func (reb *Reb) setXact(xctn *xs.Rebalance) { reb.xreb.Store(unsafe.Pointer(xctn)) }

func (reb *Reb) logHdr(rebID int64, smap *cluster.Smap) string {
	stage := stages[reb.stages.stage.Load()]
	smapv := "v<???>"
	if smap != nil {
		smapv = "v" + strconv.FormatInt(smap.Version, 10)
	}
	return fmt.Sprintf("%s[g%d,%s,%s]", reb.t, rebID, smapv, stage)
}

func (reb *Reb) warnID(remoteID int64, tid string) (s string) {
	const warn = "t[%s] runs %s g[%d] (local g[%d])"
	if id := reb.RebID(); id < remoteID {
		s = fmt.Sprintf(warn, tid, "newer", remoteID, id)
	} else {
		s = fmt.Sprintf(warn, tid, "older", remoteID, id)
	}
	return
}

func (reb *Reb) _waitForSmap() (smap *cluster.Smap, err error) {
	smap = (*cluster.Smap)(reb.smap.Load())
	if smap != nil {
		return
	}
	var (
		config = cmn.GCO.Get()
		sleep  = cmn.Timeout.CplaneOperation()
		maxwt  = config.Rebalance.DestRetryTime.D()
		curwt  time.Duration
	)
	maxwt = cos.MinDuration(maxwt, config.Timeout.SendFile.D()/3)
	glog.Warningf("%s: waiting to start...", reb.t)
	time.Sleep(sleep)
	for curwt < maxwt {
		smap = (*cluster.Smap)(reb.smap.Load())
		if smap != nil {
			return
		}
		time.Sleep(sleep)
		curwt += sleep
	}
	return nil, fmt.Errorf("%s: timed out waiting for usable Smap", reb.t)
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *Reb) changeStage(newStage uint32) {
	// first, set own stage
	reb.stages.stage.Store(newStage)
	var (
		req = stageNtfn{
			daemonID: reb.t.SID(), stage: newStage, rebID: reb.rebID.Load(),
		}
		hdr = transport.ObjHdr{}
	)
	hdr.Opaque = reb.encodeStageNtfn(&req)
	// second, notify all
	if err := reb.pushes.Send(&transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Warningf("Failed to broadcast ack %s: %v", stages[newStage], err)
	}
}

// Aborts global rebalance and notifies all other targets.
// (compare with `Abort` above)
func (reb *Reb) abortAndBroadcast(err error) {
	if xreb := reb.xctn(); xreb == nil || !xreb.Abort(err) {
		return
	}
	var (
		req = stageNtfn{
			daemonID: reb.t.SID(),
			rebID:    reb.RebID(),
			stage:    rebStageAbort,
		}
		hdr = transport.ObjHdr{}
	)
	hdr.Opaque = reb.encodeStageNtfn(&req)
	if err := reb.pushes.Send(&transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Errorf("Failed to broadcast abort notification: %v", err)
	}
}

// Returns if the target is quiescent: transport queue is empty, or xaction
// has already aborted or finished.
func (reb *Reb) isQuiescent() bool {
	// Finished or aborted xaction = no traffic
	xctn := reb.xctn()
	if xctn == nil || xctn.IsAborted() || xctn.Finished() {
		return true
	}

	// Check for both regular and EC transport queues are empty
	return reb.inQueue.Load() == 0 && reb.onAir.Load() == 0
}

/////////////
// lomAcks TODO: lomAck.q[lom.Uname()] = lom.Bprops().BID and, subsequently, LIF => LOM reinit
/////////////

func (reb *Reb) lomAcks() *[cos.MultiSyncMapCount]*lomAcks { return &reb.lomacks }

func (reb *Reb) addLomAck(lom *cluster.LOM) {
	idx := lom.CacheIdx()
	lomAck := reb.lomAcks()[idx]
	lomAck.mu.Lock()
	lomAck.q[lom.Uname()] = lom
	lomAck.mu.Unlock()
}

func (reb *Reb) delLomAck(lom *cluster.LOM) {
	idx := lom.CacheIdx()
	lomAck := reb.lomAcks()[idx]
	lomAck.mu.Lock()
	if lomOrig, ok := lomAck.q[lom.Uname()]; ok {
		delete(lomAck.q, lom.Uname())
		cluster.FreeLOM(lomOrig) // NOTE: free the original (pending) LOM
	}
	lomAck.mu.Unlock()
}
