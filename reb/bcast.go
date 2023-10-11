// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	jsoniter "github.com/json-iterator/go"
)

type (
	syncCallback func(tsi *meta.Snode, rargs *rebArgs) (ok bool)

	Status struct {
		Targets     meta.Nodes    `json:"targets"`             // targets I'm waiting for ACKs from
		SmapVersion int64         `json:"smap_version,string"` // current Smap version (via smapOwner)
		RebVersion  int64         `json:"reb_version,string"`  // Smap version of *this* rebalancing op
		RebID       int64         `json:"reb_id,string"`       // rebalance ID
		Stats       cluster.Stats `json:"stats"`               // transmitted/received totals
		Stage       uint32        `json:"stage"`               // the current stage - see enum above
		Aborted     bool          `json:"aborted"`             // aborted?
		Running     bool          `json:"running"`             // running?
		Quiescent   bool          `json:"quiescent"`           // true when queue is empty
	}
)

////////////////////////////////////////////
// rebalance manager: node <=> node comm. //
////////////////////////////////////////////

// main method
func (reb *Reb) bcast(rargs *rebArgs, cb syncCallback) (errCnt int) {
	var (
		cnt atomic.Int32
		wg  = cos.NewLimitedWaitGroup(cmn.MaxBcastParallel(), len(rargs.smap.Tmap))
	)
	for _, tsi := range rargs.smap.Tmap {
		if tsi.ID() == reb.t.SID() {
			continue
		}
		wg.Add(1)
		go func(tsi *meta.Snode) {
			if !cb(tsi, rargs) {
				cnt.Inc()
			}
			wg.Done()
		}(tsi)
	}
	wg.Wait()
	errCnt = int(cnt.Load())
	return
}

// pingTarget checks if target is running (type syncCallback)
// TODO: reuse keepalive
func (reb *Reb) pingTarget(tsi *meta.Snode, rargs *rebArgs) (ok bool) {
	var (
		ver    = rargs.smap.Version
		sleep  = cmn.Rom.CplaneOperation()
		logHdr = reb.logHdr(rargs.id, rargs.smap)
		tname  = tsi.StringEx()
	)
	for i := 0; i < 4; i++ {
		_, code, err := reb.t.Health(tsi, cmn.Rom.MaxKeepalive(), nil)
		if err == nil {
			if i > 0 {
				nlog.Infof("%s: %s is online", logHdr, tname)
			}
			return true
		}
		if !cos.IsUnreachable(err, code) {
			nlog.Errorf("%s: health(%s) returned %v(%d) - aborting", logHdr, tname, err, code)
			return
		}
		nlog.Warningf("%s: waiting for %s, err %v(%d)", logHdr, tname, err, code)
		time.Sleep(sleep)
		nver := reb.t.Sowner().Get().Version
		if nver > ver {
			return
		}
	}
	nlog.Errorf("%s: timed out waiting for %s", logHdr, tname)
	return
}

// wait for target to get ready to receive objects (type syncCallback)
func (reb *Reb) rxReady(tsi *meta.Snode, rargs *rebArgs) (ok bool) {
	var (
		curwt time.Duration
		sleep = cmn.Rom.CplaneOperation() * 2
		maxwt = rargs.config.Rebalance.DestRetryTime.D() + rargs.config.Rebalance.DestRetryTime.D()/2
		xreb  = reb.xctn()
	)
	for curwt < maxwt {
		if reb.stages.isInStage(tsi, rebStageTraverse) {
			// do not request the node stage if it has sent stage notification
			return true
		}
		if _, ok = reb.checkStage(tsi, rargs, rebStageTraverse); ok {
			return
		}
		if err := xreb.AbortedAfter(sleep); err != nil {
			return
		}
		curwt += sleep
	}
	logHdr, tname := reb.logHdr(rargs.id, rargs.smap), tsi.StringEx()
	nlog.Errorf("%s: timed out waiting for %s to reach %s state", logHdr, tname, stages[rebStageTraverse])
	return
}

// wait for the target to reach strage = rebStageFin (i.e., finish traversing and sending)
// if the target that has reached rebStageWaitAck but not yet in the rebStageFin stage,
// separately check whether it is waiting for my ACKs
func (reb *Reb) waitFinExtended(tsi *meta.Snode, rargs *rebArgs) (ok bool) {
	var (
		curwt      time.Duration
		status     *Status
		sleep      = rargs.config.Timeout.CplaneOperation.D()
		maxwt      = rargs.config.Rebalance.DestRetryTime.D()
		sleepRetry = cmn.KeepaliveRetryDuration(rargs.config)
		logHdr     = reb.logHdr(rargs.id, rargs.smap)
		xreb       = reb.xctn()
	)
	debug.Assertf(reb.RebID() == xreb.RebID(), "%s (rebID=%d) vs %s", logHdr, reb.RebID(), xreb)
	for curwt < maxwt {
		if err := xreb.AbortedAfter(sleep); err != nil {
			nlog.Infof("%s: abort wack (%v)", logHdr, err)
			return
		}
		if reb.stages.isInStage(tsi, rebStageFin) {
			return true // tsi stage=<fin>
		}
		// otherwise, inquire status and check the stage
		curwt += sleep
		if status, ok = reb.checkStage(tsi, rargs, rebStageFin); ok || status == nil {
			return
		}
		if err := xreb.AbortErr(); err != nil {
			nlog.Infof("%s: abort wack (%v)", logHdr, err)
			return
		}
		//
		// tsi in rebStageWaitAck
		//
		var w4me bool // true: this target is waiting for ACKs from me
		for _, si := range status.Targets {
			if si.ID() == reb.t.SID() {
				nlog.Infof("%s: keep wack <= %s[%s]", logHdr, tsi.StringEx(), stages[status.Stage])
				w4me = true
				break
			}
		}
		if !w4me {
			nlog.Infof("%s: %s[%s] ok (not waiting for me)", logHdr, tsi.StringEx(), stages[status.Stage])
			ok = true
			return
		}
		time.Sleep(sleepRetry)
		curwt += sleepRetry
	}
	nlog.Errorf("%s: timed out waiting for %s to reach %s", logHdr, tsi.StringEx(), stages[rebStageFin])
	return
}

// calls tsi.reb.RebStatus() and handles conditions; may abort the current xreb
// returns:
// - `Status` or nil
// - OK iff the desiredStage has been reached
func (reb *Reb) checkStage(tsi *meta.Snode, rargs *rebArgs, desiredStage uint32) (status *Status, ok bool) {
	var (
		sleepRetry = cmn.KeepaliveRetryDuration(rargs.config)
		logHdr     = reb.logHdr(rargs.id, rargs.smap)
		query      = url.Values{apc.QparamRebStatus: []string{"true"}}
		xreb       = reb.xctn()
		tname      = tsi.StringEx()
	)
	if xreb == nil || xreb.IsAborted() {
		return
	}
	debug.Assertf(reb.RebID() == xreb.RebID(), "%s (rebID=%d) vs %s", logHdr, reb.RebID(), xreb)
	body, code, err := reb.t.Health(tsi, apc.DefaultTimeout, query)
	if err != nil {
		if errAborted := xreb.AbortedAfter(sleepRetry); errAborted != nil {
			nlog.Infoln(logHdr, "abort check status", errAborted)
			return
		}
		body, code, err = reb.t.Health(tsi, apc.DefaultTimeout, query) // retry once
	}
	if err != nil {
		ctx := fmt.Sprintf("health(%s) failure: %v(%d)", tname, err, code)
		err = cmn.NewErrAborted(xreb.Name(), ctx, err)
		reb.abortAndBroadcast(err)
		return
	}

	status = &Status{}
	err = jsoniter.Unmarshal(body, status)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, logHdr, "reb status from "+tname, cos.BHead(body), err)
		reb.abortAndBroadcast(err)
		return
	}
	// enforce global transaction ID
	if status.RebID > reb.rebID.Load() {
		err := cmn.NewErrAborted(xreb.Name(), logHdr, fmt.Errorf("%s runs newer g%d", tname, status.RebID))
		reb.abortAndBroadcast(err)
		return
	}
	if xreb.IsAborted() {
		return
	}
	// let the target to catch-up
	if status.RebID < reb.RebID() {
		nlog.Warningf("%s: %s runs older (g%d) global rebalance - keep waiting...", logHdr, tname, status.RebID)
		return
	}
	// Remote target has aborted its running rebalance with the same ID.
	// Do not call `reb.abortAndBroadcast()` - no need.
	if status.RebID == reb.RebID() && status.Aborted {
		err := cmn.NewErrAborted(xreb.Name(), logHdr, fmt.Errorf("status 'aborted' from %s", tname))
		xreb.Abort(err)
		return
	}
	if status.Stage >= desiredStage {
		ok = true
		return
	}
	nlog.Infof("%s: %s[%s] not yet at the right stage %s", logHdr, tname, stages[status.Stage], stages[desiredStage])
	return
}
