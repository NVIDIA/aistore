// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
)

type (
	syncCallback func(tsi *meta.Snode, rargs *rargs) (ok bool)

	Status struct {
		Targets     meta.Nodes `json:"targets"`             // targets I'm waiting for ACKs from
		SmapVersion int64      `json:"smap_version,string"` // current Smap version (via smapOwner)
		RebVersion  int64      `json:"reb_version,string"`  // Smap version of *this* rebalancing op
		RebID       int64      `json:"reb_id,string"`       // rebalance ID
		Stats       core.Stats `json:"stats"`               // transmitted/received totals
		Stage       uint32     `json:"stage"`               // the current stage - see enum above
		Aborted     bool       `json:"aborted"`             // aborted?
		Running     bool       `json:"running"`             // running?
	}
)

////////////////////////////////////////////
// rebalance manager: node <=> node comm. //
////////////////////////////////////////////

// main method
func bcast(rargs *rargs, cb syncCallback) (errCnt int) {
	var (
		cnt atomic.Int32
		wg  = cos.NewLimitedWaitGroup(sys.MaxParallelism(), len(rargs.smap.Tmap))
	)
	for _, tsi := range rargs.smap.Tmap {
		if tsi.ID() == core.T.SID() {
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
	return int(cnt.Load())
}

// pingTarget checks if target is running (type syncCallback)
func _pingTarget(tsi *meta.Snode, rargs *rargs) (ok bool) {
	const retries = 4
	var (
		ver   = rargs.smap.Version
		sleep = cmn.Rom.CplaneOperation()
		tname = tsi.StringEx()
	)
	for i := range retries {
		_, code, err := core.T.Health(tsi, cmn.Rom.MaxKeepalive(), nil)
		if err == nil {
			if i > 0 {
				nlog.Infoln(rargs.logHdr, tname, "is now reachable")
			}
			return true
		}
		if !cos.IsUnreachable(err, code) {
			nlog.Errorf("%s: health(%s) returned %v(%d) - aborting", rargs.logHdr, tname, err, code)
			return
		}
		nlog.Warningln(rargs.logHdr, "waiting for:", tname, "[", err, code, "]")
		time.Sleep(sleep)
		nver := core.T.Sowner().Get().Version
		if nver > ver {
			return
		}
	}
	nlog.Errorln(rargs.logHdr, "timed out waiting for:", tname)
	return
}

// wait for target to get ready to receive objects (type syncCallback)
func (reb *Reb) rxReady(tsi *meta.Snode, rargs *rargs) bool /*ready*/ {
	var (
		curwt time.Duration
		sleep = cmn.Rom.CplaneOperation() * 2
		maxwt = rargs.config.Rebalance.DestRetryTime.D() + rargs.config.Rebalance.DestRetryTime.D()/2
		xreb  = rargs.xreb
	)
	for curwt < maxwt {
		if reb.stages.isInStage(tsi, rebStageTraverse) {
			// do not request the node stage if it has sent stage notification
			return true
		}
		status, ok := reb.checkStage(tsi, rargs, rebStageTraverse)
		if ok {
			debug.Assertf(status.Running, "%s: not running: [ tid=%s abrted=%t stage=%d reb-id=%d vs my %s ]",
				core.T.String(), tsi.ID(), status.Aborted, status.Stage, status.RebID, xreb.ID())

			debug.Assert(xact.RebID2S(status.RebID) == xreb.ID(), "xid: ", status.RebID, " vs ", xreb.ID())
			return true
		}
		if xreb.IsAborted() {
			return false
		}
		if err := xreb.AbortedAfter(sleep); err != nil {
			return false
		}
		curwt += sleep
	}
	tname := tsi.StringEx()
	nlog.Errorln(rargs.logHdr, "timed out waiting for", tname, "to reach", stages[rebStageTraverse], "stage")
	return false
}

// wait for the target to reach `rebStageFin` (i.e., finish traversing and sending)
// if the target that has reached rebStageWaitAck but not yet in the rebStageFin stage,
// separately check whether it is waiting for my ACKs
func (reb *Reb) waitAcksExtended(tsi *meta.Snode, rargs *rargs) (ok bool) {
	var (
		curwt      time.Duration
		status     *Status
		sleep      = rargs.config.Timeout.CplaneOperation.D()
		maxwt      = rargs.config.Rebalance.DestRetryTime.D()
		sleepRetry = cmn.KeepaliveRetryDuration(rargs.config)
		xreb       = rargs.xreb
	)
	debug.Assertf(reb.rebID() == xreb.RebID(), "%s (rebID=%d) vs %s", rargs.logHdr, reb.rebID(), xreb)

	for curwt < maxwt {
		if err := xreb.AbortedAfter(sleep); err != nil {
			nlog.Infof("%s: abort wack (%v)", rargs.logHdr, err)
			return false
		}
		if reb.stages.isInStage(tsi, rebStageFin) {
			return true // tsi stage=<fin>
		}
		// otherwise, inquire status and check the stage
		curwt += sleep
		if status, ok = reb.checkStage(tsi, rargs, rebStageFin); ok || status == nil {
			return ok
		}
		if err := xreb.AbortErr(); err != nil {
			nlog.Infof("%s: abort wack (%v)", rargs.logHdr, err)
			return false
		}
		//
		// tsi in rebStageWaitAck
		//
		var w4me bool // true: this target is waiting for ACKs from me
		for _, si := range status.Targets {
			if si.ID() == core.T.SID() {
				nlog.Infof("%s: keep wack <= %s[%s]", rargs.logHdr, tsi.StringEx(), stages[status.Stage])
				w4me = true
				break
			}
		}
		if !w4me {
			nlog.Infof("%s: %s[%s] ok (not waiting for me)", rargs.logHdr, tsi.StringEx(), stages[status.Stage])
			return true // Ok
		}
		time.Sleep(sleepRetry)
		curwt += sleepRetry
	}

	nlog.Errorf("%s: timed out waiting for %s to reach %s", rargs.logHdr, tsi.StringEx(), stages[rebStageFin])
	return false
}

// calls tsi.reb.RebStatus() and handles conditions; may abort the current xreb
// returns:
// - `Status` or nil
// - OK iff the desiredStage has been reached
func (reb *Reb) checkStage(tsi *meta.Snode, rargs *rargs, desiredStage uint32) (status *Status, ok bool) {
	var (
		sleepRetry = cmn.KeepaliveRetryDuration(rargs.config)
		query      = url.Values{apc.QparamRebStatus: []string{"true"}}
		xreb       = rargs.xreb
		tname      = tsi.StringEx()
	)
	if xreb == nil || xreb.IsAborted() {
		return nil, false
	}
	debug.Assertf(reb.rebID() == xreb.RebID(), "%s (rebID=%d) vs %s", rargs.logHdr, reb.rebID(), xreb)
	body, code, err := core.T.Health(tsi, apc.DefaultTimeout, query)
	if err != nil {
		if errAborted := xreb.AbortedAfter(sleepRetry); errAborted != nil {
			nlog.Infoln(rargs.logHdr, "abort check status", errAborted)
			return nil, false
		}
		body, code, err = core.T.Health(tsi, apc.DefaultTimeout, query) // retry once
	}
	if err != nil {
		ctx := fmt.Sprintf("health(%s) failure: %v(%d)", tname, err, code)
		err = cmn.NewErrAborted(xreb.Name(), ctx, err)
		reb.abortAll(err, rargs.xreb)
		return nil, false
	}

	status = &Status{}
	err = jsoniter.Unmarshal(body, status)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, rargs.logHdr, "reb status from "+tname, cos.BHead(body), err)
		reb.abortAll(err, rargs.xreb)
		return nil, false
	}
	//
	// enforce global RebID
	//
	otherXid := xact.RebID2S(status.RebID)
	if status.RebID > reb.rebID() {
		err := cmn.NewErrAborted(xreb.Name(), rargs.logHdr, errors.New(tname+" runs newer "+otherXid))
		reb.abortAll(err, rargs.xreb)
		return status, false
	}

	if status.RebID == 0 && !status.Running {
		nlog.Warningf("%s: %s[%s, v%d] starting up - joining", rargs.logHdr, tname, otherXid, status.RebVersion)
	}

	if xreb.IsAborted() {
		return status, false
	}
	// keep waiting
	if status.RebID < reb.rebID() {
		var what = "runs"
		if !status.Running {
			what = "transitioning(?) from"
		}
		nlog.Warningf("%s: %s[%s, v%d] %s older rebalance - keep waiting", rargs.logHdr, tname, otherXid, status.RebVersion, what)
		return status, false
	}
	// other target aborted same ID (do not call `reb.abortAll` - no need)
	if status.RebID == reb.rebID() && status.Aborted {
		err := cmn.NewErrAborted(xreb.Name(), rargs.logHdr, fmt.Errorf("status 'aborted' from %s", tname))
		xreb.Abort(err)
		reb.lazydel.stop()
		return status, false
	}
	if status.Stage >= desiredStage {
		return status, true // Ok
	}

	nlog.Infof("%s: %s[%s, v%d] not yet at the right stage %s", rargs.logHdr, tname, stages[status.Stage], status.RebVersion, stages[desiredStage])
	return status, false
}
