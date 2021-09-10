// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xreg"
	jsoniter "github.com/json-iterator/go"
)

type (
	Status struct {
		Targets     cluster.Nodes           `json:"targets"`             // targets I'm waiting for ACKs from
		SmapVersion int64                   `json:"smap_version,string"` // current Smap version (via smapOwner)
		RebVersion  int64                   `json:"reb_version,string"`  // Smap version of *this* rebalancing op
		RebID       int64                   `json:"reb_id,string"`       // rebalance ID
		StatsDelta  stats.ExtRebalanceStats `json:"stats_delta"`         // objects and sizes sent/recv-ed
		Stage       uint32                  `json:"stage"`               // the current stage - see enum above
		Aborted     bool                    `json:"aborted"`             // aborted?
		Running     bool                    `json:"running"`             // running?
		Quiescent   bool                    `json:"quiescent"`           // true when queue is empty
	}
)

////////////////////////////////////////////
// rebalance manager: node <=> node comm. //
////////////////////////////////////////////

// via GET /v1/health (cmn.Health)
func (reb *Manager) RebStatus(status *Status) {
	reb.RLock()
	var (
		targets    cluster.Nodes
		config     = cmn.GCO.Get()
		sleepRetry = cmn.KeepaliveRetryDuration(config)
		rsmap      = (*cluster.Smap)(reb.smap.Load())
		tsmap      = reb.t.Sowner().Get()
		marked     = xreg.GetRebMarked()
	)
	status.Aborted, status.Running = marked.Interrupted, marked.Xact != nil
	status.Stage = reb.stages.stage.Load()
	status.RebID = reb.rebID.Load()
	status.Quiescent = reb.isQuiescent()

	status.SmapVersion = tsmap.Version
	if rsmap != nil {
		status.RebVersion = rsmap.Version
	}
	beginStats := (*stats.ExtRebalanceStats)(reb.beginStats.Load())
	curStats := reb.getStats()
	reb.RUnlock()
	// stats
	if beginStats == nil {
		return
	}
	status.StatsDelta.RebTxCount = curStats.RebTxCount - beginStats.RebTxCount
	status.StatsDelta.RebTxSize = curStats.RebTxSize - beginStats.RebTxSize
	status.StatsDelta.RebRxCount = curStats.RebRxCount - beginStats.RebRxCount
	status.StatsDelta.RebRxSize = curStats.RebRxSize - beginStats.RebRxSize

	// wack info
	if status.Stage != rebStageWaitAck {
		return
	}
	if status.SmapVersion != status.RebVersion {
		glog.Warningf("%s: Smap version %d != %d", reb.t.Snode(), status.SmapVersion, status.RebVersion)
		return
	}

	reb.awaiting.mu.Lock()
	status.Targets, targets = reb.awaiting.targets, reb.awaiting.targets
	now := mono.NanoTime()
	if rsmap == nil || time.Duration(now-reb.awaiting.ts) < sleepRetry {
		goto ret
	}
	reb.awaiting.ts = now
	reb.awaiting.targets = reb.awaiting.targets[:0]
	for _, lomAck := range reb.lomAcks() {
		lomAck.mu.Lock()
	nack:
		for _, lom := range lomAck.q {
			tsi, err := cluster.HrwTarget(lom.Uname(), rsmap)
			if err != nil {
				continue
			}
			for _, si := range targets {
				if si.ID() == tsi.ID() {
					continue nack
				}
			}
			targets = append(targets, tsi)
			if len(targets) >= maxWackTargets { // limit reporting
				lomAck.mu.Unlock()
				goto ret
			}
		}
		lomAck.mu.Unlock()
	}
ret:
	reb.awaiting.mu.Unlock()
}

// main method
func (reb *Manager) bcast(md *rebArgs, cb syncCallback) (errCnt int) {
	var cnt atomic.Int32
	wg := cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), len(md.smap.Tmap))
	for _, tsi := range md.smap.Tmap {
		if tsi.ID() == reb.t.SID() {
			continue
		}
		wg.Add(1)
		go func(tsi *cluster.Snode) {
			if !cb(tsi, md) {
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
func (reb *Manager) pingTarget(tsi *cluster.Snode, md *rebArgs) (ok bool) {
	var (
		ver    = md.smap.Version
		sleep  = md.config.Timeout.CplaneOperation.D()
		logHdr = reb.logHdr(md)
	)
	for i := 0; i < 4; i++ {
		_, code, err := reb.t.Health(tsi, md.config.Timeout.MaxKeepalive.D(), nil)
		if err == nil {
			if i > 0 {
				glog.Infof("%s: %s is online", logHdr, tsi)
			}
			return true
		}
		if !cos.IsUnreachable(err, code) {
			glog.Errorf("%s: health(%s) returned err %v(%d) - aborting", logHdr, tsi, err, code)
			return
		}
		glog.Warningf("%s: waiting for %s, err %v(%d)", logHdr, tsi, err, code)
		time.Sleep(sleep)
		nver := reb.t.Sowner().Get().Version
		if nver > ver {
			return
		}
	}
	glog.Errorf("%s: timed-out waiting for %s", logHdr, tsi)
	return
}

// wait for target to get ready to receive objects (type syncCallback)
func (reb *Manager) rxReady(tsi *cluster.Snode, md *rebArgs) (ok bool) {
	var (
		sleep  = md.config.Timeout.CplaneOperation.D() * 2
		maxwt  = md.config.Rebalance.DestRetryTime.D() + md.config.Rebalance.DestRetryTime.D()/2
		curwt  time.Duration
		logHdr = reb.logHdr(md)
	)
	for curwt < maxwt {
		if reb.stages.isInStage(tsi, rebStageTraverse) {
			// do not request the node stage if it has sent push notification
			return true
		}
		if _, ok = reb.checkGlobStatus(tsi, rebStageTraverse, md); ok {
			return
		}
		if reb.xact().Aborted() || reb.xact().AbortedAfter(sleep) {
			glog.Infof("%s: abort rx-ready", logHdr)
			return
		}
		curwt += sleep
	}
	glog.Errorf("%s: timed out waiting for %s to reach %s state", logHdr, tsi, stages[rebStageTraverse])
	return
}

// wait for the target to reach strage = rebStageFin (i.e., finish traversing and sending)
// if the target that has reached rebStageWaitAck but not yet in the rebStageFin stage,
// separately check whether it is waiting for my ACKs
func (reb *Manager) waitFinExtended(tsi *cluster.Snode, md *rebArgs) (ok bool) {
	var (
		sleep      = md.config.Timeout.CplaneOperation.D()
		maxwt      = md.config.Rebalance.DestRetryTime.D()
		sleepRetry = cmn.KeepaliveRetryDuration(md.config)
		logHdr     = reb.logHdr(md)
		curwt      time.Duration
		status     *Status
	)
	for curwt < maxwt {
		if reb.xact().AbortedAfter(sleep) {
			glog.Infof("%s: abort wack", logHdr)
			return
		}
		if reb.stages.isInStage(tsi, rebStageFin) {
			// do not request the node stage if it has sent push notification
			return true
		}
		curwt += sleep
		if status, ok = reb.checkGlobStatus(tsi, rebStageFin, md); ok {
			return
		}
		if reb.xact().Aborted() {
			glog.Infof("%s: abort wack", logHdr)
			return
		}
		//
		// tsi in rebStageWaitAck
		//
		var w4me bool // true: this target is waiting for ACKs from me
		for _, si := range status.Targets {
			if si.ID() == reb.t.SID() {
				glog.Infof("%s: keep wack <= %s[%s]", logHdr, tsi, stages[status.Stage])
				w4me = true
				break
			}
		}
		if !w4me {
			glog.Infof("%s: %s[%s] ok (not waiting for me)", logHdr, tsi, stages[status.Stage])
			ok = true
			return
		}
		time.Sleep(sleepRetry)
		curwt += sleepRetry
	}
	glog.Errorf("%s: timed out waiting for %s to reach %s", logHdr, tsi, stages[rebStageFin])
	return
}

// calls tsi.reb.RebStatus() and handles conditions; may abort the current xreb
// returns OK if the desiredStage has been reached
func (reb *Manager) checkGlobStatus(tsi *cluster.Snode, desiredStage uint32, md *rebArgs) (status *Status, ok bool) {
	var (
		sleepRetry = cmn.KeepaliveRetryDuration(md.config)
		logHdr     = reb.logHdr(md)
		query      = url.Values{cmn.URLParamRebStatus: []string{"true"}}
	)
	body, code, err := reb.t.Health(tsi, cmn.DefaultTimeout, query)
	if err != nil {
		if reb.xact().AbortedAfter(sleepRetry) {
			glog.Infof("%s: abort", logHdr)
			return
		}
		body, code, err = reb.t.Health(tsi, cmn.DefaultTimeout, query) // retry once
	}
	if err != nil {
		glog.Errorf("%s: health(%s) returned err %v(%d) - aborting", logHdr, tsi, err, code)
		reb.abortRebalance()
		return
	}
	status = &Status{}
	err = jsoniter.Unmarshal(body, status)
	if err != nil {
		glog.Errorf("%s: unexpected: failed to unmarshal (%s: %v)", logHdr, tsi, err)
		reb.abortRebalance()
		return
	}
	// enforce same global transaction ID
	if status.RebID > reb.rebID.Load() {
		glog.Errorf("%s: %s runs newer (g%d) transaction - aborting...", logHdr, tsi, status.RebID)
		reb.abortRebalance()
		return
	}
	// let the target to catch-up
	if status.RebID < reb.RebID() {
		glog.Warningf("%s: %s runs older (g%d) transaction - keep waiting...", logHdr, tsi, status.RebID)
		return
	}
	// Remote target has aborted its running rebalance with the same ID as local,
	// but resilver is still running. Abort local xaction with `Abort`,
	// do not use `abortRebalance` - no need to broadcast.
	if status.RebID == reb.RebID() && status.Aborted {
		glog.Warningf("%s aborted %s[g%d] - aborting as well...", tsi, cmn.ActRebalance, status.RebID)
		reb.xact().Abort(nil)
		return
	}

	if status.Stage >= desiredStage {
		ok = true
		return
	}
	glog.Infof("%s: %s[%s] not yet at the right stage %s", logHdr, tsi, stages[status.Stage], stages[desiredStage])
	return
}
