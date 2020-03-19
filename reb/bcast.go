// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

//
// rebalance: cluster-wide synchronization at certain stages
//

type (
	Status struct {
		Tmap        cluster.NodeMap         `json:"tmap"`                // targets I'm waiting for ACKs from
		SmapVersion int64                   `json:"smap_version,string"` // current Smap version (via smapOwner)
		RebVersion  int64                   `json:"reb_version,string"`  // Smap version of *this* rebalancing op
		RebID       int64                   `json:"reb_id,string"`       // rebalance ID
		StatsDelta  stats.ExtRebalanceStats `json:"stats_delta"`         // objects and sizes transmitted/received by this reb oper
		BatchCurr   int                     `json:"batch_curr"`          // the current batch ID processing by EC rebalance
		BatchLast   int                     `json:"batch_last"`          // the last batch ID to be processed by EC rebalance
		Stage       uint32                  `json:"stage"`               // the current stage - see enum above
		Aborted     bool                    `json:"aborted"`             // aborted?
		Running     bool                    `json:"running"`             // running?
		Quiescent   bool                    `json:"quiescent"`           // transport queue is empty
	}
)

// via GET /v1/health (cmn.Health)
func (reb *Manager) RebStatus(status *Status) {
	var (
		now        time.Time
		targets    cluster.NodeMap
		config     = cmn.GCO.Get()
		sleepRetry = cmn.KeepaliveRetryDuration(config)
		rsmap      = (*cluster.Smap)(reb.smap.Load())
		tsmap      = reb.t.GetSowner().Get()
	)
	status.Aborted, status.Running = IsRebalancing(cmn.ActRebalance)
	status.Stage = reb.stages.stage.Load()
	status.RebID = reb.rebID.Load()
	status.Quiescent = reb.isQuiescent()
	if status.Stage > rebStageECRepair && status.Stage < rebStageECCleanup {
		status.BatchCurr = int(reb.stages.currBatch.Load())
		status.BatchLast = int(reb.stages.lastBatch.Load())
	} else {
		status.BatchCurr = 0
		status.BatchLast = 0
	}

	status.SmapVersion = tsmap.Version
	if rsmap != nil {
		status.RebVersion = rsmap.Version
	}
	// stats
	beginStats := (*stats.ExtRebalanceStats)(reb.beginStats.Load())
	if beginStats == nil {
		return
	}
	curStats := reb.getStats()
	status.StatsDelta.TxRebCount = curStats.TxRebCount - beginStats.TxRebCount
	status.StatsDelta.RxRebCount = curStats.RxRebCount - beginStats.RxRebCount
	status.StatsDelta.TxRebSize = curStats.TxRebSize - beginStats.TxRebSize
	status.StatsDelta.RxRebSize = curStats.RxRebSize - beginStats.RxRebSize

	// wack info
	if status.Stage != rebStageWaitAck {
		return
	}
	if status.SmapVersion != status.RebVersion {
		glog.Warningf("%s: Smap version %d != %d", reb.t.Snode(), status.SmapVersion, status.RebVersion)
		return
	}

	reb.awaiting.mu.Lock()
	status.Tmap, targets = reb.awaiting.targets, reb.awaiting.targets
	now = time.Now()
	if now.Sub(reb.awaiting.ts) < sleepRetry {
		reb.awaiting.mu.Unlock()
		return
	}
	reb.awaiting.ts = now
	for tid := range reb.awaiting.targets {
		delete(reb.awaiting.targets, tid)
	}
	max := rsmap.CountTargets() - 1
	for _, lomAck := range reb.lomAcks() {
		lomAck.mu.Lock()
		for _, lom := range lomAck.q {
			tsi, err := cluster.HrwTarget(lom.Uname(), rsmap)
			if err != nil {
				continue
			}
			targets[tsi.ID()] = tsi
			if len(targets) >= max {
				lomAck.mu.Unlock()
				goto ret
			}
		}
		lomAck.mu.Unlock()
	}
ret:
	reb.awaiting.mu.Unlock()
	status.Stage = reb.stages.stage.Load()
}

// returns the number of targets that have not reached `stage` yet. It is
// assumed that this target checks other nodes only after it has reached
// the `stage`, that is why it is skipped inside the loop
func (reb *Manager) nodesNotInStage(md *rebArgs, stage uint32) int {
	count := 0
	reb.stages.mtx.Lock()
	for _, si := range md.smap.Tmap {
		if si.ID() == reb.t.Snode().ID() {
			continue
		}
		if !reb.stages.isInStageBatchUnlocked(si, stage, 0) {
			count++
			continue
		}
		_, exists := reb.ec.nodeData(si.ID())
		if stage == rebStageECDetect && !exists {
			count++
			continue
		}
	}
	reb.stages.mtx.Unlock()
	return count
}

// main method
func (reb *Manager) bcast(md *rebArgs, cb syncCallback) (errCnt int) {
	var (
		wg  = &sync.WaitGroup{}
		cnt atomic.Int32
	)
	for _, tsi := range md.smap.Tmap {
		if tsi.ID() == reb.t.Snode().ID() {
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
		sleep  = md.config.Timeout.CplaneOperation
		logHdr = reb.logHdr(md)
	)
	for i := 0; i < 3; i++ {
		_, err := reb.t.Health(tsi, false, md.config.Timeout.MaxKeepalive)
		if err == nil {
			if i > 0 {
				glog.Infof("%s: %s is online", logHdr, tsi)
			}
			return true
		}
		glog.Warningf("%s: waiting for %s, err %v", logHdr, tsi, err)
		time.Sleep(sleep)
		nver := reb.t.GetSowner().Get().Version
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
		sleep  = md.config.Timeout.CplaneOperation * 2
		maxwt  = md.config.Rebalance.DestRetryTime + md.config.Rebalance.DestRetryTime/2
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
		sleep      = md.config.Timeout.CplaneOperation
		maxwt      = md.config.Rebalance.DestRetryTime
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
		if status.Stage <= rebStageECNamespace {
			glog.Infof("%s: keep waiting for %s[%s]", logHdr, tsi, stages[status.Stage])
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			if status.Stage != rebStageInactive {
				curwt = 0 // keep waiting forever or until tsi finishes traversing&transmitting
			}
			continue
		}
		//
		// tsi in rebStageWaitAck
		//
		var w4me bool // true: this target is waiting for ACKs from me
		for tid := range status.Tmap {
			if tid == reb.t.Snode().ID() {
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
	)

	body, err := reb.t.Health(tsi, true, cmn.DefaultTimeout)
	if err != nil {
		if reb.xact().AbortedAfter(sleepRetry) {
			glog.Infof("%s: abort", logHdr)
			return
		}
		body, err = reb.t.Health(tsi, true, cmn.DefaultTimeout) // retry once
	}
	if err != nil {
		glog.Errorf("%s: failed to call %s, err: %v", logHdr, tsi, err)
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
		glog.Warningf("%s has aborted rebalance (g%d) - aborting...", tsi.ID(), status.RebID)
		reb.xact().Abort()
		return
	}

	if status.Stage >= desiredStage {
		ok = true
		return
	}
	glog.Infof("%s: %s[%s] not yet at the right stage %s", logHdr, tsi, stages[status.Stage], stages[desiredStage])
	return
}

// a generic wait loop for a stage when the target should just wait without extra actions
func (reb *Manager) waitStage(si *cluster.Snode, md *rebArgs, stage uint32) bool {
	sleep := md.config.Timeout.CplaneOperation * 2
	maxwt := md.config.Rebalance.DestRetryTime + md.config.Rebalance.DestRetryTime/2
	curwt := time.Duration(0)
	for curwt < maxwt {
		if reb.stages.isInStage(si, stage) {
			return true
		}

		status, ok := reb.checkGlobStatus(si, stage, md)
		if ok && status.Stage >= stage {
			return true
		}

		if reb.xact().Aborted() || reb.xact().AbortedAfter(sleep) {
			glog.Infof("%s: abort %s", reb.logHdr(md), stages[stage])
			return false
		}

		curwt += sleep
		time.Sleep(sleep)
	}
	return false
}

// Wait until all nodes finishes namespace building (just wait)
func (reb *Manager) waitNamespace(si *cluster.Snode, md *rebArgs) bool {
	return reb.waitStage(si, md, rebStageECNamespace)
}

// Wait until all nodes finishes moving local slices/object to correct mpath
func (reb *Manager) waitECResilver(si *cluster.Snode, md *rebArgs) bool {
	return reb.waitStage(si, md, rebStageECRepair)
}

// Wait until all nodes clean up everything
func (reb *Manager) waitECCleanup(si *cluster.Snode, md *rebArgs) bool {
	return reb.waitStage(si, md, rebStageECCleanup)
}

// Wait until all nodes finishes exchanging slice lists (do pull request if
// the remote target's data is still missing).
// Returns `true` if a target has sent its namespace or the xaction
// has been aborted, indicating that no need to do extra pull requests.
func (reb *Manager) waitECData(si *cluster.Snode, md *rebArgs) bool {
	sleep := md.config.Timeout.CplaneOperation * 2
	locStage := uint32(rebStageECDetect)
	maxwt := md.config.Rebalance.DestRetryTime + md.config.Rebalance.DestRetryTime/2
	curwt := time.Duration(0)

	for curwt < maxwt {
		if reb.xact().Aborted() {
			return true
		}
		_, exists := reb.ec.nodeData(si.ID())
		if reb.stages.isInStage(si, locStage) && exists {
			return true
		}

		outjson, status, err := reb.t.RebalanceNamespace(si)
		if err != nil {
			// something bad happened, aborting
			glog.Errorf("[g%d]: failed to call %s, err: %v", reb.rebID.Load(), si, err)
			reb.abortRebalance()
			return false
		}
		if status == http.StatusAccepted {
			// the node is not ready, wait for more
			curwt += sleep
			time.Sleep(sleep)
			continue
		}
		slices := make([]*rebCT, 0)
		if status != http.StatusNoContent {
			// TODO: send the number of items in push request and preallocate `slices`?
			if err := jsoniter.Unmarshal(outjson, &slices); err != nil {
				// not a severe error: next wait loop re-requests the data
				glog.Warningf("Invalid JSON received from %s: %v", si, err)
				curwt += sleep
				time.Sleep(sleep)
				continue
			}
		}
		reb.ec.setNodeData(si.ID(), slices)
		reb.stages.setStage(si.ID(), rebStageECDetect, 0)
		return true
	}
	return false
}

// The loop waits until the minimal number of targets have sent push notifications.
// First stages are very short and fast targets may send their notifications too
// quickly. So, for the first notifications there is no wait loop - just a single check.
// Returns `true` if all targets have sent push notifications or the xaction
// has been aborted, indicating that no need to do extra pull requests.
func (reb *Manager) waitForPushReqs(md *rebArgs, stage uint32, timeout ...time.Duration) bool {
	const defaultWaitTime = time.Minute
	maxMissing := len(md.smap.Tmap) / 2 // TODO: is it OK to wait for half of them?
	curWait := time.Duration(0)
	sleep := md.config.Timeout.CplaneOperation * 2
	maxWait := defaultWaitTime
	if len(timeout) != 0 {
		maxWait = timeout[0]
	}
	for curWait < maxWait {
		if reb.xact().Aborted() {
			return true
		}
		cnt := reb.nodesNotInStage(md, stage)
		if cnt < maxMissing || stage <= rebStageECNamespace {
			return cnt == 0
		}
		time.Sleep(sleep)
		curWait += sleep
	}
	return false
}

// Returns true if all targets in the cluster are quiescent: all
// transport queues are empty
func (reb *Manager) nodesQuiescent(md *rebArgs) bool {
	quiescent := true
	locStage := reb.stages.stage.Load()
	for _, si := range md.smap.Tmap {
		if si.ID() == reb.t.Snode().ID() && !reb.isQuiescent() {
			quiescent = false
			break
		}
		status, ok := reb.checkGlobStatus(si, locStage, md)
		if !ok || !status.Quiescent {
			quiescent = false
			break
		}
	}
	return quiescent
}
