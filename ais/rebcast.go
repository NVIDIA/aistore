// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

//
// global rebalance: cluster-wide synchronization at certain stages
//

type (
	rebStatus struct {
		Tmap        cluster.NodeMap         `json:"tmap"`                // targets I'm waiting for ACKs from
		SmapVersion int64                   `json:"smap_version,string"` // current Smap version (via smapowner)
		RebVersion  int64                   `json:"reb_version,string"`  // Smap version of *this* rebalancing op
		GlobRebID   int64                   `json:"glob_reb_id,string"`  // global rebalance ID
		StatsDelta  stats.ExtRebalanceStats `json:"stats_delta"`         // objects and sizes transmitted/received by this reb oper
		Stage       uint32                  `json:"stage"`               // the current stage - see enum above
		Aborted     bool                    `json:"aborted"`             // aborted?
		Running     bool                    `json:"running"`             // running?
	}

	// push notification struct - a target sends it when it enters `stage`
	pushReq struct {
		DaemonID string `json:"sid"`             // sender's ID
		RebID    int64  `json:"rebid,string"`    // sender's global rebalance ID
		Stage    uint32 `json:"stage"`           // stage the sender has just reached
		Batch    int    `json:"batch"`           // batch when restoring
		Extra    []byte `json:"extra,omitempty"` // metadata
	}
)

// via GET /v1/health (cmn.Health)
func (reb *rebManager) getGlobStatus(status *rebStatus) {
	var (
		now        time.Time
		tmap       cluster.NodeMap
		config     = cmn.GCO.Get()
		sleepRetry = keepaliveRetryDuration(config)
		rsmap      = (*smapX)(reb.smap.Load())
		tsmap      = reb.t.smapowner.get()
	)
	status.Aborted, status.Running = xaction.Registry.IsRebalancing(cmn.ActGlobalReb)
	status.Stage = reb.stage.Load()
	status.GlobRebID = reb.globRebID.Load()
	status.SmapVersion = tsmap.version()
	if rsmap != nil {
		status.RebVersion = rsmap.version()
	}
	// stats
	beginStats := (*stats.ExtRebalanceStats)(reb.beginStats.Load())
	if beginStats == nil {
		return
	}
	curntStats := reb.getStats()
	status.StatsDelta.TxRebCount = curntStats.TxRebCount - beginStats.TxRebCount
	status.StatsDelta.RxRebCount = curntStats.RxRebCount - beginStats.RxRebCount
	status.StatsDelta.TxRebSize = curntStats.TxRebSize - beginStats.TxRebSize
	status.StatsDelta.RxRebSize = curntStats.RxRebSize - beginStats.RxRebSize

	// wack info
	if status.Stage != rebStageWaitAck {
		return
	}
	if status.SmapVersion != status.RebVersion {
		glog.Warningf("%s: Smap version %d != %d", reb.t.si.Name(), status.SmapVersion, status.RebVersion)
		return
	}

	reb.tcache.mu.Lock()
	status.Tmap, tmap = reb.tcache.tmap, reb.tcache.tmap
	now = time.Now()
	if now.Sub(reb.tcache.ts) < sleepRetry {
		reb.tcache.mu.Unlock()
		return
	}
	reb.tcache.ts = now
	for tid := range reb.tcache.tmap {
		delete(reb.tcache.tmap, tid)
	}
	max := rsmap.CountTargets() - 1
	for _, lomack := range reb.lomAcks() {
		lomack.mu.Lock()
		for _, lom := range lomack.q {
			tsi, err := cluster.HrwTarget(lom.Bck(), lom.Objname, &rsmap.Smap)
			if err != nil {
				continue
			}
			tmap[tsi.DaemonID] = tsi
			if len(tmap) >= max {
				lomack.mu.Unlock()
				goto ret
			}
		}
		lomack.mu.Unlock()
	}
ret:
	reb.tcache.mu.Unlock()
	status.Stage = reb.stage.Load()
}

// returns the number of targets that have not reached `stage` yet. It is
// assumed that this target checks other nodes only after it has reached
// the `stage`, that is why it is skipped inside the loop
func (reb *rebManager) nodesNotInStage(stage uint32) int {
	smap := reb.t.smapowner.get()
	count := 0
	reb.stageMtx.Lock()
	for _, si := range smap.Tmap {
		if si.DaemonID == reb.t.si.DaemonID {
			continue
		}
		if _, ok := reb.nodeStages[si.DaemonID]; !ok {
			count++
			continue
		}
		_, exists := reb.ecReb.nodeData(si.DaemonID)
		if stage == rebStageECDetect && !exists {
			count++
			continue
		}
	}
	reb.stageMtx.Unlock()
	return count
}

// main method
func (reb *rebManager) bcast(md *globalRebArgs, cb rebSyncCallback) (errCnt int) {
	var (
		wg  = &sync.WaitGroup{}
		cnt atomic.Int32
	)
	for _, tsi := range md.smap.Tmap {
		if tsi.DaemonID == reb.t.si.DaemonID {
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

// pingTarget checks if target is running (type rebSyncCallback)
// TODO: reuse keepalive
func (reb *rebManager) pingTarget(tsi *cluster.Snode, md *globalRebArgs) (ok bool) {
	var (
		ver   = md.smap.Version
		sleep = md.config.Timeout.CplaneOperation
		args  = callArgs{
			si: tsi,
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Base:   tsi.IntraControlNet.DirectURL,
				Path:   cmn.URLPath(cmn.Version, cmn.Health),
			},
			timeout: md.config.Timeout.MaxKeepalive,
		}
		loghdr = reb.loghdr(reb.globRebID.Load(), md.smap)
	)
	for i := 0; i < 3; i++ {
		res := reb.t.call(args)
		if res.err == nil {
			if i > 0 {
				glog.Infof("%s: %s is online", loghdr, tsi.Name())
			}
			return true
		}
		glog.Warningf("%s: waiting for %s, err %v", loghdr, tsi.Name(), res.err)
		time.Sleep(sleep)
		nver := reb.t.smapowner.get().version()
		if nver > ver {
			return
		}
	}
	glog.Errorf("%s: timed-out waiting for %s", loghdr, tsi.Name())
	return
}

// wait for target to get ready to receive objects (type rebSyncCallback)
func (reb *rebManager) rxReady(tsi *cluster.Snode, md *globalRebArgs) (ok bool) {
	var (
		ver    = md.smap.Version
		sleep  = md.config.Timeout.CplaneOperation * 2
		maxwt  = md.config.Rebalance.DestRetryTime + md.config.Rebalance.DestRetryTime/2
		curwt  time.Duration
		loghdr = reb.loghdr(reb.globRebID.Load(), md.smap)
	)
	for curwt < maxwt {
		if reb.isNodeInStage(tsi, rebStageTraverse) {
			// do not request the node stage if it has sent push notification
			return true
		}
		if _, ok = reb.checkGlobStatus(tsi, ver, rebStageTraverse, md); ok {
			return
		}
		if md.xreb.Aborted() || md.xreb.AbortedAfter(sleep) {
			glog.Infof("%s: abrt rx-ready", loghdr)
			return
		}
		curwt += sleep
	}
	glog.Errorf("%s: timed out waiting for %s to reach %s state", loghdr, tsi.Name(), rebStage[rebStageTraverse])
	return
}

// wait for the target to reach strage = rebStageFin (i.e., finish traversing and sending)
// if the target that has reached rebStageWaitAck but not yet in the rebStageFin stage,
// separately check whether it is waiting for my ACKs
func (reb *rebManager) waitFinExtended(tsi *cluster.Snode, md *globalRebArgs) (ok bool) {
	var (
		ver        = md.smap.Version
		sleep      = md.config.Timeout.CplaneOperation
		maxwt      = md.config.Rebalance.DestRetryTime
		sleepRetry = keepaliveRetryDuration(md.config)
		loghdr     = reb.loghdr(reb.globRebID.Load(), md.smap)
		curwt      time.Duration
		status     *rebStatus
	)
	for curwt < maxwt {
		if md.xreb.AbortedAfter(sleep) {
			glog.Infof("%s: abrt wack", loghdr)
			return
		}
		if reb.isNodeInStage(tsi, rebStageFin) {
			// do not request the node stage if it has sent push notification
			return true
		}
		curwt += sleep
		if status, ok = reb.checkGlobStatus(tsi, ver, rebStageFin, md); ok {
			return
		}
		if md.xreb.Aborted() {
			glog.Infof("%s: abrt wack", loghdr)
			return
		}
		if status.Stage <= rebStageECNameSpace {
			glog.Infof("%s: keep waiting for %s[%s]", loghdr, tsi.Name(), rebStage[status.Stage])
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
			if tid == reb.t.si.DaemonID {
				glog.Infof("%s: keep wack <= %s[%s]", loghdr, tsi.Name(), rebStage[status.Stage])
				w4me = true
				break
			}
		}
		if !w4me {
			glog.Infof("%s: %s[%s] ok (not waiting for me)", loghdr, tsi.Name(), rebStage[status.Stage])
			ok = true
			return
		}
		time.Sleep(sleepRetry)
		curwt += sleepRetry
	}
	glog.Errorf("%s: timed out waiting for %s to reach %s", loghdr, tsi.Name(), rebStage[rebStageFin])
	return
}

// calls tsi.reb.getGlobStatus() and handles conditions; may abort the current xreb
// returns OK if the desiredStage has been reached
func (reb *rebManager) checkGlobStatus(tsi *cluster.Snode, ver int64,
	desiredStage uint32, md *globalRebArgs) (status *rebStatus, ok bool) {
	var (
		query      = url.Values{}
		sleepRetry = keepaliveRetryDuration(md.config)
		loghdr     = reb.loghdr(reb.globRebID.Load(), md.smap)
	)
	query.Add(cmn.URLParamRebStatus, "true")
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Base:   tsi.URL(cmn.NetworkIntraControl),
			Path:   cmn.URLPath(cmn.Version, cmn.Health),
			Query:  query,
		},
		timeout: defaultTimeout,
	}
	res := reb.t.call(args)
	if res.err != nil {
		if md.xreb.AbortedAfter(sleepRetry) {
			glog.Infof("%s: abrt", loghdr)
			return
		}
		res = reb.t.call(args) // retry once
	}
	if res.err != nil {
		glog.Errorf("%s: failed to call %s, err: %v", loghdr, tsi.Name(), res.err)
		md.xreb.Abort()
		return
	}
	status = &rebStatus{}
	err := jsoniter.Unmarshal(res.outjson, status)
	if err != nil {
		glog.Errorf("%s: unexpected: failed to unmarshal %s response, err: %v", loghdr, tsi.Name(), err)
		md.xreb.Abort()
		return
	}
	// enforce Smap consistency across this xreb
	tver, rver := status.SmapVersion, status.RebVersion
	if tver > ver || rver > ver {
		glog.Errorf("%s: %s has newer Smap (v%d, v%d) - aborting...", loghdr, tsi.Name(), tver, rver)
		md.xreb.Abort()
		return
	}
	// enforce same global transaction ID
	if status.GlobRebID > reb.globRebID.Load() {
		glog.Errorf("%s: %s runs newer (g%d) transaction - aborting...", loghdr, tsi.Name(), status.GlobRebID)
		md.xreb.Abort()
		return
	}
	// let the target to catch-up
	if tver < ver || rver < ver {
		glog.Warningf("%s: %s has older Smap (v%d, v%d) - keep waiting...", loghdr, tsi.Name(), tver, rver)
		return
	}
	if status.GlobRebID < reb.globRebID.Load() {
		glog.Warningf("%s: %s runs older (g%d) transaction - keep waiting...", loghdr, tsi.Name(), status.GlobRebID)
		return
	}
	if status.Stage >= desiredStage {
		ok = true
		return
	}
	glog.Infof("%s: %s[%s] not yet at the right stage", loghdr, tsi.Name(), rebStage[status.Stage])
	return
}

// a generic wait loop for a stage when the target should just wait without extra actions
func (reb *rebManager) waitStage(si *cluster.Snode, md *globalRebArgs, stage uint32) bool {
	sleep := md.config.Timeout.CplaneOperation * 2
	maxwt := md.config.Rebalance.DestRetryTime + md.config.Rebalance.DestRetryTime/2
	curwt := time.Duration(0)
	for curwt < maxwt {
		if reb.isNodeInStage(si, stage) {
			return true
		}

		status, ok := reb.checkGlobStatus(si, md.smap.Version, stage, md)
		if ok && status.Stage >= stage {
			return true
		}

		if md.xreb.Aborted() || md.xreb.AbortedAfter(sleep) {
			glog.Infof("g%d: abrt %s", reb.globRebID.Load(), rebStage[stage])
			return false
		}

		curwt += sleep
		time.Sleep(sleep)
	}
	return false
}

// Wait until all nodes finishes namespace building (just wait)
func (reb *rebManager) waitNamespace(si *cluster.Snode, md *globalRebArgs) bool {
	return reb.waitStage(si, md, rebStageECNameSpace)
}

// Wait until all nodes finishes moving local slices/object to correct mpath
func (reb *rebManager) waitECLocalReb(si *cluster.Snode, md *globalRebArgs) bool {
	return reb.waitStage(si, md, rebStageECGlobRepair)
}

// Wait until all nodes clean up everything
func (reb *rebManager) waitECCleanup(si *cluster.Snode, md *globalRebArgs) bool {
	return reb.waitStage(si, md, rebStageECCleanup)
}

// Wait until all nodes finishes exchanging slice lists (do pull request if
// the remote target's data is still missing)
func (reb *rebManager) waitECData(si *cluster.Snode, md *globalRebArgs) bool {
	sleep := md.config.Timeout.CplaneOperation * 2
	locStage := uint32(rebStageECDetect)
	maxwt := md.config.Rebalance.DestRetryTime + md.config.Rebalance.DestRetryTime/2
	curwt := time.Duration(0)

	// pull the data
	query := url.Values{}
	header := make(http.Header)
	header.Set(cmn.HeaderCallerID, reb.t.si.DaemonID)
	query.Add(cmn.URLParamRebData, "true")
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Header: header,
			Base:   si.URL(cmn.NetworkIntraData),
			Path:   cmn.URLPath(cmn.Version, cmn.Rebalance),
			Query:  query,
		},
		timeout: defaultTimeout,
	}
	for curwt < maxwt {
		_, exists := reb.ecReb.nodeData(si.DaemonID)
		if reb.isNodeInStage(si, locStage) && exists {
			return true
		}

		res := reb.t.call(args)
		if res.err == nil {
			slices := make([]*ecRebSlice, 0)
			if res.status != http.StatusNoContent {
				// TODO: send the number of items in push request and preallocate `slices`
				if err := jsoniter.Unmarshal(res.outjson, &slices); err != nil {
					// not a severe error: return false so next wait loop re-requests the data
					glog.Warningf("Invalid JSON received from %s: %v", si.Name(), err)
					curwt += sleep
					time.Sleep(sleep)
					continue
				}
			}
			reb.ecReb.setNodeData(si.DaemonID, slices)
			reb.setStage(si.DaemonID, rebStageECDetect)
			return true
		}

		if res.err != nil {
			// something bad happened, aborting
			glog.Errorf("[g%d]: failed to call %s, err: %v", reb.globRebID.Load(), si.Name(), res.err)
			md.xreb.Abort()
			return false
		}

		curwt += sleep
		time.Sleep(sleep)
	}
	return false
}

// The loop waits until the minimal number of targets have sent push notifications.
// If this targets has received notifications from all other targets the function
// returns true, indicating that there is no need to do extra pull requests.
// First stages are very short and fast targets may send their notifications too
// quickly. So, for the first notifications there is no wait loop - just a single check
func (reb *rebManager) waitForPushReqs(md *globalRebArgs, stage uint32, timeout ...time.Duration) bool {
	const defaultWaitTime = time.Minute
	maxMissing := len(md.smap.Tmap) / 2 // TODO: is it OK to wait for half of them?
	curWait := time.Duration(0)
	sleep := md.config.Timeout.CplaneOperation * 2
	maxWait := defaultWaitTime
	if len(timeout) != 0 {
		maxWait = timeout[0]
	}
	for curWait < maxWait {
		if md.xreb.Aborted() {
			return false
		}
		cnt := reb.nodesNotInStage(stage)
		if cnt < maxMissing || stage <= rebStageECNameSpace {
			return cnt == 0
		}
		time.Sleep(sleep)
		curWait += sleep
	}
	return false
}
