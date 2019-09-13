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
	jsoniter "github.com/json-iterator/go"
)

//
// global rebalance: cluster-wide synchronization at certain stages
//

type rebStatus struct {
	Tmap        cluster.NodeMap         `json:"tmap"`         // targets I'm waiting for ACKs from
	SmapVersion int64                   `json:"smap_version"` // current Smap version (via smapowner)
	RebVersion  int64                   `json:"reb_version"`  // Smap version of *this* rebalancing operation (m.b. invariant)
	GlobRebID   int64                   `json:"glob_reb_id"`  // global rebalance ID
	StatsDelta  stats.ExtRebalanceStats `json:"stats_delta"`  // objects and sizes transmitted/received by this reb oper
	Stage       uint32                  `json:"stage"`        // the current stage - see enum above
	Aborted     bool                    `json:"aborted"`      // aborted?
	Running     bool                    `json:"running"`      // running?
}

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
	status.Aborted, status.Running = reb.t.xactions.isRebalancing(cmn.ActGlobalReb)
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
			tsi, err := cluster.HrwTarget(lom.Bucket(), lom.Objname, &rsmap.Smap)
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

// main method
func (reb *rebManager) bcast(smap *smapX, globRebID int64, config *cmn.Config, cb rebSyncCallback, xreb *xactGlobalReb) (errCnt int) {
	var (
		ver = smap.version()
		wg  = &sync.WaitGroup{}
		cnt atomic.Int32
	)
	for _, tsi := range smap.Tmap {
		if tsi.DaemonID == reb.t.si.DaemonID {
			continue
		}
		wg.Add(1)
		go func(tsi *cluster.Snode) {
			if !cb(tsi, smap, globRebID, config, ver, xreb) {
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
func (reb *rebManager) pingTarget(tsi *cluster.Snode, smap *smapX, globRebID int64, config *cmn.Config, ver int64, _ *xactGlobalReb) (ok bool) {
	var (
		sleep = config.Timeout.CplaneOperation
		args  = callArgs{
			si: tsi,
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Base:   tsi.IntraControlNet.DirectURL,
				Path:   cmn.URLPath(cmn.Version, cmn.Health),
			},
			timeout: config.Timeout.MaxKeepalive,
		}
	)
	for i := 0; i < 3; i++ {
		res := reb.t.call(args)
		if res.err == nil {
			if i > 0 {
				glog.Infof("%s: %s is online", reb.loghdr(globRebID, smap), tsi.Name())
			}
			return true
		}
		glog.Warningf("%s: waiting for %s, err %v", reb.loghdr(globRebID, smap), tsi.Name(), res.err)
		time.Sleep(sleep)
		nver := reb.t.smapowner.get().version()
		if nver > ver {
			return
		}
	}
	glog.Errorf("%s: timed-out waiting for %s", reb.loghdr(globRebID, smap), tsi.Name())
	return
}

// wait for target to get ready to receive objects (type rebSyncCallback)
func (reb *rebManager) rxReady(tsi *cluster.Snode, smap *smapX, globRebID int64, config *cmn.Config, ver int64, xreb *xactGlobalReb) (ok bool) {
	var (
		sleep = config.Timeout.CplaneOperation * 2
		maxwt = config.Rebalance.DestRetryTime + config.Rebalance.DestRetryTime/2
		curwt time.Duration
	)
	for curwt < maxwt {
		if _, ok = reb.checkGlobStatus(tsi, smap, globRebID, config, ver, xreb, rebStageTraverse); ok {
			return
		}
		if xreb.Aborted() || xreb.abortedAfter(sleep) {
			glog.Infof("%s: abrt rx-ready", reb.loghdr(globRebID, smap))
			return
		}
		curwt += sleep
	}
	glog.Errorf("%s: timed out waiting for %s to reach %s state", reb.loghdr(globRebID, smap), tsi.Name(), rebStage[rebStageTraverse])
	return
}

// wait for the target to reach strage = rebStageFin (i.e., finish traversing and sending)
// if the target that has reached rebStageWaitAck but not yet in the rebStageFin stage,
// separately check whether it is waiting for my ACKs
func (reb *rebManager) waitFinExtended(tsi *cluster.Snode, smap *smapX, globRebID int64, config *cmn.Config, ver int64, xreb *xactGlobalReb) (ok bool) {
	var (
		sleep      = config.Timeout.CplaneOperation
		maxwt      = config.Rebalance.DestRetryTime
		sleepRetry = keepaliveRetryDuration(config)
		curwt      time.Duration
		status     *rebStatus
	)
	for curwt < maxwt {
		if xreb.abortedAfter(sleep) {
			glog.Infof("%s: abrt wack", reb.loghdr(globRebID, smap))
			return
		}
		curwt += sleep
		if status, ok = reb.checkGlobStatus(tsi, smap, globRebID, config, ver, xreb, rebStageFin); ok {
			return
		}
		if xreb.Aborted() {
			glog.Infof("%s: abrt wack", reb.loghdr(globRebID, smap))
			return
		}
		if status.Stage <= rebStageTraverse {
			glog.Infof("%s: keep waiting for %s[%s]", reb.loghdr(globRebID, smap), tsi.Name(), rebStage[status.Stage])
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
				glog.Infof("%s: keep wack <= %s[%s]", reb.loghdr(globRebID, smap), tsi.Name(), rebStage[status.Stage])
				w4me = true
				break
			}
		}
		if !w4me {
			glog.Infof("%s: %s[%s] ok (not waiting for me)", reb.loghdr(globRebID, smap), tsi.Name(), rebStage[status.Stage])
			ok = true
			return
		}
		time.Sleep(sleepRetry)
		curwt += sleepRetry
	}
	glog.Errorf("%s: timed out waiting for %s to reach %s", reb.loghdr(globRebID, smap), tsi.Name(), rebStage[rebStageFin])
	return
}

// calls tsi.reb.getGlobStatus() and handles conditions; may abort the current xreb
// returns OK if the desiredStage has been reached
func (reb *rebManager) checkGlobStatus(tsi *cluster.Snode, smap *smapX, globRebID int64, config *cmn.Config, ver int64,
	xreb *xactGlobalReb, desiredStage uint32) (status *rebStatus, ok bool) {
	var (
		query      = url.Values{}
		sleepRetry = keepaliveRetryDuration(config)
		loghdr     = reb.loghdr(globRebID, smap)
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
		if xreb.abortedAfter(sleepRetry) {
			glog.Infof("%s: abrt", loghdr)
			return
		}
		res = reb.t.call(args) // retry once
	}
	if res.err != nil {
		glog.Errorf("%s: failed to call %s, err: %v", loghdr, tsi.Name(), res.err)
		xreb.Abort()
		return
	}
	status = &rebStatus{}
	err := jsoniter.Unmarshal(res.outjson, status)
	if err != nil {
		glog.Errorf("%s: unexpected: failed to unmarshal %s response, err: %v", loghdr, tsi.Name(), err)
		xreb.Abort()
		return
	}
	// enforce Smap consistency across this xreb
	tver, rver := status.SmapVersion, status.RebVersion
	if tver > ver || rver > ver {
		glog.Errorf("%s: %s has newer Smap (v%d, v%d) - aborting...", loghdr, tsi.Name(), tver, rver)
		xreb.Abort()
		return
	}
	// enforce same global transaction ID
	if status.GlobRebID > globRebID {
		glog.Errorf("%s: %s runs newer (g%d) transaction - aborting...", loghdr, tsi.Name(), status.GlobRebID)
		xreb.Abort()
		return
	}
	// let the target to catch-up
	if tver < ver || rver < ver {
		glog.Warningf("%s: %s has older Smap (v%d, v%d) - keep waiting...", loghdr, tsi.Name(), tver, rver)
		return
	}
	if status.GlobRebID < globRebID {
		glog.Warningf("%s: %s runs older (g%d) transaction - keep waiting...", loghdr, tsi.Name(), status.GlobRebID)
		return
	}
	if status.Stage >= desiredStage {
		ok = true
		return
	}
	glog.Infof("%s: %s[%s] not yet at the right stage, wait more...", loghdr, tsi.Name(), rebStage[status.Stage])
	return
}
