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
	jsoniter "github.com/json-iterator/go"
)

//
// global rebalance: cluster-wide synchronization at certain stages
//

// main method
func (reb *rebManager) bcast(smap *smapX, config *cmn.Config, cb rebSyncCallback, xreb *xactGlobalReb) (errCnt int) {
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
			if !cb(tsi, config, ver, xreb) {
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
func (reb *rebManager) pingTarget(tsi *cluster.Snode, config *cmn.Config, ver int64, _ *xactGlobalReb) (ok bool) {
	var (
		tname = reb.t.si.Name()
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
				glog.Infof("%s: %s is online", tname, tsi.Name())
			}
			return true
		}
		glog.Warningf("%s: waiting for %s, err %v", tname, tsi.Name(), res.err)
		time.Sleep(sleep)
		nver := reb.t.smapowner.get().version()
		if nver > ver {
			return
		}
	}
	glog.Errorf("%s: timed-out waiting for %s", tname, tsi.Name())
	return
}

// wait for target to get ready to receive objects (type rebSyncCallback)
func (reb *rebManager) rxReady(tsi *cluster.Snode, config *cmn.Config, ver int64, xreb *xactGlobalReb) (ok bool) {
	var (
		sleep = config.Timeout.CplaneOperation
		maxwt = config.Rebalance.DestRetryTime
		curwt time.Duration
	)
	for curwt < maxwt {
		if _, ok = reb.checkGlobStatus(tsi, config, ver, xreb, rebStageTraverse); ok {
			return
		}
		if xreb.Aborted() {
			glog.Infof("abrt rx-ready %s", tsi.Name())
			return
		}
		time.Sleep(sleep)
		curwt += sleep
	}
	return
}

// wait for the target to reach strage = rebStageFin (i.e., finish traversing and sending)
// if the target that has reached rebStageWaitAck but not yet in the rebStageFin stage,
// separately check whether it is waiting for my ACKs
func (reb *rebManager) waitFinExtended(tsi *cluster.Snode, config *cmn.Config, ver int64, xreb *xactGlobalReb) (ok bool) {
	var (
		sleep      = config.Timeout.CplaneOperation
		maxwt      = config.Rebalance.DestRetryTime
		tname      = reb.t.si.Name()
		sleepRetry = keepaliveRetryDuration(config)
		curwt      time.Duration
		status     *rebStatus
	)
	for curwt < maxwt {
		time.Sleep(sleep)
		curwt += sleep
		if status, ok = reb.checkGlobStatus(tsi, config, ver, xreb, rebStageFin); ok {
			return
		}
		if xreb.Aborted() {
			glog.Infof("abrt wack %s", tsi.Name())
			return
		}
		if status.Stage <= rebStageTraverse {
			glog.Infof("%s %s: %s %s - keep waiting",
				tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
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
				glog.Infof("%s %s: <= %s %s - keep wack",
					tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
				w4me = true
				break
			}
		}
		if !w4me {
			glog.Infof("%s %s: %s %s - not waiting for me",
				tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
			return
		}
		time.Sleep(sleepRetry)
		curwt += sleepRetry
	}
	return
}

// calls tsi.reb.getGlobStatus() and handles conditions; may abort the current xreb
// returns OK if the desiredStage has been reached
func (reb *rebManager) checkGlobStatus(tsi *cluster.Snode, config *cmn.Config, ver int64, xreb *xactGlobalReb,
	desiredStage uint32) (status *rebStatus, ok bool) {
	var (
		tname      = reb.t.si.Name()
		query      = url.Values{}
		sleepRetry = keepaliveRetryDuration(config)
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
		time.Sleep(sleepRetry)
		res = reb.t.call(args) // retry once
	}
	if res.err != nil {
		glog.Errorf("%s: failed to call %s, err: %v", tname, tsi.Name(), res.err)
		xreb.Abort()
		return
	}
	status = &rebStatus{}
	err := jsoniter.Unmarshal(res.outjson, status)
	if err != nil {
		glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]", tsi.Name(), err, string(res.outjson))
		xreb.Abort()
		return
	}
	// enforce Smap consistency across this xreb
	tver, rver := status.SmapVersion, status.RebVersion
	if tver > ver || rver > ver {
		glog.Warningf("%s Smap v%d: %s has newer Smap (v%d, v%d) - aborting...", tname, ver, tsi.Name(), tver, rver)
		xreb.Abort()
		return
	}
	if tver < ver || rver < ver {
		glog.Warningf("%s Smap v%d: %s has older Smap (v%d, v%d) - aborting...", tname, ver, tsi.Name(), tver, rver)
	}
	if status.Stage >= desiredStage {
		ok = true
		return
	}
	glog.Infof("%s %s: %s %s - keep waiting",
		tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
	return
}
