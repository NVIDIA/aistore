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
func (reb *rebManager) pingTarget(tsi *cluster.Snode, config *cmn.Config, ver int64, _ *xactGlobalReb) (ok bool) {
	var (
		tname      = reb.t.si.Name()
		maxwt      = config.Rebalance.DestRetryTime
		sleep      = config.Timeout.CplaneOperation
		sleepRetry = keepaliveRetryDuration(config)
		curwt      time.Duration
		args       = callArgs{
			si: tsi,
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Base:   tsi.IntraControlNet.DirectURL,
				Path:   cmn.URLPath(cmn.Version, cmn.Health),
			},
			timeout: config.Timeout.CplaneOperation,
		}
	)
	for curwt < maxwt {
		res := reb.t.call(args)
		if res.err == nil {
			if curwt > 0 {
				glog.Infof("%s: %s is online", tname, tsi.Name())
			}
			return true
		}
		args.timeout = sleepRetry
		glog.Warningf("%s: waiting for %s, err %v", tname, tsi.Name(), res.err)
		time.Sleep(sleep)
		curwt += sleep
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
		tname      = reb.t.si.Name()
		query      = url.Values{}
		sleep      = config.Timeout.CplaneOperation
		sleepRetry = keepaliveRetryDuration(config)
		maxwt      = config.Rebalance.DestRetryTime
		curwt      time.Duration
	)
	// prepare fillinStatus() request
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
	for curwt < maxwt {
		if xreb.Aborted() {
			glog.Infof("abrt rx-ready %s", tsi.Name())
			return
		}
		res := reb.t.call(args)
		if res.err != nil {
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			res = reb.t.call(args) // retry once
		}
		if res.err != nil {
			glog.Errorf("%s: failed to call %s, err: %v", tname, tsi.Name(), res.err)
			return
		}
		status := &rebStatus{}
		err := jsoniter.Unmarshal(res.outjson, status)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]",
				tsi.Name(), err, string(res.outjson))
			return
		}
		tver := status.SmapVersion
		if tver > ver {
			glog.Warningf("%s Smap v%d: %s has newer Smap v%d - aborting...", tname, ver, tsi.Name(), tver)
			xreb.Abort()
			return
		}
		if tver < ver {
			glog.Warningf("%s Smap v%d: %s Smap v%d(%t, %t) - more waiting...",
				tname, ver, tsi.Name(), tver, status.Aborted, status.Running)
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			continue
		}
		if status.SmapVersion != status.RebVersion {
			glog.Warningf("%s Smap v%d: %s Smap v%d(v%d, %t, %t) - even more waiting...",
				tname, ver, tsi.Name(), tver, status.RebVersion, status.Aborted, status.Running)
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			continue
		}
		if status.Stage >= rebStageTraverse {
			return true // good
		}
		glog.Infof("%s %s: %s %s - keep waiting",
			tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
		time.Sleep(sleep)
		curwt += sleep
	}
	return
}

// wait for target to a) finish traversing and b) cease waiting for my ACKs (type rebSyncCallback)
func (reb *rebManager) pollDone(tsi *cluster.Snode, config *cmn.Config, ver int64, xreb *xactGlobalReb) (ok bool) {
	var (
		tname      = reb.t.si.Name()
		query      = url.Values{}
		sleep      = config.Timeout.CplaneOperation
		sleepRetry = keepaliveRetryDuration(config)
		maxwt      = config.Rebalance.DestRetryTime
		curwt      time.Duration
	)
	// prepare fillinStatus() request
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
	for curwt < maxwt {
		time.Sleep(sleep)
		curwt += sleep
		if xreb.Aborted() {
			glog.Infof("abrt poll-done %s", tsi.Name())
			return
		}
		res := reb.t.call(args)
		if res.err != nil {
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			res = reb.t.call(args) // retry once
		}
		if res.err != nil {
			glog.Errorf("%s: failed to call %s, err: %v", tname, tsi.Name(), res.err)
			return
		}
		status := &rebStatus{}
		err := jsoniter.Unmarshal(res.outjson, status)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]",
				tsi.Name(), err, string(res.outjson))
			return
		}
		tver := status.SmapVersion
		if tver > ver {
			glog.Warningf("%s Smap v%d: %s has newer Smap v%d - aborting...", tname, ver, tsi.Name(), tver)
			xreb.Abort()
			return
		}
		if tver < ver {
			glog.Warningf("%s Smap v%d: %s Smap v%d(%t, %t) - more waiting...",
				tname, ver, tsi.Name(), tver, status.Aborted, status.Running)
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			continue
		}
		if status.SmapVersion != status.RebVersion {
			glog.Warningf("%s Smap v%d: %s Smap v%d(v%d, %t, %t) - even more waiting...",
				tname, ver, tsi.Name(), tver, status.RebVersion, status.Aborted, status.Running)
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			continue
		}
		// depending on stage the tsi is:
		if status.Stage > rebStageWaitAck {
			glog.Infof("%s %s: %s %s - done waiting", tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
			return true // good
		}
		if status.Stage <= rebStageTraverse {
			glog.Infof("%s %s: %s %s - keep waiting",
				tname, rebStage[reb.stage.Load()], tsi.Name(), rebStage[status.Stage])
			time.Sleep(sleepRetry)
			curwt += sleepRetry
			if status.Stage != rebStageInactive {
				curwt = 0 // NOTE: keep waiting forever or until tsi finishes traversing&transmitting
			}
		} else {
			var w4me bool // true: this target is waiting for ACKs from me (on the objects it had sent)
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
	}
	return
}
