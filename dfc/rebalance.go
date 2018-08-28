// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/json-iterator/go"
)

const NeighborRebalanceStartDelay = 10 * time.Second

var runRebalanceOnce = &sync.Once{}

type xrebpathrunner struct {
	t         *targetrunner
	mpathplus string
	xreb      *xactRebalance
	wg        *sync.WaitGroup
	newsmap   *Smap
	aborted   bool
}

// pingTarget pings target to check if it is running. After DestRetryTime it
// assumes that target is dead. Returns true if target is healthy and running,
// false otherwise.
func (t *targetrunner) pingTarget(si *daemonInfo, timeout time.Duration, deadline time.Duration) bool {
	query := url.Values{}
	query.Add(URLParamFromID, t.si.DaemonID)

	pollstarted, ok := time.Now(), false
	callArgs := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodGet,
			base:   si.InternalNet.DirectURL,
			path:   URLPath(Rversion, Rhealth),
			query:  query,
		},
		timeout: timeout,
	}

	for {
		res := t.call(callArgs)
		if res.err == nil {
			ok = true
			break
		}

		if res.status > 0 {
			glog.Infof("%s is offline with status %d, err: %v", si.PublicNet.DirectURL, res.status, res.err)
		} else {
			glog.Infof("%s is offline, err: %v", si.PublicNet.DirectURL, res.err)
		}
		callArgs.timeout = time.Duration(float64(callArgs.timeout)*1.5 + 0.5)
		if callArgs.timeout > ctx.config.Timeout.MaxKeepalive {
			callArgs.timeout = ctx.config.Timeout.MaxKeepalive
		}
		if time.Since(pollstarted) > deadline {
			break
		}
		time.Sleep(ctx.config.Timeout.CplaneOperation * keepaliveRetryFactor * 2)
	}

	return ok
}

// waitForRebalanceFinish waits for the other target to complete the current
// rebalancing operation.
func (t *targetrunner) waitForRebalanceFinish(si *daemonInfo, rebalanceVersion int64) {
	// Phase 1: Call and check if smap is at least our version.
	query := url.Values{}
	query.Add(URLParamWhat, GetWhatSmap)
	args := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodGet,
			path:   URLPath(Rversion, Rdaemon),
			query:  query,
		},
		timeout: defaultTimeout,
	}

	for {
		res := t.call(args)
		// retry once
		if res.err == context.DeadlineExceeded {
			args.timeout = ctx.config.Timeout.CplaneOperation * keepaliveTimeoutFactor * 2
			res = t.call(args)
		}

		if res.err != nil {
			glog.Errorf("Failed to call %s, err: %v - assuming down/unavailable", si.PublicNet.DirectURL, res.err)
			return
		}

		tsmap := &Smap{}
		err := jsoniter.Unmarshal(res.outjson, tsmap)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal response, err: %v [%v]", err, string(res.outjson))
			return
		}

		if tsmap.version() >= rebalanceVersion {
			break
		}

		time.Sleep(ctx.config.Timeout.CplaneOperation * keepaliveRetryFactor * 2)
	}

	// Phase 2: Wait to ensure any rebalancing on neighbor has kicked in.
	time.Sleep(NeighborRebalanceStartDelay)

	// Phase 3: Call thy neighbor to check whether it is rebalancing and wait until it is not.
	args = callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodGet,
			base:   si.InternalNet.DirectURL,
			path:   URLPath(Rversion, Rhealth),
		},
		timeout: defaultTimeout,
	}

	for {
		res := t.call(args)
		if res.err != nil {
			glog.Errorf("Failed to call %s, err: %v - assuming down/unavailable", si.PublicNet.DirectURL, res.err)
			break
		}

		status := &thealthstatus{}
		err := jsoniter.Unmarshal(res.outjson, status)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]", si.PublicNet.DirectURL, err, string(res.outjson))
			break
		}

		if !status.IsRebalancing {
			break
		}

		glog.Infof("waiting for rebalance: %v", si.PublicNet.DirectURL)
		time.Sleep(ctx.config.Timeout.CplaneOperation * keepaliveRetryFactor * 2)
	}
}

func (t *targetrunner) runRebalance(newsmap *Smap, newtargetid string) {
	neighborCnt := len(newsmap.Tmap) - 1

	//
	// first, check whether all the Smap-ed targets are up and running
	//
	glog.Infof("rebalance: Smap ver %d, newtargetid=%s", newsmap.version(), newtargetid)

	wg := &sync.WaitGroup{}
	wg.Add(neighborCnt)
	cancelCh := make(chan string, neighborCnt)

	for _, si := range newsmap.Tmap {
		if si.DaemonID == t.si.DaemonID {
			continue
		}

		go func(si *daemonInfo) {
			ok := t.pingTarget(
				si,
				ctx.config.Timeout.CplaneOperation*keepaliveTimeoutFactor,
				ctx.config.Rebalance.DestRetryTime,
			)
			if !ok {
				cancelCh <- si.PublicNet.DirectURL
			}
			wg.Done()
		}(si)
	}
	wg.Wait()

	close(cancelCh)
	if len(cancelCh) > 0 {
		sid := <-cancelCh
		glog.Errorf("Not starting rebalancing x-action: target %s appears to be offline", sid)
		return
	}

	// find and abort in-progress x-action if exists and if its smap version is lower
	// start new x-action unless the one for the current version is already in progress
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	runnersCnt := len(availablePaths) * 2

	xreb := t.xactinp.renewRebalance(newsmap.Version, t, runnersCnt)
	if xreb == nil {
		return
	}
	pmarker := t.xactinp.rebalanceInProgress()
	file, err := CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	glog.Infoln(xreb.tostring())
	wg = &sync.WaitGroup{}

	allr := make([]*xrebpathrunner, 0, runnersCnt)
	for _, mpathInfo := range availablePaths {
		rc := &xrebpathrunner{t: t, mpathplus: makePathCloud(mpathInfo.Path), xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		go rc.oneRebalance()
		allr = append(allr, rc)

		rl := &xrebpathrunner{t: t, mpathplus: makePathLocal(mpathInfo.Path), xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		go rl.oneRebalance()
		allr = append(allr, rl)
	}
	wg.Wait()
	if pmarker != "" {
		var aborted bool
		for _, r := range allr {
			if r.aborted {
				aborted = true
				break
			}
		}
		if !aborted {
			if err := os.Remove(pmarker); err != nil {
				glog.Errorf("Failed to remove rebalance-in-progress mark %s, err: %v", pmarker, err)
			}
		}
	}
	if newtargetid == t.si.DaemonID {
		glog.Infof("rebalance: %s <= self", newtargetid)
		t.pollRebalancingDone(newsmap) // until the cluster is fully rebalanced - see t.httpobjget
	}
	xreb.etime = time.Now()
	glog.Infoln(xreb.tostring())
	t.xactinp.del(xreb.id)
}

func (t *targetrunner) pollRebalancingDone(newSmap *Smap) {
	wg := &sync.WaitGroup{}
	wg.Add(len(newSmap.Tmap) - 1)

	for _, si := range newSmap.Tmap {
		if si.DaemonID == t.si.DaemonID {
			continue
		}

		go func(si *daemonInfo) {
			t.waitForRebalanceFinish(si, newSmap.version())
			wg.Done()
		}(si)
	}

	wg.Wait()
}

//=========================
//
// rebalance-runner methods
//
//=========================

func (rcl *xrebpathrunner) oneRebalance() {
	if err := filepath.Walk(rcl.mpathplus, rcl.rebwalkf); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rcl.mpathplus, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rcl.mpathplus, err)
		}
	}
	rcl.xreb.confirmCh <- struct{}{}
	rcl.wg.Done()
}

// the walking callback is execited by the LRU xaction
// (notice the receiver)
func (rcl *xrebpathrunner) rebwalkf(fqn string, osfi os.FileInfo, err error) error {
	// Check if we should abort
	select {
	case <-rcl.xreb.abrt:
		err = fmt.Errorf("%s aborted, exiting rebwalkf path %s", rcl.xreb.tostring(), rcl.mpathplus)
		glog.Infoln(err)
		glog.Flush()
		rcl.aborted = true
		return err
	default:
		break
	}

	// Skip working files
	if iswork, _ := rcl.t.isworkfile(fqn); iswork {
		return nil
	}
	if err != nil {
		// If we are traversing non-existing file we should not care
		if os.IsNotExist(err) {
			glog.Errorf("rebwalkf attempted to read a file that was deleted, file: %s", fqn)
			return nil
		}
		// Otherwise we care
		glog.Errorf("rebwalkf invoked with err: %v", err)
		return err
	}
	// Skip dirs
	if osfi.Mode().IsDir() {
		return nil
	}
	// rebalance maybe
	bucket, objname, errstr := rcl.t.fqn2bckobj(fqn)
	if errstr != "" {
		glog.Warningf("%s - skipping...", errstr)
		return nil
	}
	si, errstr := HrwTarget(bucket, objname, rcl.newsmap)
	if errstr != "" {
		return fmt.Errorf(errstr)
	}
	if si.DaemonID == rcl.t.si.DaemonID {
		return nil
	}

	// do rebalance
	if glog.V(4) {
		glog.Infof("%s/%s %s => %s", bucket, objname, rcl.t.si.DaemonID, si.DaemonID)
	}
	if errstr = rcl.t.sendfile(http.MethodPut, bucket, objname, si, osfi.Size(), "", ""); errstr != "" {
		glog.Infof("Failed to rebalance %s/%s: %s", bucket, objname, errstr)
	} else {
		// FIXME: TODO: delay the removal or (even) rely on the LRU
		if err := os.Remove(fqn); err != nil {
			glog.Errorf("Failed to delete %s after it has been moved, err: %v", fqn, err)
		}
	}
	return nil
}
