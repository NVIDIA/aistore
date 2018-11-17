/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
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
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/stats"
	"github.com/json-iterator/go"
)

const NeighborRebalanceStartDelay = 10 * time.Second

var (
	runRebalanceOnce      = &sync.Once{}
	runLocalRebalanceOnce = &sync.Once{}
)

type xrebpathrunner struct {
	t         *targetrunner
	mpathplus string
	xreb      *xactRebalance
	wg        *sync.WaitGroup
	newsmap   *smapX
	aborted   bool
	fileMoved int64
	byteMoved int64
}

type localRebPathRunner struct {
	t         *targetrunner
	mpath     string
	xreb      *xactLocalRebalance
	aborted   bool
	fileMoved int64
	byteMoved int64
}

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

// the walking callback is executed by the LRU xaction
func (rcl *xrebpathrunner) rebwalkf(fqn string, osfi os.FileInfo, err error) error {
	// Check if we should abort
	select {
	case <-rcl.xreb.ChanAbort():
		err = fmt.Errorf("%s: aborted for path %s", rcl.xreb, rcl.mpathplus)
		glog.Infoln(err)
		glog.Flush()
		rcl.aborted = true
		return err
	default:
		break
	}

	// Skip files which are not movable (for example workfiles)
	if spec, _ := cluster.FileSpec(fqn); spec != nil && !spec.PermToMove() {
		return nil
	}
	if err != nil {
		// If we are traversing non-existing file we should not care
		if os.IsNotExist(err) {
			glog.Warningf("%s does not exist", fqn)
			return nil
		}
		// Otherwise we care
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	// Skip dirs
	if osfi.Mode().IsDir() {
		return nil
	}
	// rebalance maybe
	bucket, objname, err := cluster.ResolveFQN(fqn, rcl.t.bmdowner)
	if err != nil {
		// It may happen that local rebalance have not yet moved this particular
		// file and we got and error in fqn2bckobj. Since we might need still
		// move the file to another target we need to check if mountpath has
		// changed and prevent from the skipping it.
		if changed, _, e := rcl.t.changedMountpath(fqn); e != nil || !changed {
			glog.Warningf("%v - skipping...", err)
			return nil
		}
	}
	si, errstr := hrwTarget(bucket, objname, rcl.newsmap)
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
		// LRU cleans up the file later
		rcl.fileMoved++
		rcl.byteMoved += osfi.Size()
	}
	return nil
}

// LOCAL REBALANCE

func (rb *localRebPathRunner) run() {
	if err := filepath.Walk(rb.mpath, rb.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rb.mpath, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rb.mpath, err)
		}
	}

	rb.xreb.confirmCh <- struct{}{}
}

func (rb *localRebPathRunner) walk(fqn string, fileInfo os.FileInfo, err error) error {
	// Check if we should abort
	select {
	case <-rb.xreb.ChanAbort():
		err = fmt.Errorf("%s aborted, exiting rebwalkf path %s", rb.xreb, rb.mpath)
		glog.Infoln(err)
		glog.Flush()
		rb.aborted = true
		return err
	default:
		break
	}

	// Skip files which are not movable (for example workfiles)
	if spec, _ := cluster.FileSpec(fqn); spec != nil && !spec.PermToMove() {
		return nil
	}
	if err != nil {
		// If we are traversing non-existing file we should not care
		if os.IsNotExist(err) {
			glog.Warningf("%s does not exist", fqn)
			return nil
		}
		// Otherwise we care
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	// Skip dirs
	if fileInfo.IsDir() {
		return nil
	}

	// Check if we need to move files around
	changed, newFQN, err := rb.t.changedMountpath(fqn)
	if err != nil {
		glog.Warningf("%v - skipping...", err)
		return nil
	}
	if !changed {
		return nil
	}

	if glog.V(4) {
		glog.Infof("%s => %s", fqn, newFQN)
	}
	dir := filepath.Dir(newFQN)
	if err := cmn.CreateDir(dir); err != nil {
		glog.Errorf("Failed to create dir: %s", dir)
		rb.xreb.abort()
		rb.t.fshc(err, newFQN)
		return nil
	}

	// Copy the file instead of moving, LRU takes care of obsolete copies
	if glog.V(4) {
		glog.Infof("Copying %s -> %s", fqn, newFQN)
	}
	if errFQN, err := copyFile(fqn, newFQN); err != nil {
		glog.Error(err.Error())
		rb.t.fshc(err, errFQN)
		rb.xreb.abort()
		return nil
	}
	rb.fileMoved++
	rb.byteMoved += fileInfo.Size()

	return nil
}

// pingTarget pings target to check if it is running. After DestRetryTime it
// assumes that target is dead. Returns true if target is healthy and running,
// false otherwise.
func (t *targetrunner) pingTarget(si *cluster.Snode, timeout time.Duration, deadline time.Duration) bool {
	query := url.Values{}
	query.Add(cmn.URLParamFromID, t.si.DaemonID)

	pollstarted, ok := time.Now(), false
	callArgs := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodGet,
			base:   si.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Health),
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
func (t *targetrunner) waitForRebalanceFinish(si *cluster.Snode, rebalanceVersion int64) {
	// Phase 1: Call and check if smap is at least our version.
	query := url.Values{}
	query.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	args := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodGet,
			path:   cmn.URLPath(cmn.Version, cmn.Daemon),
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

		tsmap := &smapX{}
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
			base:   si.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Health),
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

func (t *targetrunner) runRebalance(newsmap *smapX, newtargetid string) {
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

		go func(si *cluster.Snode) {
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
	availablePaths, _ := fs.Mountpaths.Get()
	runnerCnt := len(availablePaths) * 2

	xreb := t.xactinp.renewRebalance(newsmap.Version, t, runnerCnt)
	if xreb == nil {
		return
	}
	pmarker := t.xactinp.rebalanceInProgress()
	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	glog.Infoln(xreb.String())
	wg = &sync.WaitGroup{}

	allr := make([]*xrebpathrunner, 0, runnerCnt)
	for _, mpathInfo := range availablePaths {
		rc := &xrebpathrunner{t: t, mpathplus: fs.Mountpaths.MakePathCloud(mpathInfo.Path), xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		go rc.oneRebalance()
		allr = append(allr, rc)

		rl := &xrebpathrunner{t: t, mpathplus: fs.Mountpaths.MakePathLocal(mpathInfo.Path), xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		go rl.oneRebalance()
		allr = append(allr, rl)
	}
	wg.Wait()

	if pmarker != "" {
		var aborted bool
		totalMovedN, totalMovedBytes := int64(0), int64(0)
		for _, r := range allr {
			if r.aborted {
				aborted = true
			}
			totalMovedN += r.fileMoved
			totalMovedBytes += r.byteMoved
		}
		if !aborted {
			if err := os.Remove(pmarker); err != nil {
				glog.Errorf("Failed to remove rebalance-in-progress mark %s, err: %v", pmarker, err)
			}
		}
		if totalMovedN > 0 {
			t.statsif.Add(stats.RebalGlobalCount, totalMovedN)
			t.statsif.Add(stats.RebalGlobalSize, totalMovedBytes)
		}
	}
	if newtargetid == t.si.DaemonID {
		glog.Infof("rebalance: %s <= self", newtargetid)
		t.pollRebalancingDone(newsmap) // until the cluster is fully rebalanced - see t.httpobjget
	}
	xreb.EndTime(time.Now())
	glog.Infoln(xreb.String())
	t.xactinp.del(xreb.ID())
}

func (t *targetrunner) pollRebalancingDone(newSmap *smapX) {
	wg := &sync.WaitGroup{}
	wg.Add(len(newSmap.Tmap) - 1)

	for _, si := range newSmap.Tmap {
		if si.DaemonID == t.si.DaemonID {
			continue
		}

		go func(si *cluster.Snode) {
			t.waitForRebalanceFinish(si, newSmap.version())
			wg.Done()
		}(si)
	}

	wg.Wait()
}

func (t *targetrunner) runLocalRebalance() {
	availablePaths, _ := fs.Mountpaths.Get()
	runnerCnt := len(availablePaths) * 2
	xreb := t.xactinp.renewLocalRebalance(t, runnerCnt)

	pmarker := t.xactinp.localRebalanceInProgress()
	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}
	allr := make([]*localRebPathRunner, 0, runnerCnt)

	wg := &sync.WaitGroup{}
	glog.Infof("starting local rebalance with %d runners\n", runnerCnt)
	for _, mpathInfo := range availablePaths {
		runner := &localRebPathRunner{t: t, mpath: fs.Mountpaths.MakePathCloud(mpathInfo.Path), xreb: xreb}
		wg.Add(1)
		go func(runner *localRebPathRunner) {
			runner.run()
			wg.Done()
		}(runner)
		allr = append(allr, runner)

		runner = &localRebPathRunner{t: t, mpath: fs.Mountpaths.MakePathLocal(mpathInfo.Path), xreb: xreb}
		wg.Add(1)
		go func(runner *localRebPathRunner) {
			runner.run()
			wg.Done()
		}(runner)
		allr = append(allr, runner)
	}
	wg.Wait()

	if pmarker != "" {
		var aborted bool
		totalMovedN, totalMovedBytes := int64(0), int64(0)
		for _, r := range allr {
			if r.aborted {
				aborted = true
			}
			totalMovedN += r.fileMoved
			totalMovedBytes += r.byteMoved
		}
		if !aborted {
			if err := os.Remove(pmarker); err != nil {
				glog.Errorf("Failed to remove rebalance-in-progress mark %s, err: %v", pmarker, err)
			}
		}
		if totalMovedN > 0 {
			t.statsif.Add(stats.RebalLocalCount, totalMovedN)
			t.statsif.Add(stats.RebalLocalSize, totalMovedBytes)
		}
	}

	xreb.EndTime(time.Now())
	glog.Infoln(xreb.String())
	t.xactinp.del(xreb.ID())
}
