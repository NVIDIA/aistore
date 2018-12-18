// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/dfcpub/atime"
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

type (
	globalRebJogger struct {
		t           *targetrunner
		mpathplus   string
		xreb        *xactRebalance
		wg          *sync.WaitGroup
		newsmap     *smapX
		atimeRespCh chan *atime.Response
		objectMoved int64
		byteMoved   int64
		aborted     bool
	}
	localRebJogger struct {
		t           *targetrunner
		mpath       string
		xreb        *xactLocalRebalance
		wg          *sync.WaitGroup
		objectMoved int64
		byteMoved   int64
		aborted     bool
	}
)

//
// GLOBAL REBALANCE
//

func (rcl *globalRebJogger) jog() {
	rcl.atimeRespCh = make(chan *atime.Response, 1)
	if err := filepath.Walk(rcl.mpathplus, rcl.walk); err != nil {
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
func (rcl *globalRebJogger) walk(fqn string, osfi os.FileInfo, err error) error {
	// Check if we should abort
	select {
	case <-rcl.xreb.ChanAbort():
		rcl.aborted = true
		return fmt.Errorf("%s: aborted, path %s", rcl.xreb, rcl.mpathplus)
	default:
		break
	}
	if err != nil {
		// If we are traversing non-existing object we should not care
		if os.IsNotExist(err) {
			glog.Warningf("%s %s", fqn, cmn.DoesNotExist)
			return nil
		}
		// Otherwise we care
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: rcl.t, Fqn: fqn}
	if errstr := lom.Fill(0); errstr != "" {
		if glog.V(4) {
			glog.Infof("%s, err %s - skipping...", lom, errstr)
		}
		return nil
	}
	// rebalance, maybe
	si, errstr := hrwTarget(lom.Bucket, lom.Objname, rcl.newsmap)
	if errstr != "" {
		return fmt.Errorf(errstr)
	}
	if si.DaemonID == rcl.t.si.DaemonID {
		return nil
	}
	// do rebalance
	if glog.V(4) {
		glog.Infof("%s %s => %s", lom, rcl.t.si, si)
	}
	if errstr = rcl.t.sendfile(http.MethodPut, lom.Bucket, lom.Objname, si, osfi.Size(), "", "", rcl.atimeRespCh); errstr != "" {
		glog.Infof("Failed to rebalance %s: %s", lom, errstr)
	} else {
		// LRU cleans up the object later
		rcl.objectMoved++
		rcl.byteMoved += osfi.Size()
	}
	return nil
}

//
// LOCAL REBALANCE
//

func (rb *localRebJogger) jog() {
	if err := filepath.Walk(rb.mpath, rb.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rb.mpath, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rb.mpath, err)
		}
	}

	rb.xreb.confirmCh <- struct{}{}
	rb.wg.Done()
}

func (rb *localRebJogger) walk(fqn string, fileInfo os.FileInfo, err error) error {
	// Check if we should abort
	select {
	case <-rb.xreb.ChanAbort():
		rb.aborted = true
		return fmt.Errorf("%s aborted, path %s", rb.xreb, rb.mpath)
	default:
		break
	}
	if err != nil {
		// If we are traversing non-existing object we should not care
		if os.IsNotExist(err) {
			glog.Warningf("%s %s", fqn, cmn.DoesNotExist)
			return nil
		}
		// Otherwise we care
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	if fileInfo.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: rb.t, Fqn: fqn}
	if errstr := lom.Fill(0); errstr != "" {
		if glog.V(4) {
			glog.Infof("%s, err %v - skipping...", lom, err)
		}
		return nil
	}
	if !lom.Misplaced {
		return nil
	}
	if glog.V(4) {
		glog.Infof("%s => %s", lom, lom.Newfqn)
	}
	dir := filepath.Dir(lom.Newfqn)
	if err := cmn.CreateDir(dir); err != nil {
		glog.Errorf("Failed to create dir: %s", dir)
		rb.xreb.Abort()
		rb.t.fshc(err, lom.Newfqn)
		return nil
	}

	// Copy the object instead of moving, LRU takes care of obsolete copies
	if glog.V(4) {
		glog.Infof("Copying %s => %s", fqn, lom.Newfqn)
	}
	if errFQN, err := copyFile(fqn, lom.Newfqn); err != nil {
		glog.Error(err.Error())
		rb.t.fshc(err, errFQN)
		rb.xreb.Abort()
		return nil
	}
	rb.objectMoved++
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

	config := cmn.GCO.Get()
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
		if callArgs.timeout > config.Timeout.MaxKeepalive {
			callArgs.timeout = config.Timeout.MaxKeepalive
		}
		if time.Since(pollstarted) > deadline {
			break
		}
		time.Sleep(config.Timeout.CplaneOperation * keepaliveRetryFactor * 2)
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

	config := cmn.GCO.Get()
	for {
		res := t.call(args)
		// retry once
		if res.err == context.DeadlineExceeded {
			args.timeout = config.Timeout.CplaneOperation * keepaliveTimeoutFactor * 2
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

		time.Sleep(config.Timeout.CplaneOperation * keepaliveRetryFactor * 2)
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
		time.Sleep(config.Timeout.CplaneOperation * keepaliveRetryFactor * 2)
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

	config := cmn.GCO.Get()
	for _, si := range newsmap.Tmap {
		if si.DaemonID == t.si.DaemonID {
			continue
		}

		go func(si *cluster.Snode) {
			ok := t.pingTarget(
				si,
				config.Timeout.CplaneOperation*keepaliveTimeoutFactor,
				config.Rebalance.DestRetryTime,
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

	// FIXME: It is a little bit hacky... Duplication is not the best idea..
	contentRebalancers := 0
	for _, contentResolver := range fs.CSM.RegisteredContentTypes {
		if contentResolver.PermToMove() {
			contentRebalancers++
		}
	}
	runnerCnt := contentRebalancers * len(availablePaths) * 2

	xreb := t.xactions.renewRebalance(newsmap.Version, runnerCnt)
	if xreb == nil {
		return
	}
	pmarker := t.xactions.rebalanceInProgress()
	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	glog.Infoln(xreb.String())
	wg = &sync.WaitGroup{}

	allr := make([]*globalRebJogger, 0, runnerCnt)
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		// Do not start rebalance if given content type has no permission to move.
		// NOTE: In the future we might have case where we would like to check it
		// per object/workfile basis rather that directory level.
		if !contentResolver.PermToMove() {
			continue
		}

		for _, mpathInfo := range availablePaths {
			rc := &globalRebJogger{t: t, mpathplus: fs.Mountpaths.MakePathCloud(mpathInfo.Path, contentType), xreb: xreb, wg: wg, newsmap: newsmap}
			wg.Add(1)
			go rc.jog()
			allr = append(allr, rc)

			rl := &globalRebJogger{t: t, mpathplus: fs.Mountpaths.MakePathLocal(mpathInfo.Path, contentType), xreb: xreb, wg: wg, newsmap: newsmap}
			wg.Add(1)
			go rl.jog()
			allr = append(allr, rl)
		}
	}
	wg.Wait()

	if pmarker != "" {
		var aborted bool
		totalMovedN, totalMovedBytes := int64(0), int64(0)
		for _, r := range allr {
			if r.aborted {
				aborted = true
			}
			totalMovedN += r.objectMoved
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
	// FIXME: It is a little bit hacky... Duplication is not the best idea..
	contentRebalancers := 0
	for _, contentResolver := range fs.CSM.RegisteredContentTypes {
		if contentResolver.PermToMove() {
			contentRebalancers++
		}
	}
	runnerCnt := contentRebalancers * len(availablePaths) * 2
	xreb := t.xactions.renewLocalRebalance(runnerCnt)

	pmarker := t.xactions.localRebalanceInProgress()
	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}
	allr := make([]*localRebJogger, 0, runnerCnt)

	wg := &sync.WaitGroup{}
	glog.Infof("starting local rebalance with %d runners\n", runnerCnt)
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		// Do not start rebalance if given content type has no permission to move.
		// NOTE: In the future we might have case where we would like to check it
		// per object/workfile basis rather that directory level.
		if !contentResolver.PermToMove() {
			continue
		}

		for _, mpathInfo := range availablePaths {
			jogger := &localRebJogger{t: t, mpath: fs.Mountpaths.MakePathCloud(mpathInfo.Path, contentType), xreb: xreb, wg: wg}
			wg.Add(1)
			go jogger.jog()
			allr = append(allr, jogger)

			jogger = &localRebJogger{t: t, mpath: fs.Mountpaths.MakePathLocal(mpathInfo.Path, contentType), xreb: xreb, wg: wg}
			wg.Add(1)
			go jogger.jog()
			allr = append(allr, jogger)
		}
	}
	wg.Wait()

	if pmarker != "" {
		var aborted bool
		totalMovedN, totalMovedBytes := int64(0), int64(0)
		for _, r := range allr {
			if r.aborted {
				aborted = true
			}
			totalMovedN += r.objectMoved
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
}
