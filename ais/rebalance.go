// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
)

const NeighborRebalanceStartDelay = 10 * time.Second

var (
	runLocalRebalanceOnce = &sync.Once{}
)

type (
	globalRebJogger struct {
		t           *targetrunner
		mpath       string
		xreb        *xactRebalance
		wg          *sync.WaitGroup
		newsmap     *smapX
		atimeRespCh chan *atime.Response
		objectMoved int64
		byteMoved   int64
	}
	// FIXME: copy-paste, embed same base
	localRebJogger struct {
		t           *targetrunner
		mpath       string
		xreb        *xactLocalRebalance
		wg          *sync.WaitGroup
		objectMoved int64
		byteMoved   int64
		slab        *memsys.Slab2
		buf         []byte
	}
)

//
// GLOBAL REBALANCE
//

func (rcl *globalRebJogger) jog() {
	rcl.atimeRespCh = make(chan *atime.Response, 1)
	if err := filepath.Walk(rcl.mpath, rcl.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rcl.mpath, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rcl.mpath, err)
		}
	}

	rcl.xreb.confirmCh <- struct{}{}
	rcl.wg.Done()
}

func (rcl *globalRebJogger) rebalanceObjCallback(hdr transport.Header, r io.ReadCloser, err error) {
	uname := cluster.Uname(hdr.Bucket, hdr.Objname)
	rcl.t.rtnamemap.Unlock(uname, false)

	if err != nil {
		glog.Errorf("failed to send obj rebalance: %s/%s, err: %v", hdr.Bucket, hdr.Objname, err)
	} else {
		atomic.AddInt64(&rcl.objectMoved, 1)
		atomic.AddInt64(&rcl.byteMoved, hdr.ObjAttrs.Size)
	}

	rcl.wg.Done()
}

// the walking callback is executed by the LRU xaction
func (rcl *globalRebJogger) walk(fqn string, fi os.FileInfo, err error) error {
	var (
		file *cmn.FileHandle
	)

	if rcl.xreb.Aborted() {
		return fmt.Errorf("%s: aborted, path %s", rcl.xreb, rcl.mpath)
	}
	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if fi.Mode().IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: rcl.t, FQN: fqn}
	if errstr := lom.Fill("", 0); errstr != "" {
		if glog.V(4) {
			glog.Infof("%s, err %s - skipping...", lom, errstr)
		}
		return nil
	}

	// rebalance, maybe
	si, errstr := hrwTarget(lom.Bucket, lom.Objname, rcl.newsmap)
	if errstr != "" {
		return errors.New(errstr)
	}
	if si.DaemonID == rcl.t.si.DaemonID {
		return nil
	}
	// do rebalance
	if glog.V(4) {
		glog.Infof("%s %s => %s", lom, tname(rcl.t.si), tname(si))
	}

	if errstr := lom.Fill("", cluster.LomAtime|cluster.LomCksum|cluster.LomCksumMissingRecomp); errstr != "" {
		return errors.New(errstr)
	}
	if !lom.Exists() || lom.IsCopy() {
		return nil
	}

	// NOTE: Unlock happens in case of error or in rebalanceObjCallback.
	rcl.t.rtnamemap.Lock(lom.Uname, false)

	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		glog.Errorf("failed to open file: %s, err: %v", lom.FQN, err)
		rcl.t.rtnamemap.Unlock(lom.Uname, false)
		return err
	}

	cksumType, cksumValue := lom.Cksum.Get()
	hdr := transport.Header{
		Bucket:  lom.Bucket,
		Objname: lom.Objname,
		IsLocal: lom.BckIsLocal,
		Opaque:  []byte(rcl.t.si.DaemonID),
		ObjAttrs: transport.ObjectAttrs{
			Size:       fi.Size(),
			Atime:      lom.Atime.UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version,
		},
	}

	rcl.wg.Add(1) // NOTE: Done happens in case of SendV error or in rebalanceObjCallback.
	if err := rcl.t.streams.rebalance.SendV(hdr, file, rcl.rebalanceObjCallback, si); err != nil {
		glog.Errorf("failed to rebalance: %s, err: %v", lom.FQN, err)
		rcl.t.rtnamemap.Unlock(lom.Uname, false)
		rcl.wg.Done()
		return err
	}
	return nil
}

//
// LOCAL REBALANCE
//

func (rb *localRebJogger) jog() {
	rb.buf = rb.slab.Alloc()
	if err := filepath.Walk(rb.mpath, rb.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rb.mpath, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rb.mpath, err)
		}
	}

	rb.xreb.confirmCh <- struct{}{}
	rb.slab.Free(rb.buf)
	rb.wg.Done()
}

func (rb *localRebJogger) walk(fqn string, fileInfo os.FileInfo, err error) error {
	if rb.xreb.Aborted() {
		return fmt.Errorf("%s aborted, path %s", rb.xreb, rb.mpath)
	}

	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if fileInfo.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: rb.t, FQN: fqn}
	if errstr := lom.Fill("", cluster.LomFstat|cluster.LomCopy); errstr != "" {
		if glog.V(4) {
			glog.Infof("%s, err %v - skipping...", lom, err)
		}
		return nil
	}
	// local rebalance: skip local copies
	if !lom.Exists() || lom.IsCopy() {
		return nil
	}
	// check whether locally-misplaced
	if !lom.Misplaced() {
		return nil
	}
	if glog.V(4) {
		glog.Infof("%s => %s", lom, lom.HrwFQN)
	}
	dir := filepath.Dir(lom.HrwFQN)
	if err := cmn.CreateDir(dir); err != nil {
		glog.Errorf("Failed to create dir: %s", dir)
		rb.xreb.Abort()
		rb.t.fshc(err, lom.HrwFQN)
		return nil
	}

	// Copy the object instead of moving, LRU takes care of obsolete copies.
	// Note that global rebalance can run at the same time and by copying we
	// allow local and global rebalance to work in parallel - global rebalance
	// can still access the old object.

	if glog.V(4) {
		glog.Infof("Copying %s => %s", fqn, lom.HrwFQN)
	}

	rb.t.rtnamemap.Lock(lom.Uname, false)
	if err := lom.CopyObject(lom.HrwFQN, rb.buf); err != nil {
		rb.t.rtnamemap.Unlock(lom.Uname, false)
		if !os.IsNotExist(err) {
			rb.xreb.Abort()
			rb.t.fshc(err, lom.HrwFQN)
			return err
		}
		return nil
	}
	rb.t.rtnamemap.Unlock(lom.Uname, false)
	rb.objectMoved++
	rb.byteMoved += fileInfo.Size()
	return nil
}

// pingTarget pings target to check if it is running. After DestRetryTime it
// assumes that target is dead. Returns true if target is healthy and running,
// false otherwise.
func (t *targetrunner) pingTarget(si *cluster.Snode, config *cmn.Config) (ok bool) {
	var (
		timeout = keepaliveTimeoutDuration(config)
		retries = int(config.Rebalance.DestRetryTime / timeout)
		query   = url.Values{}
	)
	query.Add(cmn.URLParamFromID, t.si.DaemonID)

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
	for i := 1; i <= retries; i++ {
		res := t.call(callArgs)
		if res.err == nil {
			ok = true
			glog.Infof("%s: %s is online", tname(t.si), tname(si))
			break
		}
		s := fmt.Sprintf("retry #%d: %s is offline, err: %v", i, tname(si), res.err)
		if res.status > 0 {
			glog.Infof("%s, status=%d", s, res.status)
		} else {
			glog.Infoln(s)
		}
		callArgs.timeout += callArgs.timeout / 2
		if callArgs.timeout > config.Timeout.MaxKeepalive {
			callArgs.timeout = config.Timeout.MaxKeepalive
		}
		time.Sleep(keepaliveRetryDuration(config))
	}
	return
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
			args.timeout = keepaliveTimeoutDuration(config) * 2
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

		time.Sleep(keepaliveRetryDuration(config))
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
		time.Sleep(keepaliveRetryDuration(config))
	}
}

func (t *targetrunner) runRebalance(newsmap *smapX, newTargetID string) {
	var (
		wg       = &sync.WaitGroup{}
		cnt      = newsmap.CountTargets() - 1
		cancelCh = make(chan string, cnt)
		ver      = newsmap.version()
		config   = cmn.GCO.Get()
	)
	glog.Infof("%s: Smap v%d, newTargetID=%s", tname(t.si), ver, newTargetID)
	// first, check whether all the Smap-ed targets are up and running
	for _, si := range newsmap.Tmap {
		if si.DaemonID == t.si.DaemonID {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			ok := t.pingTarget(si, config)
			if !ok {
				cancelCh <- si.PublicNet.DirectURL
			}
			wg.Done()
		}(si)
	}
	wg.Wait()

	close(cancelCh)
	if len(cancelCh) > 0 {
		for sid := range cancelCh {
			glog.Errorf("Not starting rebalancing: %s appears to be offline, Smap v%d",
				newsmap.printname(sid), ver)
		}
		return
	}

	// abort in-progress xaction if exists and if its Smap version is lower
	// start new xaction unless the one for the current version is already in progress
	availablePaths, _ := fs.Mountpaths.Get()
	runnerCnt := len(availablePaths) * 2
	xreb := t.xactions.renewRebalance(ver, runnerCnt)
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
	// TODO: currently supporting a single content-type: Object
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		rc := &globalRebJogger{t: t, mpath: mpathC, xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		allr = append(allr, rc)
		go rc.jog()

		mpathL := mpathInfo.MakePath(fs.ObjectType, true /*is local*/)
		rl := &globalRebJogger{t: t, mpath: mpathL, xreb: xreb, wg: wg, newsmap: newsmap}
		wg.Add(1)
		allr = append(allr, rl)
		go rl.jog()
	}
	wg.Wait()

	if pmarker != "" {
		var (
			totalMovedN, totalMovedBytes int64
			aborted                      bool
		)
		for _, r := range allr {
			aborted = aborted || r.xreb.Aborted()
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
	if newTargetID == t.si.DaemonID {
		glog.Infof("rebalance %s(self)", tname(t.si))
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
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		runnerCnt         = len(availablePaths) * 2
		allr              = make([]*localRebJogger, 0, runnerCnt)
		xreb              = t.xactions.renewLocalRebalance(runnerCnt)
		pmarker           = t.xactions.localRebalanceInProgress()
		file, err         = cmn.CreateFile(pmarker)
	)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}
	wg := &sync.WaitGroup{}
	glog.Infof("starting local rebalance with %d runners\n", runnerCnt)
	slab := gmem2.SelectSlab2(cmn.MiB) // FIXME: estimate

	// TODO: currently supporting a single content-type: Object
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		jogger := &localRebJogger{t: t, mpath: mpathC, xreb: xreb, wg: wg, slab: slab}
		wg.Add(1)
		allr = append(allr, jogger)
		go jogger.jog()

		mpathL := mpathInfo.MakePath(fs.ObjectType, true /*is local*/)
		jogger = &localRebJogger{t: t, mpath: mpathL, xreb: xreb, wg: wg, slab: slab}
		wg.Add(1)
		allr = append(allr, jogger)
		go jogger.jog()
	}
	wg.Wait()

	if pmarker != "" {
		var aborted bool
		totalMovedN, totalMovedBytes := int64(0), int64(0)
		for _, r := range allr {
			aborted = aborted || r.xreb.Aborted()
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
