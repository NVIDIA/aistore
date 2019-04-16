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
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
)

const (
	localRebType = iota
	globalRebType
)

const NeighborRebalanceStartDelay = 10 * time.Second

type (
	rebJoggerBase struct {
		m            *rebManager
		xreb         *xactRebBase
		mpath        string
		wg           *sync.WaitGroup
		objectsMoved atomic.Int64
		bytesMoved   atomic.Int64
	}

	globalRebJogger struct {
		rebJoggerBase
		smap *smapX // cluster.Smap?
	}

	localRebJogger struct {
		rebJoggerBase
		slab *memsys.Slab2
		buf  []byte
	}

	rebManager struct {
		t           *targetrunner
		streams     *transport.StreamBundle
		objectsSent *filter.Filter
	}
)

// persistent mark indicating rebalancing in progress
func persistentMarker(rebType int) string {
	switch rebType {
	case localRebType:
		return filepath.Join(cmn.GCO.Get().Confdir, cmn.LocalRebMarker)
	case globalRebType:
		return filepath.Join(cmn.GCO.Get().Confdir, cmn.GlobalRebMarker)
	}

	cmn.Assert(false)
	return ""
}

func (reb *rebManager) recvRebalanceObj(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	lom, errstr := cluster.LOM{T: reb.t, Bucket: hdr.Bucket, Objname: hdr.Objname}.Init()
	if errstr != "" {
		glog.Error(errstr)
		return
	}
	lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
	lom.SetVersion(hdr.ObjAttrs.Version)
	roi := &recvObjInfo{
		t:            reb.t,
		lom:          lom,
		migrated:     true,
		r:            ioutil.NopCloser(objReader),
		cksumToCheck: cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
	}
	roi.init()

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Rebalance %s from %s", roi.lom, hdr.Opaque)
	}

	if err, _ := roi.recv(); err != nil {
		glog.Error(err)
		return
	}

	reb.t.statsif.AddMany(stats.NamedVal64{stats.RxCount, 1}, stats.NamedVal64{stats.RxSize, hdr.ObjAttrs.Size})
}

//
// GLOBAL REBALANCE
//

func (rj *globalRebJogger) jog() {
	if err := filepath.Walk(rj.mpath, rj.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rj.mpath, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rj.mpath, err)
		}
	}

	rj.xreb.confirmCh <- struct{}{}
	rj.wg.Done()
}

func (rj *globalRebJogger) rebalanceObjCallback(hdr transport.Header, r io.ReadCloser, err error) {
	uname := cluster.Bo2Uname(hdr.Bucket, hdr.Objname)
	rj.m.t.rtnamemap.Unlock(uname, false)

	if err != nil {
		glog.Errorf("failed to send obj rebalance: %s/%s, err: %v", hdr.Bucket, hdr.Objname, err)
	} else {
		rj.objectsMoved.Inc()
		rj.bytesMoved.Add(hdr.ObjAttrs.Size)
	}

	rj.wg.Done()
}

// the walking callback is executed by the LRU xaction
func (rj *globalRebJogger) walk(fqn string, fi os.FileInfo, inerr error) (err error) {
	var (
		file                  *cmn.FileHandle
		lom                   *cluster.LOM
		si                    *cluster.Snode
		errstr                string
		hdr                   transport.Header
		cksum                 cmn.Cksummer
		cksumType, cksumValue string
	)
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s: aborted, path %s", rj.xreb, rj.mpath)
	}
	if inerr != nil {
		if errstr = cmn.PathWalkErr(inerr); errstr != "" {
			glog.Errorf(errstr)
			return inerr
		}
		return nil
	}
	if fi.Mode().IsDir() {
		return nil
	}
	lom, errstr = cluster.LOM{T: rj.m.t, FQN: fqn}.Init()
	if errstr != "" {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s, err %s - skipping...", lom, errstr)
		}
		return nil
	}

	// rebalance, maybe
	si, errstr = hrwTarget(lom.Bucket, lom.Objname, rj.smap)
	if errstr != "" {
		return errors.New(errstr)
	}
	if si.DaemonID == rj.m.t.si.DaemonID {
		return nil
	}

	// Skip objects that were already sent via GFN (due to probabilistic filtering
	// false-positives, albeit rare, are still possible)
	uname := lom.Uname()
	if rj.m.objectsSent.Lookup([]byte(uname)) {
		rj.m.objectsSent.Delete([]byte(uname)) // it will not be used anymore
		return nil
	}

	// LOCK & rebalance
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s => %s", lom, rj.m.t.si.Name(), si.Name())
	}
	rj.m.t.rtnamemap.Lock(uname, false) // NOTE: unlock in rebalanceObjCallback()

	errstr = lom.Load(false)
	if errstr != "" || !lom.Exists() || lom.IsCopy() {
		goto rerr
	}
	if cksum, errstr = lom.CksumComputeIfMissing(); errstr != "" {
		goto rerr
	}
	cksumType, cksumValue = cksum.Get()
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		goto rerr
	}
	hdr = transport.Header{
		Bucket:  lom.Bucket,
		Objname: lom.Objname,
		IsLocal: lom.BckIsLocal,
		Opaque:  []byte(si.DaemonID),
		ObjAttrs: transport.ObjectAttrs{
			Size:       fi.Size(),
			Atime:      lom.Atime().UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version(),
		},
	}

	rj.wg.Add(1) // NOTE: wg.Done() in rebalanceObjCallback()
	if err := rj.m.t.rebManager.streams.SendV(hdr, file, rj.rebalanceObjCallback, si); err != nil {
		rj.wg.Done()
		goto rerr
	}
	return nil
rerr:
	rj.m.t.rtnamemap.Unlock(uname, false)
	if errstr != "" {
		err = errors.New(errstr)
	}
	if err != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Errorf("%s, err: %v", lom, err)
		}
	}
	return
}

//
// LOCAL REBALANCE
//

func (rj *localRebJogger) jog() {
	rj.buf = rj.slab.Alloc()
	if err := filepath.Walk(rj.mpath, rj.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %s traversal due to: %s", rj.mpath, s)
		} else {
			glog.Errorf("Failed to traverse %s, err: %v", rj.mpath, err)
		}
	}

	rj.xreb.confirmCh <- struct{}{}
	rj.slab.Free(rj.buf)
	rj.wg.Done()
}

func (rj *localRebJogger) walk(fqn string, fileInfo os.FileInfo, err error) error {
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s aborted, path %s", rj.xreb, rj.mpath)
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
	lom, errstr := cluster.LOM{T: rj.m.t, FQN: fqn}.Init()
	if errstr != "" {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s, err %v - skipping #1...", lom, errstr)
		}
		return nil
	}
	// skip the directory if rebalance is disabled for the bucket
	if !lom.RebalanceConf().Enabled {
		return filepath.SkipDir
	}
	errstr = lom.Load(false)
	if errstr != "" {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s, err %v - skipping #2...", lom, errstr)
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
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s => %s", lom, lom.HrwFQN)
	}
	dir := filepath.Dir(lom.HrwFQN)
	if err := cmn.CreateDir(dir); err != nil {
		glog.Errorf("Failed to create dir: %s", dir)
		rj.xreb.Abort()
		rj.m.t.fshc(err, lom.HrwFQN)
		return nil
	}

	// Copy the object instead of moving, LRU takes care of obsolete copies.
	// Note that global rebalance can run at the same time and by copying we
	// allow local and global rebalance to work in parallel - global rebalance
	// can still access the old object.
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Copying %s => %s", fqn, lom.HrwFQN)
	}

	rj.m.t.rtnamemap.Lock(lom.Uname(), false)
	dst, erc := lom.CopyObject(lom.HrwFQN, rj.buf)
	if erc != nil {
		rj.m.t.rtnamemap.Unlock(lom.Uname(), false)
		if !os.IsNotExist(erc) {
			rj.xreb.Abort()
			rj.m.t.fshc(erc, lom.HrwFQN)
			return erc
		}
		return nil
	}
	dst.Load(true)
	rj.m.t.rtnamemap.Unlock(lom.Uname(), false)
	rj.objectsMoved.Inc()
	rj.bytesMoved.Add(fileInfo.Size())
	return nil
}

// pingTarget pings target to check if it is running. After DestRetryTime it
// assumes that target is dead. Returns true if target is healthy and running,
// false otherwise.
func (reb *rebManager) pingTarget(si *cluster.Snode, config *cmn.Config) (ok bool) {
	var (
		timeout = keepaliveTimeoutDuration(config)
		retries = int(config.Rebalance.DestRetryTime / timeout)
		query   = url.Values{}
	)
	query.Add(cmn.URLParamFromID, reb.t.si.DaemonID)

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
		res := reb.t.call(callArgs)
		if res.err == nil {
			ok = true
			glog.Infof("%s: %s is online", reb.t.si.Name(), si.Name())
			break
		}
		s := fmt.Sprintf("retry #%d: %s is offline, err: %v", i, si.Name(), res.err)
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
func (reb *rebManager) waitForRebalanceFinish(si *cluster.Snode, rebalanceVersion int64) {
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
		res := reb.t.call(args)
		// retry once
		if res.err == context.DeadlineExceeded {
			args.timeout = keepaliveTimeoutDuration(config) * 2
			res = reb.t.call(args)
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
		res := reb.t.call(args)
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

func (reb *rebManager) abortGlobalReb() {
	globalRebRunning := reb.t.xactions.globalXactRunning(cmn.ActGlobalReb)
	if !globalRebRunning {
		glog.Infof("not running, nothing to abort")
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Error("Global rebalance is aborting...")
	}

	reb.t.xactions.abortGlobalXact(cmn.ActGlobalReb)

	pmarker := persistentMarker(globalRebType)
	if err := os.Remove(pmarker); err != nil && !os.IsNotExist(err) {
		glog.Errorf("failed to remove in-progress mark %s, err: %v", pmarker, err)
	}
}

func (reb *rebManager) runGlobalReb(smap *smapX, newTargetID string) {
	var (
		wg       = &sync.WaitGroup{}
		cnt      = smap.CountTargets() - 1
		cancelCh = make(chan string, cnt)
		ver      = smap.version()
		config   = cmn.GCO.Get()
	)
	glog.Infof("%s: Smap v%d, newTargetID=%s", reb.t.si.Name(), ver, newTargetID)
	// first, check whether all the Smap-ed targets are up and running
	for _, si := range smap.Tmap {
		if si.DaemonID == reb.t.si.DaemonID {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			ok := reb.pingTarget(si, config)
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
			glog.Errorf("Not starting rebalancing: %s appears to be offline, Smap v%d", smap.printname(sid), ver)
		}
		return
	}

	// abort in-progress xaction if exists and if its Smap version is lower
	// start new xaction unless the one for the current version is already in progress
	availablePaths, _ := fs.Mountpaths.Get()
	runnerCnt := len(availablePaths) * 2
	xreb := reb.t.xactions.renewGlobalReb(ver, runnerCnt)
	if xreb == nil {
		return
	}

	// Rebalance has started so we can disable custom GFN lookup - GFN will still
	// happen but because of running rebalance.
	reb.t.gfn.global.deactivate()

	// Establish connections with nodes, as they are not created on change of smap
	reb.streams.Resync()

	reb.objectsSent.Reset() // start with empty filters

	pmarker := persistentMarker(globalRebType)

	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	glog.Infoln(xreb.String())
	wg = &sync.WaitGroup{}

	joggers := make([]*globalRebJogger, 0, runnerCnt)
	// TODO: currently supporting a single content-type: Object
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		rc := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: xreb, wg: wg}, smap: smap}
		wg.Add(1)
		joggers = append(joggers, rc)
		go rc.jog()

		mpathL := mpathInfo.MakePath(fs.ObjectType, true /*is local*/)
		rl := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: xreb, wg: wg}, smap: smap}
		wg.Add(1)
		joggers = append(joggers, rl)
		go rl.jog()
	}
	wg.Wait()

	if pmarker != "" {
		var (
			totalObjectsMoved, totalBytesMoved int64
		)
		for _, jogger := range joggers {
			totalObjectsMoved += jogger.objectsMoved.Load()
			totalBytesMoved += jogger.bytesMoved.Load()
		}
		if !xreb.Aborted() {
			if err := os.Remove(pmarker); err != nil {
				glog.Errorf("failed to remove in-progress mark %s, err: %v", pmarker, err)
			}
		}
		if totalObjectsMoved > 0 {
			reb.t.statsif.Add(stats.RebGlobalCount, totalObjectsMoved)
			reb.t.statsif.Add(stats.RebGlobalSize, totalBytesMoved)
		}
	}
	if newTargetID == reb.t.si.DaemonID {
		glog.Infof("rebalance %s(self)", reb.t.si.Name())
		reb.pollRebalancingDone(smap) // until the cluster is fully rebalanced - see t.httpobjget
	}
	xreb.EndTime(time.Now())
}

func (reb *rebManager) pollRebalancingDone(newSmap *smapX) {
	wg := &sync.WaitGroup{}
	wg.Add(len(newSmap.Tmap) - 1)

	for _, si := range newSmap.Tmap {
		if si.DaemonID == reb.t.si.DaemonID {
			continue
		}

		go func(si *cluster.Snode) {
			reb.waitForRebalanceFinish(si, newSmap.version())
			wg.Done()
		}(si)
	}

	wg.Wait()
}

func (reb *rebManager) runLocalReb() {
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		runnerCnt         = len(availablePaths) * 2
		joggers           = make([]*localRebJogger, 0, runnerCnt)

		xreb      = reb.t.xactions.renewLocalReb(runnerCnt)
		pmarker   = persistentMarker(localRebType)
		file, err = cmn.CreateFile(pmarker)
	)

	// Rebalance has started so we can disable custom GFN lookup - GFN will still
	// happen but because of running rebalance.
	reb.t.gfn.local.deactivate()

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
		jogger := &localRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: xreb, wg: wg}, slab: slab}
		wg.Add(1)
		joggers = append(joggers, jogger)
		go jogger.jog()

		mpathL := mpathInfo.MakePath(fs.ObjectType, true /*is local*/)
		jogger = &localRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: xreb, wg: wg}, slab: slab}
		wg.Add(1)
		joggers = append(joggers, jogger)
		go jogger.jog()
	}
	wg.Wait()

	if pmarker != "" {
		totalObjectsMoved, totalBytesMoved := int64(0), int64(0)
		for _, jogger := range joggers {
			totalObjectsMoved += jogger.objectsMoved.Load()
			totalBytesMoved += jogger.bytesMoved.Load()
		}
		if !xreb.Aborted() {
			if err := os.Remove(pmarker); err != nil {
				glog.Errorf("Failed to remove rebalance-in-progress mark %s, err: %v", pmarker, err)
			}
		}
		if totalObjectsMoved > 0 {
			reb.t.statsif.Add(stats.RebLocalCount, totalObjectsMoved)
			reb.t.statsif.Add(stats.RebLocalSize, totalBytesMoved)
		}
	}

	xreb.EndTime(time.Now())
}
