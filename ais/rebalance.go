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
	"unsafe"

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

type (
	rebManager struct {
		t         *targetrunner
		streams   *transport.StreamBundle
		acks      *transport.StreamBundle
		filterGFN *filter.Filter
		lomacks   [fs.LomCacheMask + 1]*LomAcks
		smap      atomic.Pointer // new smap which will be soon live
	}
	rebJoggerBase struct {
		m     *rebManager
		xreb  *xactRebBase
		mpath string
		wg    *sync.WaitGroup
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
	LomAcks struct {
		mu *sync.Mutex
		q  map[string]*cluster.LOM
	}
)

// persistent mark indicating rebalancing in progress
func persistentMarker(kind string) (pm string) {
	switch kind {
	case cmn.ActLocalReb:
		pm = filepath.Join(cmn.GCO.Get().Confdir, cmn.LocalRebMarker)
	case cmn.ActGlobalReb:
		pm = filepath.Join(cmn.GCO.Get().Confdir, cmn.GlobalRebMarker)
	default:
		cmn.Assert(false)
	}
	return
}

//
// rebManager
//

// NOTE: 9 (nine) steps
func (reb *rebManager) runGlobalReb(smap *smapX, newTargetID string) {
	var (
		wg       = &sync.WaitGroup{}
		cancelCh = make(chan *cluster.Snode, smap.CountTargets()-1)
		ver      = smap.version()
		config   = cmn.GCO.Get()
	)
	glog.Infof("%s: Smap v%d, newTargetID=%s", reb.t.si.Name(), ver, newTargetID)
	// 1. check whether other targets are up and running
	for _, si := range smap.Tmap {
		if si.DaemonID == reb.t.si.DaemonID {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			ok := reb.pingTarget(si, config)
			if !ok {
				cancelCh <- si
			}
			wg.Done()
		}(si)
	}
	wg.Wait()
	close(cancelCh)
	if len(cancelCh) > 0 {
		for si := range cancelCh {
			glog.Errorf("%s: skipping rebalance: %s offline, Smap v%d", reb.t.si.Name(), si.Name(), ver)
		}
		return
	}

	// 2. abort in-progress xaction if exists and if its Smap version is lower
	//    start new xaction unless the one for the current version is already in progress
	availablePaths, _ := fs.Mountpaths.Get()
	runnerCnt := len(availablePaths) * 2
	xreb := reb.t.xactions.renewGlobalReb(ver, runnerCnt)
	if xreb == nil {
		return
	}

	// 3. init streams and data structures
	reb.streams.Resync()
	reb.acks.Resync()

	reb.filterGFN.Reset() // start with empty filters
	acks := reb.lomAcks()
	for i := 0; i < len(acks); i++ { // init lom acks
		acks[i] = &LomAcks{mu: &sync.Mutex{}, q: make(map[string]*cluster.LOM, 64)}
	}

	// 4. create persistent mark
	pmarker := persistentMarker(cmn.ActGlobalReb)

	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(smap))
	glog.Infoln(xreb.String())

	wg = &sync.WaitGroup{}

	// 6. start mpath joggers
	// TODO: currently supporting a single content-type: Object
	// TODO: multiply joggers per mpath
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		rc := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: xreb, wg: wg}, smap: smap}
		wg.Add(1)
		go rc.jog()

		mpathL := mpathInfo.MakePath(fs.ObjectType, true /*local*/)
		rl := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: xreb, wg: wg}, smap: smap}
		wg.Add(1)
		go rl.jog()
	}
	wg.Wait()

	if pmarker != "" {
		if !xreb.Aborted() {
			if err := os.Remove(pmarker); err != nil && !os.IsNotExist(err) {
				glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.t.si.Name(), pmarker, err)
			}
		}
	}
	// 7. wait for ACKs
	var (
		sleep = config.Timeout.CplaneOperation
		maxwt = config.Rebalance.DestRetryTime + time.Duration(int64(time.Minute)*int64(smap.CountTargets()/10))
		curwt time.Duration
		cnt   int
	)
	maxwt = cmn.MinDur(maxwt, config.Rebalance.DestRetryTime*2)
	for curwt < maxwt {
		cnt = 0
		for _, lomack := range reb.lomAcks() {
			lomack.mu.Lock()
			if l := len(lomack.q); l > 0 {
				cnt += l
				for _, lom := range lomack.q {
					glog.Infof("waiting for %s ...", lom)
					break
				}
			}
			lomack.mu.Unlock()
		}
		if cnt == 0 {
			break
		}
		glog.Infof("waiting for %d acks", cnt)
		time.Sleep(sleep)
		curwt += sleep
	}
	if cnt > 0 {
		glog.Warningf("timed-out waiting for %d acks", cnt)
	}

	// 8. synchronize with the cluster
	// TODO: remove the `if`, synchronize on TBD rebalancing status: "received all ACKs"
	if newTargetID == reb.t.si.DaemonID {
		glog.Infof("%s: poll other targets for completion", reb.t.si.Name())
		reb.pollRebalancingAll(smap)
	}

	xreb.EndTime(time.Now())

	// 9. finally, deactivate GFN
	reb.t.gfn.global.deactivate()
}

func (reb *rebManager) pollRebalancingAll(newSmap *smapX) {
	wg := &sync.WaitGroup{}
	wg.Add(len(newSmap.Tmap) - 1)
	for _, si := range newSmap.Tmap {
		if si.DaemonID == reb.t.si.DaemonID {
			continue
		}
		go func(si *cluster.Snode) {
			reb.pollReb(si, newSmap.version())
			wg.Done()
		}(si)
	}
	wg.Wait()
}

// pollReb waits for the target (neighbor) to complete its current rebalancing operation
func (reb *rebManager) pollReb(tsi *cluster.Snode, rebalanceVersion int64) {
	var (
		tver       int64
		query      = url.Values{}
		config     = cmn.GCO.Get()
		sleep      = config.Timeout.CplaneOperation
		sleepRetry = keepaliveRetryDuration(config)
		maxwt      = config.Rebalance.DestRetryTime
		curwt      time.Duration
		args       = callArgs{
			si: tsi,
			req: reqArgs{
				method: http.MethodGet,
				base:   tsi.IntraControlNet.DirectURL,
				path:   cmn.URLPath(cmn.Version, cmn.Daemon),
				query:  query,
			},
			timeout: defaultTimeout,
		}
	)
	// check if neighbor's Smap is at least our version
	query.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	for curwt < maxwt {
		res := reb.t.call(args)
		if res.err == context.DeadlineExceeded {
			time.Sleep(sleepRetry)
			args.timeout = sleepRetry
			res = reb.t.call(args) // retry once
		}
		if res.err != nil {
			glog.Errorf("%s: failed to call %s, err: %v", reb.t.si.Name(), tsi.Name(), res.err)
			return
		}
		tsmap := &smapX{}
		err := jsoniter.Unmarshal(res.outjson, tsmap)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal response, err: %v [%v]", err, string(res.outjson))
			return
		}
		if tver = tsmap.version(); tver >= rebalanceVersion {
			break
		}
		time.Sleep(sleep)
	}
	if tver < rebalanceVersion {
		glog.Errorf("%s: failed to validate %s (neighbor's) Smap v%d", reb.t.si.Name(), tsi.Name(), tver)
		return
	}
	// wait until the neighbor finishes rebalancing
	args = callArgs{
		si: tsi,
		req: reqArgs{
			method: http.MethodGet,
			base:   tsi.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Health),
		},
		timeout: defaultTimeout,
	}
	time.Sleep(sleep * 2) // TODO: remove
	for {
		time.Sleep(sleep)
		res := reb.t.call(args)
		if res.err != nil {
			glog.Errorf("%s: failed to call %s, err: %v", reb.t.si.Name(), tsi.Name(), res.err)
			break
		}
		status := &thealthstatus{}
		err := jsoniter.Unmarshal(res.outjson, status)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]", tsi.Name(), err, string(res.outjson))
			break
		}
		if !status.IsRebalancing {
			break
		}
		glog.Infof("%s: waiting for %s to complete rebalancing...", reb.t.si.Name(), tsi.Name())
	}
}

// pingTarget pings target to check if it is running. After DestRetryTime it
// assumes that target is dead. Returns true if target is healthy and running,
// false otherwise.
func (reb *rebManager) pingTarget(si *cluster.Snode, config *cmn.Config) (ok bool) {
	var (
		maxwt      = config.Rebalance.DestRetryTime
		sleep      = config.Timeout.CplaneOperation
		sleepRetry = keepaliveRetryDuration(config)
		curwt      time.Duration
		args       = callArgs{
			si: si,
			req: reqArgs{
				method: http.MethodGet,
				base:   si.IntraControlNet.DirectURL,
				path:   cmn.URLPath(cmn.Version, cmn.Health),
			},
			timeout: config.Timeout.CplaneOperation,
		}
	)
	for curwt < maxwt {
		res := reb.t.call(args)
		if res.err == nil {
			if curwt > 0 {
				glog.Infof("%s: %s is online", reb.t.si.Name(), si.Name())
			}
			return true
		}
		args.timeout = sleepRetry
		glog.Warningf("%s: waiting for %s, err %v", reb.t.si.Name(), si.Name(), res.err)
		time.Sleep(sleep)
		curwt += sleep
	}
	glog.Errorf("%s: timed out waiting for %s", reb.t.si.Name(), si.Name())
	return
}

func (reb *rebManager) abortGlobalReb() {
	globalRebRunning := reb.t.xactions.globalXactRunning(cmn.ActGlobalReb)
	if !globalRebRunning {
		glog.Infof("not running, nothing to abort")
		return
	}
	reb.t.xactions.abortGlobalXact(cmn.ActGlobalReb)

	pmarker := persistentMarker(cmn.ActGlobalReb)
	if err := os.Remove(pmarker); err != nil && !os.IsNotExist(err) {
		glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.t.si.Name(), pmarker, err)
	}
}

func (reb *rebManager) lomAcks() *[fs.LomCacheMask + 1]*LomAcks { return &reb.lomacks }

func (reb *rebManager) recvRebalanceObj(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	smap := (*smapX)(reb.smap.Load())
	if smap == nil {
		var (
			config = cmn.GCO.Get()
			sleep  = config.Timeout.CplaneOperation
			maxwt  = config.Rebalance.DestRetryTime
			curwt  time.Duration
		)
		maxwt = cmn.MinDur(maxwt, config.Timeout.SendFile/3)
		glog.Warningf("%s: waiting to start...", reb.t.si.Name())
		time.Sleep(sleep)
		for curwt < maxwt {
			smap = (*smapX)(reb.smap.Load())
			if smap != nil {
				break
			}
			time.Sleep(sleep)
			curwt += sleep
		}
		if curwt >= maxwt {
			glog.Errorf("%s: timed-out waiting to start, dropping %s/%s", reb.t.si.Name(), hdr.Bucket, hdr.Objname)
			return
		}
	}
	// validate
	tsid := string(hdr.Opaque) // the sender
	tsi := smap.GetTarget(tsid)
	if tsi == nil {
		ver := smap.version()
		glog.Warningf("%s: unknown src target %s, Smap v%d, resync...", reb.t.si.Name(), tsid, ver)
		smap = reb.t.smapowner.get()
		reb.smap.Store(unsafe.Pointer(smap))
		tsi = smap.GetTarget(tsid)
		if tsi == nil {
			glog.Errorf("%s: unknown src target %s", reb.t.si.Name(), tsid)
			return
		}
		glog.Infof("resync Smap v%d => v%d", ver, smap.version())
		reb.streams.Resync()
		reb.acks.Resync()
	}
	// Rx
	lom, errstr := cluster.LOM{T: reb.t, Bucket: hdr.Bucket, Objname: hdr.Objname}.Init()
	if errstr != "" {
		glog.Error(errstr)
		return
	}
	lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
	lom.SetVersion(hdr.ObjAttrs.Version)
	roi := &recvObjInfo{
		started:      time.Now(),
		t:            reb.t,
		lom:          lom,
		workFQN:      fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut),
		r:            ioutil.NopCloser(objReader),
		cksumToCheck: cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
		migrated:     true,
	}
	if err, _ := roi.recv(); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: from %s %s", reb.t.si.Name(), tsi, roi.lom)
	}
	reb.t.statsif.AddMany(
		stats.NamedVal64{stats.RxRebCount, 1},
		stats.NamedVal64{stats.RxRebSize, hdr.ObjAttrs.Size})
	// ACK
	hdr.Opaque = []byte(reb.t.si.DaemonID) // self == src
	hdr.ObjAttrs.Size = 0
	if err := reb.acks.SendV(hdr, nil /* reader */, nil /* sent cb */, nil /* cmpl ptr */, tsi); err != nil {
		// TODO: collapse same-type errors e.g.: "src-id=>network: destination mismatch: stream => dst-id does not exist"
		glog.Error(err)
	}
}

func (reb *rebManager) recvRebalanceAck(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	lom, errstr := cluster.LOM{T: reb.t, Bucket: hdr.Bucket, Objname: hdr.Objname}.Init()
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: ack from %s on %s", reb.t.si.Name(), string(hdr.Opaque), lom)
	}
	if errstr != "" {
		glog.Errorln(errstr)
		return
	}
	_, idx := lom.Hkey()
	lomack := reb.lomAcks()[idx]
	lomack.mu.Lock()
	delete(lomack.q, lom.Uname())
	lomack.mu.Unlock()

	// TODO: support configurable delay
	// TODO: rebalancing stats and error counts
	cluster.ObjectLocker.Lock(lom.Uname(), true)
	lom.Uncache()
	_ = lom.DelAllCopies()
	if err = os.Remove(lom.FQN); err != nil && !os.IsNotExist(err) {
		glog.Errorf("%s: error removing %s, err: %v", reb.t.si.Name(), lom, err)
	}
	cluster.ObjectLocker.Unlock(lom.Uname(), true)
}

//
// globalRebJogger
//

func (rj *globalRebJogger) jog() {
	if err := filepath.Walk(rj.mpath, rj.walk); err != nil {
		if rj.xreb.Aborted() {
			glog.Infof("Aborting %s traversal", rj.mpath)
		} else {
			glog.Errorf("%s: failed to traverse %s, err: %v", rj.m.t.si.Name(), rj.mpath, err)
		}
	}

	rj.xreb.confirmCh <- struct{}{}
	rj.wg.Done()
}

func (rj *globalRebJogger) objSentCallback(hdr transport.Header, r io.ReadCloser, lomptr unsafe.Pointer, err error) {
	var lom = (*cluster.LOM)(lomptr)
	cluster.ObjectLocker.Unlock(lom.Uname(), false)
	rj.wg.Done()

	if err != nil {
		glog.Errorf("%s: failed to send o[%s/%s], err: %v", rj.m.t.si.Name(), hdr.Bucket, hdr.Objname, err)
		return
	}
	cmn.Assert(hdr.ObjAttrs.Size == lom.Size())
	rj.m.t.statsif.AddMany(
		stats.NamedVal64{stats.TxRebCount, 1},
		stats.NamedVal64{stats.TxRebSize, hdr.ObjAttrs.Size})
	_, idx := lom.Hkey()
	lomack := rj.m.lomAcks()[idx]
	lomack.mu.Lock()
	lomack.q[lom.Uname()] = lom
	lomack.mu.Unlock()
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

	// skip objects that were already sent via GFN (due to probabilistic filtering
	// false-positives, albeit rare, are still possible)
	uname := lom.Uname()
	if rj.m.filterGFN.Lookup([]byte(uname)) {
		rj.m.filterGFN.Delete([]byte(uname)) // it will not be used anymore
		return nil
	}

	// lock & transfer
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s => %s", lom, rj.m.t.si.Name(), si.Name())
	}
	cluster.ObjectLocker.Lock(uname, false) // NOTE: unlock in objSentCallback()

	_, errstr = lom.Load(false)
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
		Opaque:  []byte(rj.m.t.si.DaemonID), // self == src
		ObjAttrs: transport.ObjectAttrs{
			Size:       fi.Size(),
			Atime:      lom.Atime().UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version(),
		},
	}

	rj.wg.Add(1) // NOTE: wg.Done() in objSentCallback()
	if err := rj.m.t.rebManager.streams.SendV(hdr, file, rj.objSentCallback, unsafe.Pointer(lom) /* cmpl ptr */, si); err != nil {
		rj.wg.Done()
		goto rerr
	}
	return nil
rerr:
	cluster.ObjectLocker.Unlock(uname, false)
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
// rebManager -- local
//

func (reb *rebManager) runLocalReb() {
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		runnerCnt         = len(availablePaths) * 2
		xreb              = reb.t.xactions.renewLocalReb(runnerCnt)
		pmarker           = persistentMarker(cmn.ActLocalReb)
		file, err         = cmn.CreateFile(pmarker)
	)
	// deactivate local GFN
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

	// TODO: support non-object content types
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		jogger := &localRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: xreb, wg: wg}, slab: slab}
		wg.Add(1)
		go jogger.jog()

		mpathL := mpathInfo.MakePath(fs.ObjectType, true /*is local*/)
		jogger = &localRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: xreb, wg: wg}, slab: slab}
		wg.Add(1)
		go jogger.jog()
	}
	wg.Wait()

	if pmarker != "" {
		if !xreb.Aborted() {
			if err := os.Remove(pmarker); err != nil && !os.IsNotExist(err) {
				glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.t.si.Name(), pmarker, err)
			}
		}
	}
	xreb.EndTime(time.Now())
}

//
// localRebJogger
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
	_, errstr = lom.Load(false)
	if errstr != "" {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s, err %v - skipping #2...", lom, errstr)
		}
		return nil
	}
	// skip local copies
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

	// TODO: we take exclusive lock because the source and destination have
	// the same uname and we need to have exclusive lock on destination. If
	// we won't have exclusive we can end up with state where we read the
	// object but metadata is not yet persisted and it results in error - reproducible.
	// But taking exclusive lock on source is not a good idea since it will
	// prevent GETs from happening. Therefore, we need to think of better idea
	// to lock both source and destination but with different locks - probably
	// including mpath (whole string or some short hash) to uname, would be a good idea.
	cluster.ObjectLocker.Lock(lom.Uname(), true)
	dst, erc := lom.CopyObject(lom.HrwFQN, rj.buf)
	if erc == nil {
		erc = dst.Persist()
	}
	if erc != nil {
		cluster.ObjectLocker.Unlock(lom.Uname(), true)
		if !os.IsNotExist(erc) {
			rj.xreb.Abort()
			rj.m.t.fshc(erc, lom.HrwFQN)
			return erc
		}
		return nil
	}
	lom.Uncache()
	dst.Load(true)
	// TODO: support configurable delay
	// TODO: rebalancing stats and error counts
	// TODO: local migration of locally mirrored objects
	if err = os.Remove(lom.FQN); err != nil && !os.IsNotExist(err) {
		glog.Errorf("%s: error removing %s, err: %v", rj.m.t.si.Name(), lom, err)
	}
	cluster.ObjectLocker.Unlock(lom.Uname(), true)
	return nil
}
