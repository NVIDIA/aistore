// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

type (
	globalJogger struct {
		joggerBase
		smap  *cluster.Smap
		sema  chan struct{}
		errCh chan error
		ver   int64
	}
	globArgs struct {
		smap      *cluster.Smap
		config    *cmn.Config
		paths     fs.MPI
		pmarker   string
		ecUsed    bool
		dryRun    bool
		singleBck bool // rebalance running for a single bucket (e.g. after rename)
	}
)

func (reb *Manager) globalRebPrecheck(md *globArgs, globRebID int64) bool {
	// get EC rebalancer ready
	if md.ecUsed {
		reb.ecReb.cleanup()
		reb.ecReb.waiter.waitFor.Store(0)
	}
	// 1. check whether other targets are up and running
	if errCnt := reb.bcast(md, reb.pingTarget); errCnt > 0 {
		return false
	}
	if md.smap.Version == 0 {
		md.smap = reb.t.GetSowner().Get()
	}

	// 2. serialize (rebalancing operations - one at a time post this point)
	//    start new xaction unless the one for the current version is already in progress
	if newerSmap, alreadyRunning := reb.serialize(md.smap, md.config, globRebID); newerSmap || alreadyRunning {
		return false
	}
	if md.smap.Version == 0 {
		md.smap = reb.t.GetSowner().Get()
	}

	md.paths, _ = fs.Mountpaths.Get()
	return true
}

func (reb *Manager) globalRebInit(md *globArgs, globRebID int64, buckets ...string) bool {
	/* ================== rebStageInit ================== */
	reb.stage.Store(rebStageInit)
	reb.xreb = xaction.Registry.RenewGlobalReb(md.smap.Version, globRebID, len(md.paths)*2, reb.statRunner)
	cmn.Assert(reb.xreb != nil) // must renew given the CAS and checks above

	if len(buckets) > 0 {
		reb.xreb.SetBucket(buckets[0]) // for better identity (limited usage)
	}

	// 3. init streams and data structures
	reb.beginStats.Store(unsafe.Pointer(reb.getStats()))
	reb.beginStreams(md)
	reb.tcache.tmap = make(cluster.NodeMap, md.smap.CountTargets()-1)
	reb.tcache.mu = &sync.Mutex{}
	acks := reb.lomAcks()
	for i := 0; i < len(acks); i++ { // init lom acks
		acks[i] = &LomAcks{mu: &sync.Mutex{}, q: make(map[string]*cluster.LOM, 64)}
	}

	// 4. create persistent mark
	md.pmarker = cmn.PersistentMarker(cmn.ActGlobalReb)
	file, err := cmn.CreateFile(md.pmarker)
	if err != nil {
		glog.Errorln("Failed to create", md.pmarker, err)
		md.pmarker = ""
	} else {
		_ = file.Close()
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(md.smap))
	reb.globRebID.Store(globRebID)
	glog.Infof("%s: %s", reb.loghdr(globRebID, md.smap), reb.xreb.String())

	return true
}

// look for local slices/replicas
func (reb *Manager) buildECNamespace(md *globArgs) int {
	reb.ecReb.run()
	reb.stage.Store(rebStageECNameSpace)
	if reb.waitForPushReqs(md, rebStageECNameSpace) {
		return 0
	}
	return reb.bcast(md, reb.waitNamespace)
}

// send all collected slices to a correct target(that must have "main" object).
// It is a two-step process:
//   1. The target sends all colected data to correct targets
//   2. If the target is too fast, it may send too early(or in case of network
//      troubles) that results in data loss. But the target does not know if
//		the destination received the data. So, the targets enters
//		`rebStageECDetect` state that means "I'm ready to receive data
//		exchange requests"
//   3. In a perfect case, all push requests are successful and
//		`rebStageECDetect` stage will be finished in no time without any
//		data transfer
func (reb *Manager) distributeECNamespace(md *globArgs) error {
	const distributeTimeout = 5 * time.Minute
	if err := reb.ecReb.exchange(); err != nil {
		return err
	}
	reb.stage.Store(rebStageECDetect)
	if reb.waitForPushReqs(md, rebStageECDetect, distributeTimeout) {
		return nil
	}
	cnt := reb.bcast(md, reb.waitECData)
	if cnt != 0 {
		return fmt.Errorf("%d node failed to send their data", cnt)
	}
	return nil
}

// find out which objects are broken and how to fix them
func (reb *Manager) generateECFixList() {
	reb.ecReb.checkSlices()
	glog.Infof("Number of objects misplaced locally: %d", len(reb.ecReb.localActions))
	glog.Infof("Number of objects to be reconstructed/resent: %d", len(reb.ecReb.broken))
}

func (reb *Manager) ecFixLocal(md *globArgs) error {
	if err := reb.ecReb.rebalanceLocal(); err != nil {
		return fmt.Errorf("Failed to rebalance local slices/objects: %v", err)
	}

	reb.stage.Store(rebStageECGlobRepair)
	reb.setStage(reb.t.Snode().ID(), rebStageECGlobRepair)
	if cnt := reb.bcast(md, reb.waitECLocalReb); cnt != 0 {
		return fmt.Errorf("%d targets failed to complete local rebalance", cnt)
	}
	return nil
}

func (reb *Manager) ecFixGlobal(md *globArgs) error {
	if err := reb.ecReb.rebalanceGlobal(); err != nil {
		if !reb.xreb.Aborted() {
			glog.Errorf("EC rebalance failed: %v", err)
			reb.abortGlobal()
		}
		return err
	}

	reb.stage.Store(rebStageECCleanup)
	reb.setStage(reb.t.Snode().ID(), rebStageECCleanup)
	if cnt := reb.bcast(md, reb.waitECCleanup); cnt != 0 {
		return fmt.Errorf("%d targets failed to complete local rebalance", cnt)
	}
	return nil
}

// when at least one bucket has EC enabled
func (reb *Manager) globalRebRunEC(md *globArgs) error {
	// Update internal global rebalance args
	reb.ecReb.ra = md
	// Collect all local slices
	if cnt := reb.buildECNamespace(md); cnt != 0 {
		return fmt.Errorf("%d targets failed to build namespace", cnt)
	}
	// Waiting for all targets to send their lists of slices to other nodes
	if err := reb.distributeECNamespace(md); err != nil {
		return err
	}
	// Detect objects with misplaced or missing parts
	reb.generateECFixList()

	// Fix objects that are on local target but they are misplaced
	if err := reb.ecFixLocal(md); err != nil {
		return err
	}

	// Fix objects that needs network transfers and/or object rebuild
	if err := reb.ecFixGlobal(md); err != nil {
		return err
	}

	glog.Infof("[%s] RebalanceEC done", reb.t.Snode().ID())
	return nil
}

// when no bucket has EC enabled
func (reb *Manager) globalRebRun(md *globArgs) error {
	var (
		wg         = &sync.WaitGroup{}
		ver        = md.smap.Version
		globRebID  = reb.globRebID.Load()
		multiplier = md.config.Rebalance.Multiplier
		cfg        = cmn.GCO.Get()
	)
	_ = reb.bcast(md, reb.rxReady) // NOTE: ignore timeout
	if reb.xreb.Aborted() {
		err := fmt.Errorf("%s: aborted", reb.loghdr(globRebID, md.smap))
		return err
	}
	for _, mpathInfo := range md.paths {
		var (
			sema   chan struct{}
			mpathL string
		)
		mpathL = mpathInfo.MakePath(fs.ObjectType, cmn.ProviderAIS)
		if multiplier > 1 {
			sema = make(chan struct{}, multiplier)
		}
		rl := &globalJogger{
			joggerBase: joggerBase{m: reb, mpath: mpathL, xreb: &reb.xreb.RebBase, wg: wg},
			smap:       md.smap, sema: sema, ver: ver,
		}
		wg.Add(1)
		go rl.jog()
	}
	if cfg.CloudEnabled {
		for _, mpathInfo := range md.paths {
			var sema chan struct{}
			mpathC := mpathInfo.MakePath(fs.ObjectType, cfg.CloudProvider)
			if multiplier > 1 {
				sema = make(chan struct{}, multiplier)
			}
			rc := &globalJogger{
				joggerBase: joggerBase{m: reb, mpath: mpathC, xreb: &reb.xreb.RebBase, wg: wg},
				smap:       md.smap, sema: sema, ver: ver,
			}
			wg.Add(1)
			go rc.jog()
		}
	}
	wg.Wait()
	if reb.xreb.Aborted() {
		err := fmt.Errorf("%s: aborted", reb.loghdr(globRebID, md.smap))
		return err
	}
	return nil
}

// The function detects two cases(to reduce redundant goroutine creation):
// 1. One bucket case just calls a single rebalance worker depending on
//    whether a bucket is erasure coded. No goroutine is used.
// 2. Multi-bucket rebalance may start up to two rebalances in parallel and
//    wait for all finishes.
func (reb *Manager) globalRebSyncAndRun(md *globArgs) error {
	// 6. Capture stats, start mpath joggers TODO: currently supporting only fs.ObjectType (content-type)
	reb.stage.Store(rebStageTraverse)

	// No EC-enabled buckets - run only regular rebalance
	if !md.ecUsed {
		glog.Infof("Starting only regular rebalance")
		return reb.globalRebRun(md)
	}
	// Single bucket is rebalancing and it is a bucket with EC enabled.
	// Run only EC rebalance.
	if md.singleBck {
		glog.Infof("Starting only EC rebalance for a bucket")
		return reb.globalRebRunEC(md)
	}

	// In all other cases run both rebalances simultaneously
	var rebErr, ecRebErr error
	wg := &sync.WaitGroup{}
	ecMD := *md
	glog.Infof("Starting regular rebalance")
	wg.Add(1)
	go func() {
		defer wg.Done()
		md.ecUsed = false
		rebErr = reb.globalRebRun(md)
	}()

	wg.Add(1)
	glog.Infof("EC detected - starting EC rebalance")
	go func() {
		defer wg.Done()
		ecRebErr = reb.globalRebRunEC(&ecMD)
	}()
	wg.Wait()

	// Return the first encountered error
	if rebErr != nil {
		return rebErr
	}
	return ecRebErr
}

func (reb *Manager) globalRebWaitAck(md *globArgs) (errCnt int) {
	reb.stage.Store(rebStageWaitAck)
	globRebID := reb.globRebID.Load()
	loghdr := reb.loghdr(globRebID, md.smap)
	sleep := md.config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated
	maxwt := md.config.Rebalance.DestRetryTime
	cnt := 0
	maxwt += time.Duration(int64(time.Minute) * int64(md.smap.CountTargets()/10))
	maxwt = cmn.MinDur(maxwt, md.config.Rebalance.DestRetryTime*2)

	for {
		curwt := time.Duration(0)
		// poll for no more than maxwt while keeping track of the cumulative polling time via curwt
		// (here and elsewhere)
		for curwt < maxwt {
			cnt = 0
			var logged bool
			for _, lomack := range reb.lomAcks() {
				lomack.mu.Lock()
				if l := len(lomack.q); l > 0 {
					cnt += l
					if !logged {
						for _, lom := range lomack.q {
							tsi, err := cluster.HrwTarget(lom.Uname(), md.smap)
							if err == nil {
								glog.Infof("waiting for %s ACK from %s", lom, tsi)
								logged = true
								break
							}
						}
					}
				}
				lomack.mu.Unlock()
				if reb.xreb.Aborted() {
					glog.Infof("%s: abrt", loghdr)
					return
				}
			}
			if cnt == 0 {
				glog.Infof("%s: received all ACKs", loghdr)
				break
			}
			glog.Warningf("%s: waiting for %d ACKs", loghdr, cnt)
			if reb.xreb.AbortedAfter(sleep) {
				glog.Infof("%s: abrt", loghdr)
				return
			}
			curwt += sleep
		}
		if cnt > 0 {
			glog.Warningf("%s: timed-out waiting for %d ACK(s)", loghdr, cnt)
		}
		if reb.xreb.Aborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Mountpaths.Get()
		if len(aPaths) > len(md.paths) {
			glog.Warningf("%s: mountpath changes detected (%d, %d)", loghdr, len(aPaths), len(md.paths))
		}

		// 8. synchronize
		glog.Infof("%s: poll targets for: stage=(%s or %s***)", loghdr, stages[rebStageFin], stages[rebStageWaitAck])
		if !reb.waitForPushReqs(md, rebStageFin) {
			// no need to broadcast if all targets have sent notifications
			errCnt = reb.bcast(md, reb.waitFinExtended)
		}
		if reb.xreb.Aborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(reb.xreb, globRebID)
		if cnt == 0 || reb.xreb.Aborted() {
			break
		}
		glog.Warningf("%s: retransmitted %d, more wack...", loghdr, cnt)
	}

	return
}

// Waits until the following condition is true: no objects are received
// during certain configurable (quiescent) interval of time.
// if `cb` returns true the wait loop interrupts immediately. It is used,
// e.g., to wait for EC batch to finish: no need to wait until timeout if
// all targets have sent push notification that they are done with the batch.
func (reb *Manager) waitQuiesce(md *globArgs, maxWait time.Duration, cb func() bool) (
	aborted bool, timedout bool) {
	cmn.Assert(maxWait > 0)
	sleep := md.config.Timeout.CplaneOperation
	maxQuiet := int(maxWait/sleep) + 1
	quiescent := 0

	aborted = reb.xreb.Aborted()
	for quiescent < maxQuiet && !aborted {
		if !reb.laterx.CAS(true, false) {
			quiescent++
		} else {
			quiescent = 0
		}
		if cb != nil && cb() {
			break
		}
		aborted = reb.xreb.AbortedAfter(sleep)
	}

	return aborted, quiescent >= maxQuiet
}

func (reb *Manager) globalRebFini(md *globArgs) {
	// 10.5. keep at it... (can't close the streams as long as)
	maxWait := md.config.Rebalance.Quiesce
	aborted, _ := reb.waitQuiesce(md, maxWait, nil)
	if !aborted {
		if err := cmn.RemoveFile(md.pmarker); err != nil {
			glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.loghdr(reb.globRebID.Load(), md.smap), md.pmarker, err)
		}
	}
	reb.endStreams()
	reb.filterGFN.Reset()

	if !reb.xreb.Finished() {
		reb.xreb.EndTime(time.Now())
	} else {
		glog.Infoln(reb.xreb.String())
	}
	{
		status := &Status{}
		reb.GetGlobStatus(status)
		delta, err := jsoniter.MarshalIndent(&status.StatsDelta, "", " ")
		if err == nil {
			glog.Infoln(string(delta))
		}
	}
	reb.stage.Store(rebStageDone)
	reb.semaCh <- struct{}{}
}

// main method: 10 stages
// A note about rebalance stage management:
// Regular and EC rebalances are running in parallel. They share
// the rebalance stage in `Manager`. At this moment it is safe, because:
// 1. Parallel execution starts after `Manager` sets stage to rebStageTraverse
// 2. Regular rebalance does not change stage
// 3. EC rebalance changes the stage
// 4. Regular rebalance do checks like `stage > rebStageTraverse` or
//    `stage < rebStageWaitAck`. But since all EC stages are between
//    `Traverse` and `WaitAck` regular rebalance does not notice stage changes.
func (reb *Manager) RunGlobalReb(smap *cluster.Smap, globRebID int64, buckets ...string) {
	md := &globArgs{
		smap:      smap,
		config:    cmn.GCO.Get(),
		singleBck: len(buckets) == 1,
	}
	if len(buckets) == 0 || buckets[0] == "" {
		md.ecUsed = reb.t.GetBowner().Get().IsECUsed()
	} else {
		// single bucket rebalance is AIS case only
		bck := cluster.Bck{Name: buckets[0], Provider: cmn.ProviderAIS}
		props, ok := reb.t.GetBowner().Get().Get(&bck)
		if !ok {
			glog.Errorf("Bucket %q not found", bck.Name)
			return
		}
		md.ecUsed = props.EC.Enabled
	}

	if !reb.globalRebPrecheck(md, globRebID) {
		return
	}
	if !reb.globalRebInit(md, globRebID, buckets...) {
		return
	}

	// At this point only one rebalance is running so we can safely enable regular GFN.
	gfn := reb.t.GetGFN(cluster.GFNGlobal)
	gfn.Activate()
	defer gfn.Deactivate()

	errCnt := 0
	if err := reb.globalRebSyncAndRun(md); err == nil {
		errCnt = reb.globalRebWaitAck(md)
	} else {
		glog.Warning(err)
	}
	reb.stage.Store(rebStageFin)
	for errCnt != 0 && !reb.xreb.Aborted() {
		errCnt = reb.bcast(md, reb.waitFinExtended)
	}
	reb.globalRebFini(md)
	// clean up all collected data
	if md.ecUsed {
		reb.ecReb.cleanup()
	}

	reb.nodeStages = make(map[string]uint32) // cleanup after run
}

func (reb *Manager) onECData(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	reb.ecReb.OnData(w, hdr, objReader, err)
}

func (reb *Manager) GlobECDataStatus() (body []byte, status int) {
	globStatus := &Status{}
	reb.GetGlobStatus(globStatus)

	// the target is still collecting the data, reply that the result is not ready
	if globStatus.Stage < rebStageECDetect {
		return nil, http.StatusAccepted
	}

	// ask rebalance manager the list of all local slices
	slices, ok := reb.ecReb.nodeData(reb.t.Snode().ID())
	// no local slices found. It is possible if the number of object is small
	if !ok {
		return nil, http.StatusNoContent
	}

	body = cmn.MustMarshal(slices)
	return body, http.StatusOK
}

//
// globalJogger
//

func (rj *globalJogger) jog() {
	if rj.sema != nil {
		rj.errCh = make(chan error, cap(rj.sema)+1)
	}
	opts := &fs.Options{
		Callback: rj.walk,
		Sorted:   false,
	}
	if err := fs.Walk(rj.mpath, opts); err != nil {
		if rj.xreb.Aborted() || rj.xreb.Finished() {
			glog.Infof("Aborting %s traversal", rj.mpath)
		} else {
			glog.Errorf("%s: failed to traverse %s, err: %v", rj.m.t.Snode().Name(), rj.mpath, err)
		}
	}
	rj.xreb.NotifyDone()
	rj.wg.Done()
}

func (rj *globalJogger) objSentCallback(hdr transport.Header, r io.ReadCloser, lomptr unsafe.Pointer, err error) {
	var (
		lom = (*cluster.LOM)(lomptr)
		t   = rj.m.t
	)
	lom.Unlock(false)

	if err != nil {
		glog.Errorf("%s: failed to send o[%s/%s], err: %v", t.Snode().Name(), hdr.Bucket, hdr.ObjName, err)
		return
	}
	cmn.AssertMsg(hdr.ObjAttrs.Size == lom.Size(), lom.String()) // TODO: remove
	rj.m.statRunner.AddMany(
		stats.NamedVal64{Name: stats.TxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.TxRebSize, Value: hdr.ObjAttrs.Size})
}

// the walking callback is executed by the LRU xaction
func (rj *globalJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var (
		lom *cluster.LOM
		tsi *cluster.Snode
		t   = rj.m.t
	)
	if rj.xreb.Aborted() || rj.xreb.Finished() {
		return fmt.Errorf("%s: aborted, path %s", rj.xreb, rj.mpath)
	}
	if de.IsDir() {
		return nil
	}
	lom = &cluster.LOM{T: t, FQN: fqn}
	err = lom.Init("", "")
	if err != nil {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("%s, err %s - skipping...", lom, err)
		}
		return nil
	}

	// Skip a bucket with EC.Enabled - it is a job for EC rebalance
	if lom.Bck().Props.EC.Enabled {
		return filepath.SkipDir
	}

	// Rebalance, maybe
	tsi, err = cluster.HrwTarget(lom.Uname(), rj.smap)
	if err != nil {
		return err
	}
	if tsi.ID() == t.Snode().ID() {
		return nil
	}
	nver := t.GetSowner().Get().Version
	if nver > rj.ver {
		rj.m.abortGlobal()
		return fmt.Errorf("%s: Smap v%d < v%d, path %s", rj.xreb, rj.ver, nver, rj.mpath)
	}

	// skip objects that were already sent via GFN (due to probabilistic filtering
	// false-positives, albeit rare, are still possible)
	uname := []byte(lom.Uname())
	if rj.m.filterGFN.Lookup(uname) {
		rj.m.filterGFN.Delete(uname) // it will not be used anymore
		return nil
	}

	if err := lom.Load(); err != nil {
		return err
	}
	if rj.sema == nil { // rebalance.multiplier == 1
		err = rj.send(lom, tsi)
	} else { // // rebalance.multiplier > 1
		rj.sema <- struct{}{}
		go func() {
			ers := rj.send(lom, tsi)
			<-rj.sema
			if ers != nil {
				rj.errCh <- ers
			}
		}()
	}
	return
}

func (rj *globalJogger) send(lom *cluster.LOM, tsi *cluster.Snode) (err error) {
	var (
		file                  *cmn.FileHandle
		cksum                 *cmn.Cksum
		cksumType, cksumValue string
		lomack                *LomAcks
		idx                   int
	)
	lom.Lock(false) // NOTE: unlock in objSentCallback() unless err
	defer func() {
		if err == nil {
			return
		}
		lom.Unlock(false)
		if err != nil {
			if glog.FastV(4, glog.SmoduleReb) {
				glog.Errorf("%s, err: %v", lom, err)
			}
		}
	}()

	err = lom.Load(false)
	if err != nil {
		return
	}
	if lom.IsCopy() {
		lom.Unlock(false)
		return
	}
	if cksum, err = lom.CksumComputeIfMissing(); err != nil {
		return
	}
	cksumType, cksumValue = cksum.Get()
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		return
	}
	// cache it as pending-acknowledgement (optimistically - see objSentCallback)
	_, idx = lom.Hkey()
	lomack = rj.m.lomAcks()[idx]
	lomack.mu.Lock()
	lomack.q[lom.Uname()] = lom
	lomack.mu.Unlock()
	// transmit
	hdr := transport.Header{
		Bucket:   lom.Bucket(),
		Provider: lom.Provider(),
		ObjName:  lom.Objname,
		Opaque:   []byte(rj.m.t.Snode().ID()), // self == src
		ObjAttrs: transport.ObjectAttrs{
			Size:       lom.Size(),
			Atime:      lom.Atime().UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version(),
		},
	}
	o := transport.Obj{Hdr: hdr, Callback: rj.objSentCallback, CmplPtr: unsafe.Pointer(lom)}
	if err = rj.m.streams.Send(o, file, tsi); err != nil {
		lomack.mu.Lock()
		delete(lomack.q, lom.Uname())
		lomack.mu.Unlock()
		return
	}
	rj.m.laterx.Store(true)
	return
}
