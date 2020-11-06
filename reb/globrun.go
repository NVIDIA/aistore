// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
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
	"github.com/NVIDIA/aistore/xaction/xreg"
	"github.com/NVIDIA/aistore/xaction/xrun"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

type (
	rebalanceJogger struct {
		joggerBase
		smap *cluster.Smap
		sema *cmn.DynSemaphore
		ver  int64
	}
	rebArgs struct {
		id     int64
		smap   *cluster.Smap
		config *cmn.Config
		paths  fs.MPI
		ecUsed bool
	}
)

//
// run sequence: non-EC and EC global
//
// main method: serialized to run one at a time and goes through controlled enumerated stages
// A note on stage management:
// 1. Non-EC and EC rebalances run in parallel
// 2. Execution starts after the `Manager` sets the current stage to rebStageTraverse
// 3. Only EC rebalance changes the current stage
// 4. Global rebalance performs checks such as `stage > rebStageTraverse` or
//    `stage < rebStageWaitAck`. Since all EC stages are between
//    `Traverse` and `WaitAck` non-EC rebalance does not "notice" stage changes.
func (reb *Manager) RunRebalance(smap *cluster.Smap, id int64, notif *xaction.NotifXact) {
	md := &rebArgs{
		id:     id,
		smap:   smap,
		config: cmn.GCO.Get(),
		ecUsed: reb.t.Bowner().Get().IsECUsed(),
	}

	if !reb.rebPreInit(md) {
		return
	}
	if !reb.rebInit(md, notif) {
		return
	}

	// At this point only one rebalance is running so we can safely enable regular GFN.
	gfn := reb.t.GFN(cluster.GFNGlobal)
	gfn.Activate()
	defer gfn.Deactivate()

	errCnt := 0
	err := reb.rebSyncAndRun(md)
	if err == nil {
		errCnt = reb.rebWaitAck(md)
	} else {
		glog.Warning(err)
	}
	reb.changeStage(rebStageFin, 0)
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("global reb (v%d) in %s state", md.id, stages[rebStageFin])
	}

	for errCnt != 0 && !reb.xact().Aborted() {
		errCnt = reb.bcast(md, reb.waitFinExtended)
	}
	reb.rebFini(md, err)
}

// To optimize goroutine creation:
// 1. One bucket case just calls a single rebalance worker depending on
//    whether a bucket is erasure coded (goroutine is not used).
// 2. Multi-bucket rebalance may start both non-EC and EC in parallel.
//    It then waits until everything finishes.
func (reb *Manager) rebSyncAndRun(md *rebArgs) error {
	// 6. Capture stats, start mpath joggers
	reb.stages.stage.Store(rebStageTraverse)

	// No EC-enabled buckets - run only regular rebalance
	if !md.ecUsed {
		glog.Infof("starting global rebalance (g%d)", md.id)
		return reb.runNoEC(md)
	}

	// In all other cases run both rebalances simultaneously
	group := &errgroup.Group{}
	group.Go(func() error {
		glog.Infof("starting global rebalance (g%d)", md.id)
		return reb.runNoEC(md)
	})
	group.Go(func() error {
		glog.Infof("EC detected - starting EC rebalance (g%d)", md.id)
		return reb.runEC(md)
	})
	return group.Wait()
}

func (reb *Manager) rebPreInit(md *rebArgs) bool {
	glog.FastV(4, glog.SmoduleReb).Infof("global reb (v%d) started pre init", md.id)
	// 1. check whether other targets are up and running
	glog.FastV(4, glog.SmoduleReb).Infof("global reb broadcast (v%d)", md.id)
	if errCnt := reb.bcast(md, reb.pingTarget); errCnt > 0 {
		return false
	}
	if md.smap.Version == 0 {
		md.smap = reb.t.Sowner().Get()
	}

	// 2. serialize (rebalancing operations - one at a time post this point)
	//    start new xaction unless the one for the current version is already in progress
	glog.FastV(4, glog.SmoduleReb).Infof("global reb serialize (v%d)", md.id)
	if newerRMD, alreadyRunning := reb.serialize(md); newerRMD || alreadyRunning {
		return false
	}
	if md.smap.Version == 0 {
		md.smap = reb.t.Sowner().Get()
	}

	md.paths, _ = fs.Get()
	return true
}

func (reb *Manager) serialize(md *rebArgs) (newerRMD, alreadyRunning bool) {
	var (
		sleep  = md.config.Timeout.CplaneOperation
		canRun bool
	)
	for {
		select {
		case <-reb.semaCh:
			canRun = true
		default:
			runtime.Gosched()
		}

		// Compare rebIDs
		logHdr := reb.logHdr(md)
		if reb.rebID.Load() > md.id {
			glog.Warningf("%s: seeing newer rebID g%d, not running", logHdr, reb.rebID.Load())
			newerRMD = true
			if canRun {
				reb.semaCh <- struct{}{}
			}
			return
		}
		if reb.rebID.Load() == md.id {
			if canRun {
				reb.semaCh <- struct{}{}
			}
			glog.Warningf("%s: g%d is already running", logHdr, md.id)
			alreadyRunning = true
			return
		}

		// Check current xaction
		entry := xreg.GetRunning(xreg.XactFilter{Kind: cmn.ActRebalance})
		if entry == nil {
			if canRun {
				return
			}
			glog.Warningf("%s: waiting for ???...", logHdr)
		} else {
			otherXreb := entry.Get().(*xrun.Rebalance) // running or previous
			if canRun {
				return
			}
			if otherXreb.ID().Int() < md.id {
				otherXreb.Abort()
				glog.Warningf("%s: aborting older [%s]", logHdr, otherXreb)
			}
		}
		cmn.Assert(!canRun)
		time.Sleep(sleep)
	}
}

func (reb *Manager) rebInit(md *rebArgs, notif *xaction.NotifXact) bool {
	reb.stages.stage.Store(rebStageInit)
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("rebalance (v%d) in %s state", md.id, stages[rebStageInit])
	}

	xact := xreg.RenewRebalance(md.id, reb.statTracker)
	if xact == nil {
		return false
	}
	notif.Xact = xact
	xact.AddNotif(notif)

	// get EC rebalancer ready
	if md.ecUsed {
		reb.cleanupEC()
	}

	reb.setXact(xact.(*xrun.Rebalance))
	defer reb.xact().MarkDone()

	// 3. init streams and data structures
	reb.beginStats.Store(unsafe.Pointer(reb.getStats()))
	reb.beginStreams()
	reb.awaiting.targets = make(cluster.NodeMap, md.smap.CountTargets()-1)
	acks := reb.lomAcks()
	for i := 0; i < len(acks); i++ { // init lom acks
		acks[i] = &lomAcks{mu: &sync.Mutex{}, q: make(map[string]*cluster.LOM, 64)}
	}

	// 4. create persistent mark
	err := fs.PersistMarker(fs.RebalanceMarker)
	if err != nil {
		glog.Errorf("Failed to create marker: %v", err)
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(md.smap))
	reb.rebID.Store(md.id)
	reb.stages.cleanup()
	glog.Infof("%s: %s", reb.logHdr(md), reb.xact().String())
	return true
}

// when at least one bucket has EC enabled
func (reb *Manager) runEC(md *rebArgs) error {
	// Collect all local slices
	if cnt := reb.buildECNamespace(md); cnt != 0 {
		return fmt.Errorf("%d targets failed to build namespace", cnt)
	}
	// Waiting for all targets to send their lists of slices to other nodes
	if err := reb.distributeECNamespace(md); err != nil {
		return err
	}
	// Detect objects with misplaced or missing parts
	reb.generateECFixList(md)

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
func (reb *Manager) runNoEC(md *rebArgs) error {
	var (
		ver        = md.smap.Version
		multiplier = md.config.Rebalance.Multiplier
	)
	_ = reb.bcast(md, reb.rxReady) // NOTE: ignore timeout
	if reb.xact().Aborted() {
		return cmn.NewAbortedError(fmt.Sprintf("%s: aborted", reb.logHdr(md)))
	}

	wg := &sync.WaitGroup{}
	for _, mpathInfo := range md.paths {
		var sema *cmn.DynSemaphore
		if multiplier > 1 {
			sema = cmn.NewDynSemaphore(int(multiplier))
		}
		rl := &rebalanceJogger{
			joggerBase: joggerBase{m: reb, xreb: &reb.xact().RebBase, wg: wg},
			smap:       md.smap, sema: sema, ver: ver,
		}
		wg.Add(1)
		go rl.jog(mpathInfo)
	}
	wg.Wait()

	if reb.xact().Aborted() {
		return cmn.NewAbortedError(fmt.Sprintf("%s: aborted", reb.logHdr(md)))
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("finished rebalance walk (g%d)", md.id)
	}
	return nil
}

func (reb *Manager) rebWaitAck(md *rebArgs) (errCnt int) {
	reb.changeStage(rebStageWaitAck, 0)
	logHdr := reb.logHdr(md)
	sleep := md.config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated
	maxwt := md.config.Rebalance.DestRetryTime
	cnt := 0
	maxwt += time.Duration(int64(time.Minute) * int64(md.smap.CountTargets()/10))
	maxwt = cmn.MinDuration(maxwt, md.config.Rebalance.DestRetryTime*2)

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
				if reb.xact().Aborted() {
					glog.Infof("%s: abort", logHdr)
					return
				}
			}
			if cnt == 0 {
				glog.Infof("%s: received all ACKs", logHdr)
				break
			}
			glog.Warningf("%s: waiting for %d ACKs", logHdr, cnt)
			if reb.xact().AbortedAfter(sleep) {
				glog.Infof("%s: abort", logHdr)
				return
			}
			curwt += sleep
		}
		if cnt > 0 {
			glog.Warningf("%s: timed-out waiting for %d ACK(s)", logHdr, cnt)
		}
		if reb.xact().Aborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Get()
		if len(aPaths) > len(md.paths) {
			glog.Warningf("%s: mountpath changes detected (%d, %d)", logHdr, len(aPaths), len(md.paths))
		}

		// 8. synchronize
		glog.Infof("%s: poll targets for: stage=(%s or %s***)", logHdr, stages[rebStageFin], stages[rebStageWaitAck])
		errCnt = reb.bcast(md, reb.waitFinExtended)
		if reb.xact().Aborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(md)
		if cnt == 0 || reb.xact().Aborted() {
			break
		}
		glog.Warningf("%s: retransmitted %d, more wack...", logHdr, cnt)
	}

	return
}

// Waits until the following condition is true: no objects are received
// during certain configurable (quiescent) interval of time.
// if `cb` returns true the wait loop interrupts immediately. It is used,
// e.g., to wait for EC batch to finish: no need to wait until timeout if
// all targets have sent push notification that they are done with the batch.
func (reb *Manager) waitQuiesce(md *rebArgs, maxWait time.Duration, cb func(md *rebArgs) bool) (aborted bool) {
	var (
		sleep     = md.config.Timeout.CplaneOperation
		maxQuiet  = int(maxWait/sleep) + 1
		quiescent int
	)
	aborted = reb.xact().Aborted()
	for quiescent < maxQuiet && !aborted {
		if !reb.laterx.CAS(true, false) {
			quiescent++
		} else {
			quiescent = 0
		}
		if cb != nil && cb(md) {
			break
		}
		aborted = reb.xact().AbortedAfter(sleep)
	}

	return aborted
}

// Wait until cb returns `true` or times out. Waits forever if `maxWait` is
// omitted. The latter case is useful when it is unclear how much to wait.
// Return true is xaction was aborted during wait loop.
func (reb *Manager) waitEvent(md *rebArgs, cb func(md *rebArgs) bool, maxWait ...time.Duration) bool {
	var (
		sleep   = md.config.Timeout.CplaneOperation
		waited  = time.Duration(0)
		toWait  = time.Duration(0)
		aborted = reb.xact().Aborted()
	)
	if len(maxWait) != 0 {
		toWait = maxWait[0]
	}
	for !aborted {
		if cb(md) {
			break
		}
		aborted = reb.xact().AbortedAfter(sleep)
		waited += sleep
		if toWait > 0 && waited > toWait {
			break
		}
	}
	return aborted
}

func (reb *Manager) rebFini(md *rebArgs, err error) {
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("finishing rebalance (reb_args: %s)", reb.logHdr(md))
	}

	// 10.5. keep at it... (can't close the streams as long as)
	maxWait := md.config.Rebalance.Quiesce
	aborted := reb.waitQuiesce(md, maxWait, reb.nodesQuiescent)
	if !aborted {
		fs.RemoveMarker(fs.RebalanceMarker)
	}
	reb.endStreams(err)
	reb.filterGFN.Reset()

	if !reb.xact().Finished() {
		reb.xact().Finish()
	} else {
		glog.Infoln(reb.xact().String())
	}
	{
		status := &Status{}
		reb.RebStatus(status)
		delta, err := jsoniter.MarshalIndent(&status.StatsDelta, "", " ")
		if err == nil {
			glog.Infoln(string(delta))
		}
	}
	reb.stages.stage.Store(rebStageDone)

	// clean up all collected data
	if md.ecUsed {
		reb.cleanupEC()
	}

	reb.stages.cleanup()

	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("global reb (g%d) in state %s: finished", md.id, stages[rebStageDone])
	}
	reb.semaCh <- struct{}{}
}

func (reb *Manager) RespHandler(w http.ResponseWriter, r *http.Request) {
	var (
		caller     = r.Header.Get(cmn.HeaderCallerName)
		query      = r.URL.Query()
		getRebData = cmn.IsParseBool(query.Get(cmn.URLParamRebData))
	)

	if r.Method != http.MethodGet {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method "+r.Method)
		return
	}
	if !getRebData {
		cmn.InvalidHandler(w, r)
		return
	}
	rebStatus := &Status{}
	reb.RebStatus(rebStatus)

	// the target is still collecting the data, reply that the result is not ready
	if rebStatus.Stage < rebStageECDetect {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// ask rebalance manager the list of all local slices
	if slices, ok := reb.ec.nodeData(reb.t.Snode().ID()); ok {
		if err := cmn.WriteJSON(w, slices); err != nil {
			glog.Errorf("Failed to send data to %s", caller)
		}
		return
	}
	// no local slices found. It is possible if the number of object is small
	w.WriteHeader(http.StatusNoContent)
}

////////////////////////////////////
// rebalanceJogger: global non-EC //
////////////////////////////////////

func (rj *rebalanceJogger) jog(mpathInfo *fs.MountpathInfo) {
	// the jogger is running in separate goroutine, so use defer to be
	// sure that `Done` is called even if the jogger crashes to avoid hang up
	defer rj.wg.Done()

	opts := &fs.Options{
		Mpath:    mpathInfo,
		CTs:      []string{fs.ObjectType},
		Callback: rj.walk,
		Sorted:   false,
	}
	rj.m.t.Bowner().Get().Range(nil, nil, func(bck *cluster.Bck) bool {
		opts.ErrCallback = nil
		opts.Bck = bck.Bck
		if err := fs.Walk(opts); err != nil {
			if rj.xreb.Aborted() {
				glog.Infof("aborting traversal")
			} else {
				glog.Errorf("%s: failed to traverse, err: %v", rj.m.t.Snode(), err)
			}
			return true
		}
		return rj.m.xact().Aborted()
	})

	if rj.sema != nil {
		// Make sure that all sends have finished by acquiring all semaphores.
		rj.sema.Acquire(rj.sema.Size())
		rj.sema.Release(rj.sema.Size())
	}
}

func (rj *rebalanceJogger) objSentCallback(hdr transport.ObjHdr, r io.ReadCloser, lomptr unsafe.Pointer, err error) {
	var (
		lom = (*cluster.LOM)(lomptr)
		t   = rj.m.t
	)
	rj.m.inQueue.Dec()
	lom.Unlock(false) // NOTE: can unlock now

	rj.m.t.SmallMMSA().Free(hdr.Opaque)

	if err != nil {
		glog.Errorf("%s: failed to send o[%s/%s], err: %v", t.Snode(), hdr.Bck, hdr.ObjName, err)
		return
	}
	rj.m.statTracker.AddMany(
		stats.NamedVal64{Name: stats.RebTxCount, Value: 1},
		stats.NamedVal64{Name: stats.RebTxSize, Value: hdr.ObjAttrs.Size})
}

func (rj *rebalanceJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var (
		lom *cluster.LOM
		tsi *cluster.Snode
		t   = rj.m.t
	)
	if rj.xreb.Aborted() || rj.xreb.Finished() {
		return cmn.NewAbortedErrorDetails("traversal", rj.xreb.String())
	}
	if de.IsDir() {
		return nil
	}
	lom = &cluster.LOM{T: t, FQN: fqn}
	err = lom.Init(cmn.Bck{})
	if err != nil {
		if cmn.IsErrBucketLevel(err) {
			return err
		}
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Warningf("%s, err %s - skipping...", lom, err)
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
		err = rj.send(lom, tsi, true /*addAck*/)
	} else { // // rebalance.multiplier > 1
		rj.sema.Acquire()
		go func() {
			defer rj.sema.Release()
			if err := rj.send(lom, tsi, true /*addAck*/); err != nil {
				glog.Error(err)
			}
		}()
	}
	return
}

func (rj *rebalanceJogger) send(lom *cluster.LOM, tsi *cluster.Snode, addAck bool) (err error) {
	var (
		file                  *cmn.FileHandle
		cksum                 *cmn.Cksum
		cksumType, cksumValue string
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
	if cksum, err = lom.ComputeCksumIfMissing(); err != nil {
		return
	}
	cksumType, cksumValue = cksum.Get()
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		return
	}
	if addAck {
		// cache it as pending-acknowledgement (optimistically - see objSentCallback)
		rj.m.addLomAck(lom)
	}
	// transmit
	var (
		ack    = regularAck{rebID: rj.m.RebID(), daemonID: rj.m.t.Snode().ID()}
		mm     = rj.m.t.SmallMMSA()
		opaque = ack.NewPack(mm)
		o      = transport.AllocSend()
	)
	o.Hdr = transport.ObjHdr{
		Bck:     lom.Bck().Bck,
		ObjName: lom.ObjName,
		Opaque:  opaque,
		ObjAttrs: transport.ObjectAttrs{
			Size:       lom.Size(),
			Atime:      lom.AtimeUnix(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version(),
		},
	}
	o.Callback, o.CmplPtr = rj.objSentCallback, unsafe.Pointer(lom)

	rj.m.inQueue.Inc()
	if err = rj.m.dm.Send(o, file, tsi); err != nil {
		rj.m.inQueue.Dec()
		if addAck {
			rj.m.delLomAck(lom)
		}
		mm.Free(opaque)
		return
	}
	rj.m.laterx.Store(true)
	return
}
