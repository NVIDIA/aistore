// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
	"github.com/NVIDIA/aistore/xs"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

const (
	trname    = "reb"
	trnamePsh = "pshreb" // broadcast push notifications
)

// rebalance stage enum
const (
	rebStageInactive = iota
	rebStageInit
	rebStageTraverse
	rebStageWaitAck
	rebStageFin
	rebStageFinStreams
	rebStageDone
	rebStageAbort // one of targets aborts the rebalancing (never set, only sent)
)

const maxWackTargets = 4

var stages = map[uint32]string{
	rebStageInactive:   "<inactive>",
	rebStageInit:       "<init>",
	rebStageTraverse:   "<traverse>",
	rebStageWaitAck:    "<wack>",
	rebStageFin:        "<fin>",
	rebStageFinStreams: "<fin-streams>",
	rebStageDone:       "<done>",
	rebStageAbort:      "<abort>",
}

type (
	Manager struct {
		sync.RWMutex
		t           cluster.Target
		dm          *bundle.DataMover
		pushes      *bundle.Streams // broadcast notifications
		statTracker stats.Tracker
		filterGFN   *filter.Filter
		smap        atomic.Pointer // new smap which will be soon live
		lomacks     [cos.MultiSyncMapCount]*lomAcks
		awaiting    struct {
			mu      sync.Mutex
			targets cluster.Nodes // targets for which we are waiting for
			ts      int64         // last time we have recomputed
		}
		semaCh     *cos.Semaphore
		beginStats atomic.Pointer // *stats.ExtRebalanceStats
		xreb       atomic.Pointer // *xaction.Rebalance
		stages     *nodeStages
		ecClient   *http.Client
		rebID      atomic.Int64
		inQueue    atomic.Int64
		onAir      atomic.Int64
		laterx     atomic.Bool
	}
	lomAcks struct {
		mu *sync.Mutex
		q  map[string]*cluster.LOM // on the wire, waiting for ACK
	}
	joggerBase struct {
		m    *Manager
		xreb cluster.Xact
		wg   *sync.WaitGroup
	}
	rebJogger struct {
		joggerBase
		smap *cluster.Smap
		sema *cos.DynSemaphore
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

func NewManager(t cluster.Target, config *cmn.Config, st stats.Tracker) *Manager {
	ecClient := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout.D(),
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	reb := &Manager{
		t:           t,
		filterGFN:   filter.NewDefaultFilter(),
		statTracker: st,
		stages:      newNodeStages(),
		ecClient:    ecClient,
	}
	rebcfg := &config.Rebalance
	dmExtra := bundle.Extra{
		RecvAck:     reb.recvAck,
		Compression: rebcfg.Compression,
		Multiplier:  int(rebcfg.Multiplier),
	}
	dm, err := bundle.NewDataMover(t, trname, reb.recvObj, cluster.Migrated, dmExtra)
	if err != nil {
		cos.ExitLogf("%v", err)
	}
	reb.dm = dm
	reb.registerRecv()
	return reb
}

// NOTE: these receive handlers are statically present throughout: unreg never done
func (reb *Manager) registerRecv() {
	if err := reb.dm.RegRecv(); err != nil {
		cos.ExitLogf("%v", err)
	}
	if err := transport.HandleObjStream(trnamePsh, reb.recvPush); err != nil {
		cos.ExitLogf("%v", err)
	}
	// serialization: one at a time
	reb.semaCh = cos.NewSemaphore(1)
}

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
	rargs := &rebArgs{id: id, smap: smap, config: cmn.GCO.Get(), ecUsed: reb.t.Bowner().Get().IsECUsed()}
	logHdr := reb.logHdr(rargs)
	glog.Infof("%s: initializing...", logHdr)
	if !reb.rebSerialize(rargs) {
		return
	}
	if !reb.rebInitRenew(rargs, notif) {
		return
	}
	// single-active-target cluster (singleATC)
	// NOTE: compare with `rebFini`
	if smap.CountActiveTargets() == 1 {
		reb.stages.stage.Store(rebStageDone)
		reb.semaCh.Release()
		reb.dm.Close(nil)
		reb.pushes.Close(true)
		fs.RemoveMarker(cmn.RebalanceMarker)
		reb.xact().Finish(nil)
		return
	}

	// At this point only one rebalance is running so we can safely enable regular GFN.
	gfn := reb.t.GFN(cluster.GFNGlobal)
	gfn.Activate()
	defer gfn.Deactivate()

	errCnt := 0
	err := reb.run(rargs)
	if err == nil {
		errCnt = reb.rebWaitAck(rargs)
	} else {
		glog.Warning(err)
	}
	reb.changeStage(rebStageFin)
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infoln(logHdr)
	}

	for errCnt != 0 && !reb.xact().Aborted() {
		errCnt = reb.bcast(rargs, reb.waitFinExtended)
	}

	reb.rebFini(rargs, err)
}

// To optimize goroutine creation:
// 1. One bucket case just calls a single rebalance worker depending on
//    whether a bucket is erasure coded (goroutine is not used).
// 2. Multi-bucket rebalance may start both non-EC and EC in parallel.
//    It then waits until everything finishes.
func (reb *Manager) run(rargs *rebArgs) error {
	// 6. Capture stats, start mpath joggers
	reb.stages.stage.Store(rebStageTraverse)

	// No EC-enabled buckets - run only regular rebalance
	if !rargs.ecUsed {
		glog.Infof("starting global rebalance (g%d)", rargs.id)
		return reb.runNoEC(rargs)
	}

	// In all other cases run both rebalances simultaneously
	group := &errgroup.Group{}
	group.Go(func() error {
		glog.Infof("starting global rebalance (g%d)", rargs.id)
		return reb.runNoEC(rargs)
	})
	group.Go(func() error {
		glog.Infof("EC detected - starting EC rebalance (g%d)", rargs.id)
		return reb.runEC(rargs)
	})
	return group.Wait()
}

func (reb *Manager) rebSerialize(rargs *rebArgs) bool {
	// 1. check whether other targets are up and running
	if errCnt := reb.bcast(rargs, reb.pingTarget); errCnt > 0 {
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = reb.t.Sowner().Get()
	}
	// 2. serialize global rebalance (one at a time post this point)
	//    start new xaction unless the one for the current version is already in progress
	if newerRMD, alreadyRunning := reb._serialize(rargs); newerRMD || alreadyRunning {
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = reb.t.Sowner().Get()
	}
	rargs.paths, _ = fs.Get()
	return true
}

func (reb *Manager) _serialize(rargs *rebArgs) (newerRMD, alreadyRunning bool) {
	var (
		total    time.Duration
		sleep    = rargs.config.Timeout.CplaneOperation.D()
		maxTotal = cos.MaxDuration(20*sleep, 10*time.Second) // time to abort prev. streams
		maxwt    = cos.MaxDuration(rargs.config.Rebalance.DestRetryTime.D(), 2*maxTotal)
		acquired bool
	)
	for {
		select {
		case <-reb.semaCh.TryAcquire():
			acquired = true
		default:
			runtime.Gosched()
		}
		logHdr := reb.logHdr(rargs)
		if reb.rebID.Load() > rargs.id {
			glog.Warningf("%s: seeing newer rebalance[g%d] - not running", logHdr, reb.rebID.Load())
			newerRMD = true
			if acquired {
				reb.semaCh.Release()
			}
			return
		}
		if reb.rebID.Load() == rargs.id {
			if acquired {
				reb.semaCh.Release()
			}
			glog.Warningf("%s: rebalance[g%d] is already running", logHdr, rargs.id)
			alreadyRunning = true
			return
		}

		if acquired { // ok
			return
		}

		// try to preempt
		err := reb._preempt(rargs, logHdr, total, maxTotal)
		if err != nil {
			if total > maxwt {
				cos.ExitLogf("%v", err)
			}
			glog.Error(err)
		}
		time.Sleep(sleep)
		total += sleep
	}
}

func (reb *Manager) _preempt(rargs *rebArgs, logHdr string, total, maxTotal time.Duration) (err error) {
	entry := xreg.GetRunning(xreg.XactFilter{Kind: cmn.ActRebalance})
	if entry == nil {
		err = fmt.Errorf("%s: acquire/release asymmetry", logHdr)
		return
	}
	otherXreb := entry.Get().(*xs.Rebalance) // running or previous
	otherRebID, errID := xaction.S2RebID(otherXreb.ID())
	debug.AssertNoErr(errID)
	if otherRebID >= rargs.id {
		return
	}
	if !otherXreb.Aborted() {
		otherXreb.Abort(nil)
		glog.Warningf("%s: aborting older %s", logHdr, otherXreb)
		return
	}
	if total > maxTotal {
		err = fmt.Errorf("%s: preempting older %s takes too much time", logHdr, otherXreb)
		if xreb := reb.xact(); xreb != nil && xreb.ID() == otherXreb.ID() {
			debug.Assert(reb.dm.GetXact().ID() == otherXreb.ID())
			glog.Warningf("%s: aborting older streams...", logHdr)
			reb.abortStreams()
		}
	}
	return
}

func (reb *Manager) rebInitRenew(rargs *rebArgs, notif *xaction.NotifXact) bool {
	reb.stages.stage.Store(rebStageInit)
	rns := xreg.RenewRebalance(rargs.id, reb.statTracker)
	debug.AssertNoErr(rns.Err)
	if rns.IsRunning() {
		return false
	}
	xact := rns.Entry.Get()

	notif.Xact = xact
	xact.AddNotif(notif)

	reb.Lock()
	reb.setXact(xact.(*xs.Rebalance))

	// 3. init streams and data structures
	reb.beginStats.Store(unsafe.Pointer(reb.getStats()))
	reb.beginStreams()
	if reb.awaiting.targets == nil {
		reb.awaiting.targets = make(cluster.Nodes, 0, maxWackTargets)
	} else {
		reb.awaiting.targets = reb.awaiting.targets[:0]
	}
	acks := reb.lomAcks()
	for i := 0; i < len(acks); i++ { // init lom acks
		acks[i] = &lomAcks{mu: &sync.Mutex{}, q: make(map[string]*cluster.LOM, 64)}
	}

	// 4. create persistent mark
	if fatalErr, writeErr := fs.PersistMarker(cmn.RebalanceMarker); fatalErr != nil || writeErr != nil {
		err := writeErr
		if fatalErr != nil {
			err = fatalErr
		}
		reb.endStreams(err)
		xact.Abort(err)
		reb.Unlock()
		glog.Errorf("FATAL: %v, WRITE: %v", fatalErr, writeErr)
		return false
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(rargs.smap))
	reb.rebID.Store(rargs.id)
	reb.stages.cleanup()

	reb.Unlock()
	glog.Infof("%s: running %s", reb.logHdr(rargs), reb.xact())
	return true
}

func (reb *Manager) beginStreams() {
	debug.Assert(reb.stages.stage.Load() == rebStageInit)

	xreb := reb.xact()
	reb.dm.SetXact(xreb)
	reb.dm.Open()
	pushArgs := bundle.Args{Net: reb.dm.NetC(), Trname: trnamePsh, Extra: &transport.Extra{SenderID: xreb.ID()}}
	reb.pushes = bundle.NewStreams(reb.t.Sowner(), reb.t.Snode(), transport.NewIntraDataClient(), pushArgs)

	reb.laterx.Store(false)
	reb.inQueue.Store(0)
}

func (reb *Manager) abortStreams() {
	reb.dm.Abort()
	reb.pushes.Abort()
}

func (reb *Manager) endStreams(err error) {
	if reb.stages.stage.CAS(rebStageFin, rebStageFinStreams) {
		reb.dm.Close(err)
		reb.pushes.Close(true)
	}
}

// when at least one bucket has EC enabled
func (reb *Manager) runEC(rargs *rebArgs) error {
	_ = reb.bcast(rargs, reb.rxReady) // NOTE: ignore timeout
	if xreb := reb.xact(); xreb.Aborted() {
		return cmn.NewErrAborted(xreb.Name(), "reb-run-ec", nil)
	}

	reb.runECjoggers()

	if xreb := reb.xact(); xreb.Aborted() {
		return cmn.NewErrAborted(xreb.Name(), "reb-run-ec", nil)
	}
	glog.Infof("[%s] RebalanceEC done", reb.t.SID())
	return nil
}

// when no bucket has EC enabled
func (reb *Manager) runNoEC(rargs *rebArgs) error {
	var (
		ver        = rargs.smap.Version
		multiplier = rargs.config.Rebalance.Multiplier
	)
	_ = reb.bcast(rargs, reb.rxReady) // NOTE: ignore timeout
	if xreb := reb.xact(); xreb.Aborted() {
		return cmn.NewErrAborted(xreb.Name(), "reb-run", nil)
	}

	wg := &sync.WaitGroup{}
	for _, mpathInfo := range rargs.paths {
		var sema *cos.DynSemaphore
		if multiplier > 1 {
			sema = cos.NewDynSemaphore(int(multiplier))
		}
		rl := &rebJogger{
			joggerBase: joggerBase{m: reb, xreb: reb.xact(), wg: wg},
			smap:       rargs.smap, sema: sema, ver: ver,
		}
		wg.Add(1)
		go rl.jog(mpathInfo)
	}
	wg.Wait()

	if xreb := reb.xact(); xreb.Aborted() {
		return cmn.NewErrAborted(xreb.Name(), "reb-run", nil)
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("finished rebalance walk (g%d)", rargs.id)
	}
	return nil
}

func (reb *Manager) rebWaitAck(rargs *rebArgs) (errCnt int) {
	reb.changeStage(rebStageWaitAck)
	logHdr := reb.logHdr(rargs)
	sleep := rargs.config.Timeout.CplaneOperation.D() // NOTE: TODO: used throughout; must be separately assigned and calibrated
	maxwt := rargs.config.Rebalance.DestRetryTime.D()
	cnt := 0
	maxwt += time.Duration(int64(time.Minute) * int64(rargs.smap.CountTargets()/10))
	maxwt = cos.MinDuration(maxwt, rargs.config.Rebalance.DestRetryTime.D()*2)

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
							tsi, err := cluster.HrwTarget(lom.Uname(), rargs.smap)
							if err == nil {
								glog.Infof("waiting for %s ACK from %s", lom, tsi.StringEx())
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
			glog.Warningf("%s: timed out waiting for %d ACK(s)", logHdr, cnt)
		}
		if reb.xact().Aborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Get()
		if len(aPaths) > len(rargs.paths) {
			glog.Warningf("%s: mountpath changes detected (%d, %d)", logHdr, len(aPaths), len(rargs.paths))
		}

		// 8. synchronize
		glog.Infof("%s: poll targets for: stage=(%s or %s***)", logHdr, stages[rebStageFin], stages[rebStageWaitAck])
		errCnt = reb.bcast(rargs, reb.waitFinExtended)
		if reb.xact().Aborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(rargs)
		if cnt == 0 || reb.xact().Aborted() {
			break
		}
		glog.Warningf("%s: retransmitted %d, more wack...", logHdr, cnt)
	}

	return
}

func (reb *Manager) retransmit(rargs *rebArgs) (cnt int) {
	aborted := func() (yes bool) {
		yes = reb.xact().Aborted()
		yes = yes || (rargs.smap.Version != reb.t.Sowner().Get().Version)
		return
	}
	if aborted() {
		return
	}
	var (
		rj = &rebJogger{joggerBase: joggerBase{
			m: reb, xreb: reb.xact(),
			wg: &sync.WaitGroup{},
		}, smap: rargs.smap}
		query  = url.Values{}
		loghdr = reb.logHdr(rargs)
	)
	query.Add(cmn.URLParamSilent, "true")
	for _, lomAck := range reb.lomAcks() {
		lomAck.mu.Lock()
		for uname, lom := range lomAck.q {
			if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
				if cmn.IsObjNotExist(err) {
					glog.Warningf("%s: object not found (lom: %s, err: %v)", loghdr, lom, err)
				} else {
					glog.Errorf("%s: failed loading %s, err: %s", loghdr, lom, err)
				}
				delete(lomAck.q, uname)
				continue
			}
			tsi, _ := cluster.HrwTarget(lom.Uname(), rargs.smap)
			if reb.t.LookupRemoteSingle(lom, tsi) {
				if glog.FastV(4, glog.SmoduleReb) {
					glog.Infof("%s: HEAD ok %s at %s", loghdr, lom, tsi.StringEx())
				}
				delete(lomAck.q, uname)
				continue
			}
			// retransmit
			if roc, err := _prepSend(lom); err != nil {
				glog.Errorf("%s: failed to retransmit %s => %s: %v", loghdr, lom, tsi.StringEx(), err)
			} else {
				rj.doSend(lom, tsi, roc)
				glog.Warningf("%s: retransmitting %s => %s", loghdr, lom, tsi.StringEx())
				cnt++
			}
			if aborted() {
				lomAck.mu.Unlock()
				return 0
			}
		}
		lomAck.mu.Unlock()
		if aborted() {
			return 0
		}
	}
	return
}

func (reb *Manager) rebFini(rargs *rebArgs, err error) {
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("finishing rebalance (reb_args: %s)", reb.logHdr(rargs))
	}

	// prior to closing the streams
	if q := reb.quiesce(rargs, rargs.config.Rebalance.Quiesce.D(), reb.nodesQuiescent); q != cluster.QuiAborted {
		fs.RemoveMarker(cmn.RebalanceMarker)
	}
	reb.endStreams(err)
	reb.filterGFN.Reset()

	{
		status := &Status{}
		reb.RebStatus(status)
		delta, err := jsoniter.MarshalIndent(&status.StatsDelta, "", " ")
		if err == nil {
			glog.Infoln(string(delta))
		}
	}
	reb.stages.stage.Store(rebStageDone)
	reb.stages.cleanup()

	reb.semaCh.Release()

	if !reb.xact().Finished() {
		reb.xact().Finish(nil)
	} else {
		glog.Infoln(reb.xact().String())
	}
}

//////////////////////////////
// rebJogger: global non-EC //
//////////////////////////////

func (rj *rebJogger) jog(mpathInfo *fs.MountpathInfo) {
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

// send completion
func (rj *rebJogger) objSentCallback(hdr transport.ObjHdr, _ io.ReadCloser, arg interface{}, err error) {
	rj.m.inQueue.Dec()
	if err != nil {
		rj.m.delLomAck(arg.(*cluster.LOM))
		if bool(glog.FastV(4, glog.SmoduleReb)) || !cos.IsRetriableConnErr(err) {
			si := rj.m.t.Snode()
			glog.Errorf("%s: failed to send o[%s]: %v", si, hdr.FullName(), err)
		}
		return
	}
	rj.m.statTracker.AddMany(
		stats.NamedVal64{Name: stats.RebTxCount, Value: 1},
		stats.NamedVal64{Name: stats.RebTxSize, Value: hdr.ObjAttrs.Size},
	)
}

func (rj *rebJogger) walk(fqn string, de fs.DirEntry) (err error) {
	if rj.xreb.Aborted() {
		return cmn.NewErrAborted(rj.xreb.Name(), "jog", nil)
	}
	if de.IsDir() {
		return nil
	}
	lom := cluster.AllocLOMbyFQN(fqn) // NOTE: free on error or via (send => ... => delLomAck) path
	err = rj._lwalk(lom)
	if err != nil {
		cluster.FreeLOM(lom)
		if err == cmn.ErrSkip {
			err = nil
		}
	}
	return
}

func (rj *rebJogger) _lwalk(lom *cluster.LOM) (err error) {
	err = lom.Init(cmn.Bck{})
	if err != nil {
		if cmn.IsErrBucketLevel(err) {
			return err
		}
		return cmn.ErrSkip
	}
	// skip EC.Enabled bucket - the job for EC rebalance
	if lom.Bck().Props.EC.Enabled {
		return filepath.SkipDir
	}
	var tsi *cluster.Snode
	tsi, err = cluster.HrwTarget(lom.Uname(), rj.smap)
	if err != nil {
		return err
	}
	if tsi.ID() == rj.m.t.SID() {
		return cmn.ErrSkip
	}

	// skip objects that were already sent via GFN (due to probabilistic filtering
	// false-positives, albeit rare, are still possible)
	uname := []byte(lom.Uname())
	if rj.m.filterGFN.Lookup(uname) {
		rj.m.filterGFN.Delete(uname) // it will not be used anymore
		return cmn.ErrSkip
	}
	// prepare to send
	var roc cos.ReadOpenCloser
	if roc, err = _prepSend(lom); err != nil {
		return
	}
	// transmit
	if rj.sema == nil {
		rj.m.addLomAck(lom)
		rj.doSend(lom, tsi, roc)
	} else { // rebalance.multiplier > 1
		rj.sema.Acquire()
		rj.m.addLomAck(lom)
		go func() {
			rj.doSend(lom, tsi, roc)
			rj.sema.Release()
		}()
	}
	return
}

func _prepSend(lom *cluster.LOM) (roc cos.ReadOpenCloser, err error) {
	clone := lom.Clone(lom.FQN)
	lom.Lock(false)
	if err = lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		goto retErr
	}
	if lom.IsCopy() {
		err = cmn.ErrSkip
		goto retErr
	}
	if _, err = lom.ComputeCksumIfMissing(); err != nil { // not persisting it locally
		goto retErr
	}
	if roc, err = cos.NewFileHandle(lom.FQN); err != nil {
		roc = nil
		goto retErr
	}
	roc = cos.NewDeferROC(roc, func() {
		clone.Unlock(false)
		cluster.FreeLOM(clone)
	})
	return

retErr:
	lom.Unlock(false)
	cluster.FreeLOM(clone)
	return
}

func (rj *rebJogger) doSend(lom *cluster.LOM, tsi *cluster.Snode, roc cos.ReadOpenCloser) {
	var (
		ack    = regularAck{rebID: rj.m.RebID(), daemonID: rj.m.t.SID()}
		o      = transport.AllocSend()
		opaque = ack.NewPack()
	)
	o.Hdr.Bck = lom.Bucket()
	o.Hdr.ObjName = lom.ObjName
	o.Hdr.Opaque = opaque
	o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs())
	o.Callback, o.CmplArg = rj.objSentCallback, lom
	rj.m.inQueue.Inc()
	rj.m.dm.Send(o, roc, tsi)
}
