// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

const (
	trname    = "reb"
	trnamePsh = "pshreb" // broadcast stage notifications
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

const fmtpend = "%s: newer rebalance[g%d] pending - not running"

type (
	Reb struct {
		smap      ratomic.Pointer[meta.Smap] // next smap (new that'll become current after rebalance)
		xreb      ratomic.Pointer[xs.Rebalance]
		dm        *bundle.DataMover
		pushes    *bundle.Streams // broadcast notifications
		filterGFN *prob.Filter
		semaCh    *cos.Semaphore
		ecClient  *http.Client
		stages    *nodeStages
		lomacks   [cos.MultiSyncMapCount]*lomAcks
		awaiting  struct {
			targets meta.Nodes // targets for which we are waiting for
			ts      int64      // last time we have recomputed
			mtx     sync.Mutex
		}
		// (smap, xreb) + atomic state
		rebID   atomic.Int64
		nxtID   atomic.Int64
		inQueue atomic.Int64
		onAir   atomic.Int64
		mu      sync.RWMutex
		laterx  atomic.Bool
	}
	lomAcks struct {
		mu *sync.Mutex
		q  map[string]*core.LOM // on the wire, waiting for ACK
	}
	joggerBase struct {
		m    *Reb
		xreb *xs.Rebalance
		wg   *sync.WaitGroup
	}
	rebJogger struct {
		joggerBase
		smap *meta.Smap
		opts fs.WalkOpts
		ver  int64
	}
	rebArgs struct {
		smap   *meta.Smap
		config *cmn.Config
		apaths fs.MPI
		id     int64
		ecUsed bool
	}
)

func New(config *cmn.Config) *Reb {
	var (
		reb = &Reb{
			filterGFN: prob.NewDefaultFilter(),
			stages:    newNodeStages(),
		}
		cargs = cmn.TransportArgs{Timeout: config.Client.Timeout.D()}
	)
	if config.Net.HTTP.UseHTTPS {
		reb.ecClient = cmn.NewIntraClientTLS(cargs, config)
	} else {
		reb.ecClient = cmn.NewClient(cargs)
	}
	dmExtra := bundle.Extra{
		RecvAck:     reb.recvAck,
		Config:      config,
		Compression: config.Rebalance.Compression,
		Multiplier:  config.Rebalance.SbundleMult,
	}
	dm, err := bundle.NewDataMover(trname, reb.recvObj, cmn.OwtRebalance, dmExtra)
	if err != nil {
		cos.ExitLog(err)
	}
	debug.Assert(dm != nil)
	reb.dm = dm

	// serialize one global rebalance at a time
	reb.semaCh = cos.NewSemaphore(1)
	return reb
}

func (reb *Reb) regRecv() {
	if err := reb.dm.RegRecv(); err != nil {
		cos.ExitLog(err)
	}
	if err := transport.Handle(trnamePsh, reb.recvStageNtfn /*RecvObj*/); err != nil {
		cos.ExitLog(err)
	}
}

func (reb *Reb) unregRecv() {
	reb.dm.UnregRecv()
	err := transport.Unhandle(trnamePsh)
	debug.AssertNoErr(err)
}

// run sequence: non-EC and EC global
//
// main method: serialized to run one at a time and goes through controlled enumerated stages
// A note on stage management:
//  1. Non-EC and EC rebalances run in parallel
//  2. Execution starts after the `Reb` sets the current stage to rebStageTraverse
//  3. Only EC rebalance changes the current stage
//  4. Global rebalance performs checks such as `stage > rebStageTraverse` or
//     `stage < rebStageWaitAck`. Since all EC stages are between
//     `Traverse` and `WaitAck` non-EC rebalance does not "notice" stage changes.
func (reb *Reb) RunRebalance(smap *meta.Smap, id int64, notif *xact.NotifXact, tstats cos.StatsUpdater) {
	if reb.nxtID.Load() >= id {
		return
	}
	reb.mu.Lock()
	if reb.nxtID.Load() >= id {
		reb.mu.Unlock()
		return
	}
	debug.Assert(id > reb.rebID.Load())
	reb.nxtID.Store(id)
	reb.mu.Unlock()

	logHdr := reb.logHdr(id, smap, true /*initializing*/)
	nlog.Infoln(logHdr + ": initializing")

	bmd := core.T.Bowner().Get()
	rargs := &rebArgs{id: id, smap: smap, config: cmn.GCO.Get(), ecUsed: bmd.IsECUsed()}
	if !reb.serialize(rargs, logHdr) {
		return
	}

	reb.regRecv()

	haveStreams := smap.HasActiveTs(core.T.SID())
	if bmd.IsEmpty() {
		haveStreams = false
	}
	if !reb.initRenew(rargs, notif, logHdr, haveStreams) {
		reb.unregRecv()
		reb.semaCh.Release()
		return
	}
	if !haveStreams {
		// cleanup and leave
		nlog.Infof("%s: nothing to do: %s, %s", logHdr, smap.StringEx(), bmd.StringEx())
		reb.stages.stage.Store(rebStageDone)
		reb.unregRecv()
		reb.semaCh.Release()
		fs.RemoveMarker(fname.RebalanceMarker)
		fs.RemoveMarker(fname.NodeRestartedPrev)
		reb.xctn().Finish()
		return
	}

	// abort all running `dtor.AbortRebRes` xactions (download, dsort, etl)
	xreg.AbortByNewReb(errors.New("reason: starting " + reb.xctn().Name()))

	// At this point, only one rebalance is running

	onGFN()

	tstats.Flag(stats.NodeStateFlags, cos.Rebalancing, true)

	errCnt := 0
	err := reb.run(rargs)
	if err == nil {
		errCnt = reb.rebWaitAck(rargs)
	} else {
		nlog.Warningln(err)
	}
	reb.changeStage(rebStageFin)

	for errCnt != 0 && !reb.xctn().IsAborted() {
		errCnt = bcast(rargs, reb.waitFinExtended)
	}

	reb.fini(rargs, logHdr, err)
	tstats.Flag(stats.NodeStateFlags, cos.Rebalancing, false)

	offGFN()
}

// To optimize goroutine creation:
//  1. One bucket case just calls a single rebalance worker depending on
//     whether a bucket is erasure coded (goroutine is not used).
//  2. Multi-bucket rebalance may start both non-EC and EC in parallel.
//     It then waits until everything finishes.
func (reb *Reb) run(rargs *rebArgs) error {
	// 6. Capture stats, start mpath joggers
	reb.stages.stage.Store(rebStageTraverse)

	// No EC-enabled buckets - run only regular rebalance
	if !rargs.ecUsed {
		nlog.Infof("starting g%d", rargs.id)
		return reb.runNoEC(rargs)
	}

	// In all other cases run both rebalances simultaneously
	group := &errgroup.Group{}
	group.Go(func() error {
		nlog.Infof("starting non-EC g%d", rargs.id)
		return reb.runNoEC(rargs)
	})
	group.Go(func() error {
		nlog.Infof("starting EC g%d", rargs.id)
		return reb.runEC(rargs)
	})
	return group.Wait()
}

func (reb *Reb) serialize(rargs *rebArgs, logHdr string) bool {
	// 1. check whether other targets are up and running
	if errCnt := bcast(rargs, reb.pingTarget); errCnt > 0 {
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = core.T.Sowner().Get()
	}
	// 2. serialize global rebalance and start new xaction -
	//    but only if the one that handles the current version is _not_ already in progress
	if newerRMD, alreadyRunning := reb.acquire(rargs, logHdr); newerRMD || alreadyRunning {
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = core.T.Sowner().Get()
	}
	rargs.apaths = fs.GetAvail()
	return true
}

func (reb *Reb) acquire(rargs *rebArgs, logHdr string) (newerRMD, alreadyRunning bool) {
	var (
		total    time.Duration
		sleep    = rargs.config.Timeout.CplaneOperation.D()
		maxTotal = max(20*sleep, 10*time.Second) // time to abort prev. streams
		maxwt    = max(rargs.config.Rebalance.DestRetryTime.D(), 2*maxTotal)
		errcnt   int
		acquired bool
	)
	for {
		select {
		case <-reb.semaCh.TryAcquire():
			acquired = true
		default:
			runtime.Gosched()
		}
		if id := reb.nxtID.Load(); id > rargs.id {
			nlog.Warningf(fmtpend, logHdr, id)
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
			nlog.Warningf("%s: rebalance[g%d] is already running", logHdr, rargs.id)
			alreadyRunning = true
			return
		}

		if acquired { // ok
			if errcnt > 1 {
				nlog.Infof("%s: resolved (%d)", logHdr, errcnt)
			}
			return
		}

		// try to preempt
		err := reb._preempt(rargs, logHdr, total, maxTotal, errcnt)
		if err != nil {
			if total > maxwt {
				cos.ExitLog(err)
			}
			errcnt++
		}
		time.Sleep(sleep)
		total += sleep
	}
}

func (reb *Reb) _preempt(rargs *rebArgs, logHdr string, total, maxTotal time.Duration, errcnt int) (err error) {
	entry := xreg.GetRunning(xreg.Flt{Kind: apc.ActRebalance})
	if entry == nil {
		var (
			rebID   = reb.RebID()
			rsmap   = reb.smap.Load()
			rlogHdr = reb.logHdr(rebID, rsmap, true)
			xreb    = reb.xctn()
			s       string
		)
		if xreb != nil {
			s = ", " + xreb.String()
		}
		err = fmt.Errorf("%s: acquire/release asymmetry vs %s%s", logHdr, rlogHdr, s)
		if errcnt%2 == 1 {
			nlog.Errorln(err)
		}
		return
	}
	otherXreb := entry.Get().(*xs.Rebalance) // running or previous
	otherRebID := otherXreb.RebID()
	if otherRebID >= rargs.id {
		return
	}
	if !otherXreb.IsAborted() {
		otherXreb.Abort(cmn.ErrXactRenewAbort)
		nlog.Warningf("%s: aborting older %s", logHdr, otherXreb)
		return
	}
	if total > maxTotal {
		err = fmt.Errorf("%s: preempting older %s takes too much time", logHdr, otherXreb)
		nlog.Errorln(err)
		if xreb := reb.xctn(); xreb != nil && xreb.ID() == otherXreb.ID() {
			debug.Assert(reb.dm.GetXact().ID() == otherXreb.ID())
			nlog.Warningf("%s: aborting older streams...", logHdr)
			reb.abortStreams()
		}
	}
	return
}

func (reb *Reb) initRenew(rargs *rebArgs, notif *xact.NotifXact, logHdr string, haveStreams bool) bool {
	if id := reb.nxtID.Load(); id > rargs.id {
		nlog.Warningf(fmtpend, logHdr, id)
		return false
	}
	rns := xreg.RenewRebalance(rargs.id)
	debug.AssertNoErr(rns.Err)
	if rns.IsRunning() {
		return false
	}
	xctn := rns.Entry.Get()

	notif.Xact = xctn
	xctn.AddNotif(notif)

	reb.mu.Lock()
	if id := reb.nxtID.Load(); id > rargs.id {
		reb.mu.Unlock()
		nlog.Warningf(fmtpend, logHdr, id)
		return false
	}
	reb.stages.stage.Store(rebStageInit)
	xreb := xctn.(*xs.Rebalance)
	reb.setXact(xreb)
	reb.rebID.Store(rargs.id)

	// check Smap _prior_ to opening streams
	smap := core.T.Sowner().Get()
	if smap.Version != rargs.smap.Version {
		debug.Assert(smap.Version > rargs.smap.Version)
		nlog.Errorf("Warning %s: %s post-init version change %s => %s", core.T, xreb, rargs.smap, smap)
		// TODO: handle an unlikely corner case keeping in mind that not every change warants a different rebalance
	}

	// 3. init streams and data structures
	if haveStreams {
		reb.beginStreams(rargs.config)
	}

	if reb.awaiting.targets == nil {
		reb.awaiting.targets = make(meta.Nodes, 0, maxWackTargets)
	} else {
		reb.awaiting.targets = reb.awaiting.targets[:0]
	}
	acks := reb.lomAcks()
	for i := range len(acks) { // init lom acks
		acks[i] = &lomAcks{mu: &sync.Mutex{}, q: make(map[string]*core.LOM, 64)}
	}

	// 4. create persistent mark
	if fatalErr, writeErr := fs.PersistMarker(fname.RebalanceMarker); fatalErr != nil || writeErr != nil {
		err := writeErr
		if fatalErr != nil {
			err = fatalErr
		}
		reb.endStreams(err)
		xctn.Abort(err)
		reb.mu.Unlock()
		nlog.Errorf("FATAL: %v, WRITE: %v", fatalErr, writeErr)
		return false
	}

	// 5. ready - can receive objects
	reb.smap.Store(rargs.smap)
	reb.stages.cleanup()

	reb.mu.Unlock()
	nlog.Infof("%s: running %s", reb.logHdr(rargs.id, rargs.smap), reb.xctn())
	return true
}

func (reb *Reb) beginStreams(config *cmn.Config) {
	debug.Assert(reb.stages.stage.Load() == rebStageInit)

	xreb := reb.xctn()
	reb.dm.SetXact(xreb)
	reb.dm.Open()
	pushArgs := bundle.Args{
		Net:        reb.dm.NetC(),
		Trname:     trnamePsh,
		Multiplier: config.Rebalance.SbundleMult,
		Extra:      &transport.Extra{SenderID: xreb.ID(), Config: config},
	}
	reb.pushes = bundle.New(transport.NewIntraDataClient(), pushArgs)

	reb.laterx.Store(false)
	reb.inQueue.Store(0)
}

func (reb *Reb) abortStreams() {
	reb.dm.Abort()
	reb.pushes.Abort()
}

func (reb *Reb) endStreams(err error) {
	if reb.stages.stage.CAS(rebStageFin, rebStageFinStreams) {
		reb.dm.Close(err)
		reb.pushes.Close(true)
	}
}

// when at least one bucket has EC enabled
func (reb *Reb) runEC(rargs *rebArgs) error {
	errCnt := bcast(rargs, reb.rxReady) // ignore timeout
	xreb := reb.xctn()
	if err := xreb.AbortErr(); err != nil {
		logHdr := reb.logHdr(rargs.id, rargs.smap)
		nlog.Infoln(logHdr, "abort ec rx-ready", err, "num-fail", errCnt)
		return err
	}
	if errCnt > 0 {
		logHdr := reb.logHdr(rargs.id, rargs.smap)
		nlog.Errorln(logHdr, "ec rx-ready num-fail", errCnt) // unlikely
	}

	reb.runECjoggers()

	if err := xreb.AbortErr(); err != nil {
		logHdr := reb.logHdr(rargs.id, rargs.smap)
		nlog.Infoln(logHdr, "abort ec-joggers", err)
		return err
	}
	nlog.Infof("[%s] RebalanceEC done", core.T.SID())
	return nil
}

// when not a single bucket has EC enabled
func (reb *Reb) runNoEC(rargs *rebArgs) error {
	errCnt := bcast(rargs, reb.rxReady) // ignore timeout
	xreb := reb.xctn()
	if err := xreb.AbortErr(); err != nil {
		logHdr := reb.logHdr(rargs.id, rargs.smap)
		nlog.Infoln(logHdr, "abort rx-ready", err, "num-fail", errCnt)
		return err
	}
	if errCnt > 0 {
		logHdr := reb.logHdr(rargs.id, rargs.smap)
		nlog.Errorln(logHdr, "rx-ready num-fail", errCnt) // unlikely
	}

	wg := &sync.WaitGroup{}
	ver := rargs.smap.Version
	for _, mi := range rargs.apaths {
		rl := &rebJogger{
			joggerBase: joggerBase{m: reb, xreb: reb.xctn(), wg: wg},
			smap:       rargs.smap, ver: ver,
		}
		wg.Add(1)
		go rl.jog(mi)
	}
	wg.Wait()

	if err := xreb.AbortErr(); err != nil {
		logHdr := reb.logHdr(rargs.id, rargs.smap)
		nlog.Infoln(logHdr, "abort joggers", err)
		return err
	}
	if cmn.Rom.FastV(4, cos.SmoduleReb) {
		nlog.Infof("finished rebalance walk (g%d)", rargs.id)
	}
	return nil
}

func (reb *Reb) rebWaitAck(rargs *rebArgs) (errCnt int) {
	var (
		cnt    int
		logHdr = reb.logHdr(rargs.id, rargs.smap)
		sleep  = rargs.config.Timeout.CplaneOperation.D()
		maxwt  = rargs.config.Rebalance.DestRetryTime.D()
		xreb   = reb.xctn()
		smap   = rargs.smap
	)
	maxwt += time.Duration(int64(time.Minute) * int64(rargs.smap.CountTargets()/10))
	maxwt = min(maxwt, rargs.config.Rebalance.DestRetryTime.D()*2)
	reb.changeStage(rebStageWaitAck)

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
							tsi, err := smap.HrwHash2T(lom.Digest())
							if err == nil {
								nlog.Infof("waiting for %s ACK from %s", lom, tsi.StringEx())
								logged = true
								break
							}
						}
					}
				}
				lomack.mu.Unlock()
				if err := xreb.AbortErr(); err != nil {
					nlog.Infof("%s: abort wait-ack (%v)", logHdr, err)
					return
				}
			}
			if cnt == 0 {
				nlog.Infof("%s: received all ACKs", logHdr)
				break
			}
			nlog.Warningf("%s: waiting for %d ACKs", logHdr, cnt)
			if err := xreb.AbortedAfter(sleep); err != nil {
				nlog.Infof("%s: abort wait-ack (%v)", logHdr, err)
				return
			}

			curwt += sleep
		}
		if cnt > 0 {
			nlog.Warningf("%s: timed out waiting for %d ACK%s", logHdr, cnt, cos.Plural(cnt))
		}
		if xreb.IsAborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Get()
		if len(aPaths) > len(rargs.apaths) {
			nlog.Warningf("%s: mountpath changes detected (%d, %d)", logHdr, len(aPaths), len(rargs.apaths))
		}

		// 8. synchronize
		nlog.Infof("%s: poll targets for: stage=(%s or %s***)", logHdr, stages[rebStageFin], stages[rebStageWaitAck])
		errCnt = bcast(rargs, reb.waitFinExtended)
		if xreb.IsAborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(rargs, xreb)
		if cnt == 0 || reb.xctn().IsAborted() {
			break
		}
		nlog.Warningf("%s: retransmitted %d, more wack...", logHdr, cnt)
	}

	return
}

func (reb *Reb) retransmit(rargs *rebArgs, xreb *xs.Rebalance) (cnt int) {
	if reb._aborted(rargs) {
		return
	}
	var (
		rj = &rebJogger{joggerBase: joggerBase{
			m: reb, xreb: reb.xctn(),
			wg: &sync.WaitGroup{},
		}, smap: rargs.smap}
		loghdr = reb.logHdr(rargs.id, rargs.smap)
	)
	for _, lomAck := range reb.lomAcks() {
		lomAck.mu.Lock()
		for uname, lom := range lomAck.q {
			if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
				if cos.IsNotExist(err, 0) {
					if cmn.Rom.FastV(5, cos.SmoduleReb) {
						nlog.Infoln(loghdr, lom.Cname(), "not found")
					}
				} else {
					err = cmn.NewErrFailedTo(core.T, "load", lom.Cname(), err)
					rj.xreb.AddErr(err)
				}
				delete(lomAck.q, uname)
				continue
			}
			tsi, _ := rargs.smap.HrwHash2T(lom.Digest())
			if core.T.HeadObjT2T(lom, tsi) {
				if cmn.Rom.FastV(4, cos.SmoduleReb) {
					nlog.Infof("%s: HEAD ok %s at %s", loghdr, lom, tsi.StringEx())
				}
				delete(lomAck.q, uname)
				continue
			}
			// retransmit
			roc, err := _getReader(lom)
			if err == nil {
				err = rj.doSend(lom, tsi, roc)
			}
			if err == nil {
				if cmn.Rom.FastV(4, cos.SmoduleReb) {
					nlog.Infof("%s: retransmit %s => %s", loghdr, lom, tsi.StringEx())
				}
				cnt++
			} else {
				if cmn.IsErrStreamTerminated(err) {
					xreb.Abort(err)
					nlog.Errorf("%s: stream term-ed (%v)", loghdr, err)
				} else {
					err = fmt.Errorf("%s: failed to retransmit %s => %s: %w", loghdr, lom, tsi.StringEx(), err)
					rj.xreb.AddErr(err)
				}
			}
			if reb._aborted(rargs) {
				lomAck.mu.Unlock()
				return 0
			}
		}
		lomAck.mu.Unlock()
		if reb._aborted(rargs) {
			return 0
		}
	}
	return
}

func (reb *Reb) _aborted(rargs *rebArgs) (yes bool) {
	yes = reb.xctn().IsAborted()
	yes = yes || (rargs.smap.Version != core.T.Sowner().Get().Version)
	return
}

func (reb *Reb) fini(rargs *rebArgs, logHdr string, err error) {
	var stats core.Stats
	if cmn.Rom.FastV(4, cos.SmoduleReb) {
		nlog.Infof("finishing rebalance (reb_args: %s)", reb.logHdr(rargs.id, rargs.smap))
	}
	// prior to closing the streams
	if q := reb.quiesce(rargs, rargs.config.Transport.QuiesceTime.D(), reb.nodesQuiescent); q != core.QuiAborted {
		if errM := fs.RemoveMarker(fname.RebalanceMarker); errM == nil {
			nlog.Infof("%s: %s removed marker ok", core.T, reb.xctn())
		}
		_ = fs.RemoveMarker(fname.NodeRestartedPrev)
	}
	reb.endStreams(err)
	reb.filterGFN.Reset()
	xreb := reb.xctn()
	xreb.ToStats(&stats)
	if stats.Objs > 0 || stats.OutObjs > 0 || stats.InObjs > 0 {
		s, e := jsoniter.MarshalIndent(&stats, "", " ")
		debug.AssertNoErr(e)
		nlog.Infoln(string(s))
	}
	reb.stages.stage.Store(rebStageDone)
	reb.stages.cleanup()

	reb.unregRecv()
	reb.semaCh.Release()
	if !xreb.Finished() {
		xreb.Finish()
	}
	nlog.Infof("%s: done (%s)", logHdr, xreb)
}

//////////////////////////////
// rebJogger: global non-EC //
//////////////////////////////

func (rj *rebJogger) jog(mi *fs.Mountpath) {
	// the jogger is running in separate goroutine, so use defer to be
	// sure that `Done` is called even if the jogger crashes to avoid hang up
	defer rj.wg.Done()
	{
		rj.opts.Mi = mi
		rj.opts.CTs = []string{fs.ObjectType}
		rj.opts.Callback = rj.visitObj
		rj.opts.Sorted = false
	}
	bmd := core.T.Bowner().Get()
	bmd.Range(nil, nil, rj.walkBck)
}

func (rj *rebJogger) walkBck(bck *meta.Bck) bool {
	rj.opts.Bck.Copy(bck.Bucket())
	err := fs.Walk(&rj.opts)
	if err == nil {
		return rj.xreb.IsAborted()
	}
	if rj.xreb.IsAborted() {
		nlog.Infoln(rj.xreb.Name(), "aborting traversal")
	} else {
		nlog.Errorln(core.T.String(), rj.xreb.Name(), "failed to traverse", err)
	}
	return true
}

// send completion
func (rj *rebJogger) objSentCallback(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	rj.m.inQueue.Dec()
	if err == nil {
		rj.xreb.OutObjsAdd(1, hdr.ObjAttrs.Size) // NOTE: double-counts retransmissions
		return
	}
	// log err
	if cmn.Rom.FastV(4, cos.SmoduleReb) || !cos.IsRetriableConnErr(err) {
		if bundle.IsErrDestinationMissing(err) {
			nlog.Errorf("%s: %v, %s", rj.xreb.Name(), err, rj.smap)
		} else {
			lom, ok := arg.(*core.LOM)
			debug.Assert(ok)
			nlog.Errorf("%s: %s failed to send %s: %v", core.T, rj.xreb.Name(), lom, err)
		}
	}
}

func (rj *rebJogger) visitObj(fqn string, de fs.DirEntry) error {
	if err := rj.xreb.AbortErr(); err != nil {
		nlog.Infoln(rj.xreb.Name(), "rj-walk-visit aborted", err)
		return err
	}
	if de.IsDir() {
		return nil
	}
	lom := core.AllocLOM(fqn)
	err := rj._lwalk(lom, fqn)
	if err != nil {
		core.FreeLOM(lom)
		if err == cmn.ErrSkip {
			err = nil
		}
	}
	return err
}

func (rj *rebJogger) _lwalk(lom *core.LOM, fqn string) error {
	if err := lom.InitFQN(fqn, nil); err != nil {
		if cmn.IsErrBucketLevel(err) {
			return err
		}
		return cmn.ErrSkip
	}
	// skip EC.Enabled bucket - leave the job for EC rebalance
	if lom.ECEnabled() {
		return filepath.SkipDir
	}
	tsi, err := rj.smap.HrwHash2T(lom.Digest())
	if err != nil {
		return err
	}
	if tsi.ID() == core.T.SID() {
		return cmn.ErrSkip
	}

	// skip objects that were already sent via GFN (due to probabilistic filtering
	// false-positives, albeit rare, are still possible)
	uname := cos.UnsafeB(lom.Uname())
	if rj.m.filterGFN.Lookup(uname) {
		rj.m.filterGFN.Delete(uname)
		return cmn.ErrSkip
	}
	// prepare to send: rlock, load, new roc
	var roc cos.ReadOpenCloser
	if roc, err = _getReader(lom); err != nil {
		return err
	}

	// transmit (unlock via transport completion => roc.Close)
	rj.m.addLomAck(lom)
	if err := rj.doSend(lom, tsi, roc); err != nil {
		rj.m.delLomAck(lom, 0, false /*free LOM*/)
		return err
	}

	return nil
}

// takes rlock and keeps it _iff_ successful
func _getReader(lom *core.LOM) (roc cos.ReadOpenCloser, err error) {
	lom.Lock(false)
	if err = lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		lom.Unlock(false)
		return
	}
	if lom.IsCopy() {
		lom.Unlock(false)
		err = cmn.ErrSkip
		return
	}
	if lom.Checksum() == nil {
		if _, err = lom.ComputeSetCksum(); err != nil {
			lom.Unlock(false)
			return
		}
	}
	debug.Assert(lom.Checksum() != nil, lom.String())
	return lom.NewDeferROC()
}

func (rj *rebJogger) doSend(lom *core.LOM, tsi *meta.Snode, roc cos.ReadOpenCloser) error {
	var (
		ack    = regularAck{rebID: rj.m.RebID(), daemonID: core.T.SID()}
		o      = transport.AllocSend()
		opaque = ack.NewPack()
	)
	o.Hdr.Bck.Copy(lom.Bucket())
	o.Hdr.ObjName = lom.ObjName
	o.Hdr.Opaque = opaque
	o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
	o.Callback, o.CmplArg = rj.objSentCallback, lom
	rj.m.inQueue.Inc()
	return rj.m.dm.Send(o, roc, tsi)
}
