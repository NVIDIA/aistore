// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/fs"
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
		t         cluster.Target
		smap      atomic.Pointer // new smap which will be soon live
		xreb      atomic.Pointer // unsafe(*xact.Rebalance)
		dm        *bundle.DataMover
		pushes    *bundle.Streams // broadcast notifications
		filterGFN *prob.Filter
		semaCh    *cos.Semaphore
		ecClient  *http.Client
		stages    *nodeStages
		lomacks   [cos.MultiSyncMapCount]*lomAcks
		awaiting  struct {
			targets cluster.Nodes // targets for which we are waiting for
			ts      int64         // last time we have recomputed
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
		q  map[string]*cluster.LOM // on the wire, waiting for ACK
	}
	joggerBase struct {
		m    *Reb
		xreb *xs.Rebalance
		wg   *sync.WaitGroup
	}
	rebJogger struct {
		joggerBase
		smap *cluster.Smap
		opts fs.WalkOpts
		ver  int64
	}
	rebArgs struct {
		smap   *cluster.Smap
		config *cmn.Config
		apaths fs.MPI
		id     int64
		ecUsed bool
	}
)

func New(t cluster.Target, config *cmn.Config) *Reb {
	ecClient := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout.D(),
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	reb := &Reb{
		t:         t,
		filterGFN: prob.NewDefaultFilter(),
		stages:    newNodeStages(),
		ecClient:  ecClient,
	}
	dmExtra := bundle.Extra{
		RecvAck:     reb.recvAck,
		Compression: config.Rebalance.Compression,
		Multiplier:  config.Rebalance.SbundleMult,
	}
	dm, err := bundle.NewDataMover(t, trname, reb.recvObj, cmn.OwtMigrate, dmExtra)
	if err != nil {
		cos.ExitLogf("%v", err)
	}
	reb.dm = dm

	// serialize one global rebalance at a time
	reb.semaCh = cos.NewSemaphore(1)
	return reb
}

func (reb *Reb) regRecv() {
	if err := reb.dm.RegRecv(); err != nil {
		cos.ExitLogf("%v", err)
	}
	if err := transport.HandleObjStream(trnamePsh, reb.recvStageNtfn /*RecvObj*/); err != nil {
		cos.ExitLogf("%v", err)
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
func (reb *Reb) RunRebalance(smap *cluster.Smap, id int64, notif *xact.NotifXact) {
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
	glog.Infof("%s: initializing", logHdr)
	bmd := reb.t.Bowner().Get()
	rargs := &rebArgs{id: id, smap: smap, config: cmn.GCO.Get(), ecUsed: bmd.IsECUsed()}
	if !reb.serialize(rargs, logHdr) {
		return
	}

	reb.regRecv()
	// TODO -- FIXME: minimally, must be num(active) + num(maintenance-mode-rebalancing-out)
	haveStreams := smap.CountTargets() > 1
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
		glog.Infof("%s: nothing to do: %s, %s", logHdr, smap.StringEx(), bmd.StringEx())
		reb.stages.stage.Store(rebStageDone)
		reb.unregRecv()
		reb.semaCh.Release()
		fs.RemoveMarker(fname.RebalanceMarker)
		fs.RemoveMarker(fname.NodeRestartedPrev)
		reb.xctn().Finish(nil)
		return
	}

	// At this point, only one rebalance is running

	onGFN()

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

	for errCnt != 0 && !reb.xctn().IsAborted() {
		errCnt = reb.bcast(rargs, reb.waitFinExtended)
	}

	reb.fini(rargs, logHdr, err)

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

func (reb *Reb) serialize(rargs *rebArgs, logHdr string) bool {
	// 1. check whether other targets are up and running
	if errCnt := reb.bcast(rargs, reb.pingTarget); errCnt > 0 {
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = reb.t.Sowner().Get()
	}
	// 2. serialize global rebalance and start new xaction -
	//    but only if the one that handles the current version is _not_ already in progress
	if newerRMD, alreadyRunning := reb.acquire(rargs, logHdr); newerRMD || alreadyRunning {
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = reb.t.Sowner().Get()
	}
	rargs.apaths = fs.GetAvail()
	return true
}

func (reb *Reb) acquire(rargs *rebArgs, logHdr string) (newerRMD, alreadyRunning bool) {
	var (
		total    time.Duration
		sleep    = rargs.config.Timeout.CplaneOperation.D()
		maxTotal = cos.MaxDuration(20*sleep, 10*time.Second) // time to abort prev. streams
		maxwt    = cos.MaxDuration(rargs.config.Rebalance.DestRetryTime.D(), 2*maxTotal)
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
			glog.Warningf(fmtpend, logHdr, id)
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
			if errcnt > 1 {
				glog.Infof("%s: resolved (%d)", logHdr, errcnt)
			}
			return
		}

		// try to preempt
		err := reb._preempt(rargs, logHdr, total, maxTotal, errcnt)
		if err != nil {
			if total > maxwt {
				cos.ExitLogf("%v", err)
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
			rlogHdr = reb.logHdr(rebID, (*cluster.Smap)(rsmap), true)
			xreb    = reb.xctn()
			s       string
		)
		if xreb != nil {
			s = ", " + xreb.String()
		}
		err = fmt.Errorf("%s: acquire/release asymmetry vs %s%s", logHdr, rlogHdr, s)
		if errcnt%2 == 1 {
			glog.Error(err)
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
		glog.Warningf("%s: aborting older %s", logHdr, otherXreb)
		return
	}
	if total > maxTotal {
		err = fmt.Errorf("%s: preempting older %s takes too much time", logHdr, otherXreb)
		glog.Error(err)
		if xreb := reb.xctn(); xreb != nil && xreb.ID() == otherXreb.ID() {
			debug.Assert(reb.dm.GetXact().ID() == otherXreb.ID())
			glog.Warningf("%s: aborting older streams...", logHdr)
			reb.abortStreams()
		}
	}
	return
}

func (reb *Reb) initRenew(rargs *rebArgs, notif *xact.NotifXact, logHdr string, haveStreams bool) bool {
	if id := reb.nxtID.Load(); id > rargs.id {
		glog.Warningf(fmtpend, logHdr, id)
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
		glog.Warningf(fmtpend, logHdr, id)
		return false
	}
	reb.stages.stage.Store(rebStageInit)
	xreb := xctn.(*xs.Rebalance)
	reb.setXact(xreb)
	reb.rebID.Store(rargs.id)

	// 3. init streams and data structures
	if haveStreams {
		reb.beginStreams(rargs.config)
	}

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
	if fatalErr, writeErr := fs.PersistMarker(fname.RebalanceMarker); fatalErr != nil || writeErr != nil {
		err := writeErr
		if fatalErr != nil {
			err = fatalErr
		}
		reb.endStreams(err)
		xctn.Abort(err)
		reb.mu.Unlock()
		glog.Errorf("FATAL: %v, WRITE: %v", fatalErr, writeErr)
		return false
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(rargs.smap))
	reb.stages.cleanup()

	reb.mu.Unlock()
	glog.Infof("%s: running %s", reb.logHdr(rargs.id, rargs.smap), reb.xctn())
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
		Extra:      &transport.Extra{SenderID: xreb.ID()},
	}
	reb.pushes = bundle.New(reb.t.Sowner(), reb.t.Snode(), transport.NewIntraDataClient(), pushArgs)

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
	_ = reb.bcast(rargs, reb.rxReady) // NOTE: ignore timeout
	xreb := reb.xctn()
	if err := xreb.AbortErr(); err != nil {
		return cmn.NewErrAborted(xreb.Name(), "reb-run-ec-bcast", err)
	}

	reb.runECjoggers()

	if err := xreb.AbortErr(); err != nil {
		return cmn.NewErrAborted(xreb.Name(), "reb-run-ec-joggers", err)
	}
	glog.Infof("[%s] RebalanceEC done", reb.t.SID())
	return nil
}

// when no bucket has EC enabled
func (reb *Reb) runNoEC(rargs *rebArgs) error {
	ver := rargs.smap.Version
	_ = reb.bcast(rargs, reb.rxReady) // NOTE: ignore timeout

	xreb := reb.xctn()
	if err := xreb.AbortErr(); err != nil {
		return cmn.NewErrAborted(xreb.Name(), "reb-run-bcast", err)
	}

	wg := &sync.WaitGroup{}
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
		return cmn.NewErrAborted(xreb.Name(), "reb-run-joggers", err)
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("finished rebalance walk (g%d)", rargs.id)
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
	)
	maxwt += time.Duration(int64(time.Minute) * int64(rargs.smap.CountTargets()/10))
	maxwt = cos.MinDuration(maxwt, rargs.config.Rebalance.DestRetryTime.D()*2)
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
				if err := xreb.AbortErr(); err != nil {
					glog.Infof("%s: abort wait-ack (%v)", logHdr, err)
					return
				}
			}
			if cnt == 0 {
				glog.Infof("%s: received all ACKs", logHdr)
				break
			}
			glog.Warningf("%s: waiting for %d ACKs", logHdr, cnt)
			if err := xreb.AbortedAfter(sleep); err != nil {
				glog.Infof("%s: abort wait-ack (%v)", logHdr, err)
				return
			}

			curwt += sleep
		}
		if cnt > 0 {
			glog.Warningf("%s: timed out waiting for %d ACK%s", logHdr, cnt, cos.Plural(cnt))
		}
		if xreb.IsAborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Get()
		if len(aPaths) > len(rargs.apaths) {
			glog.Warningf("%s: mountpath changes detected (%d, %d)", logHdr, len(aPaths), len(rargs.apaths))
		}

		// 8. synchronize
		glog.Infof("%s: poll targets for: stage=(%s or %s***)", logHdr, stages[rebStageFin], stages[rebStageWaitAck])
		errCnt = reb.bcast(rargs, reb.waitFinExtended)
		if xreb.IsAborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(rargs, xreb)
		if cnt == 0 || reb.xctn().IsAborted() {
			break
		}
		glog.Warningf("%s: retransmitted %d, more wack...", logHdr, cnt)
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
				if cmn.IsObjNotExist(err) {
					glog.Warningf("%s: object not found (lom: %s, err: %v)", loghdr, lom, err)
				} else {
					glog.Errorf("%s: failed loading %s, err: %s", loghdr, lom, err)
				}
				delete(lomAck.q, uname)
				continue
			}
			tsi, _ := cluster.HrwTarget(lom.Uname(), rargs.smap)
			if reb.t.HeadObjT2T(lom, tsi) {
				if glog.FastV(4, glog.SmoduleReb) {
					glog.Infof("%s: HEAD ok %s at %s", loghdr, lom, tsi.StringEx())
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
				glog.Warningf("%s: retransmit %s => %s", loghdr, lom, tsi.StringEx())
				cnt++
			} else {
				glog.Errorf("%s: failed to retransmit %s => %s: %v", loghdr, lom, tsi.StringEx(), err)
				if cmn.IsErrStreamTerminated(err) {
					xreb.Abort(err)
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
	yes = yes || (rargs.smap.Version != reb.t.Sowner().Get().Version)
	return
}

func (reb *Reb) fini(rargs *rebArgs, logHdr string, err error) {
	var stats cluster.Stats
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("finishing rebalance (reb_args: %s)", reb.logHdr(rargs.id, rargs.smap))
	}
	// prior to closing the streams
	if q := reb.quiesce(rargs, rargs.config.Transport.QuiesceTime.D(), reb.nodesQuiescent); q != cluster.QuiAborted {
		if errM := fs.RemoveMarker(fname.RebalanceMarker); errM == nil {
			glog.Infof("%s: %s removed marker ok", reb.t, reb.xctn())
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
		glog.Infoln(string(s))
	}
	reb.stages.stage.Store(rebStageDone)
	reb.stages.cleanup()

	reb.unregRecv()
	reb.semaCh.Release()
	if !xreb.Finished() {
		xreb.Finish(nil)
	}
	glog.Infof("%s: done (%s)", logHdr, xreb)
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
	bmd := rj.m.t.Bowner().Get()
	bmd.Range(nil, nil, rj.walkBck)
}

func (rj *rebJogger) walkBck(bck *cluster.Bck) bool {
	rj.opts.Bck.Copy(bck.Bucket())
	err := fs.Walk(&rj.opts)
	if err == nil {
		return rj.xreb.IsAborted()
	}
	if rj.xreb.IsAborted() {
		glog.Infof("aborting traversal")
	} else {
		glog.Errorf("%s: failed to traverse, err: %v", rj.m.t, err)
	}
	return true
}

// send completion
func (rj *rebJogger) objSentCallback(hdr transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	rj.m.inQueue.Dec()
	if err != nil {
		if bool(glog.FastV(4, glog.SmoduleReb)) || !cos.IsRetriableConnErr(err) {
			lom, ok := arg.(*cluster.LOM)
			debug.Assert(ok)
			glog.Errorf("%s: failed to send %s: %v", rj.m.t.Snode(), lom, err)
		}
		return
	}
	xreb := rj.xreb
	xreb.OutObjsAdd(1, hdr.ObjAttrs.Size) // NOTE: double-counts retransmissions
}

func (rj *rebJogger) visitObj(fqn string, de fs.DirEntry) (err error) {
	if err := rj.xreb.AbortErr(); err != nil {
		return cmn.NewErrAborted(rj.xreb.Name(), "rj-walk", err)
	}
	if de.IsDir() {
		return nil
	}
	lom := cluster.AllocLOM(fqn)
	err = rj._lwalk(lom, fqn)
	if err != nil {
		cluster.FreeLOM(lom)
		if err == cmn.ErrSkip {
			err = nil
		}
	}
	return
}

func (rj *rebJogger) _lwalk(lom *cluster.LOM, fqn string) error {
	if err := lom.InitFQN(fqn, nil); err != nil {
		if cmn.IsErrBucketLevel(err) {
			return err
		}
		return cmn.ErrSkip
	}
	// skip EC.Enabled bucket - the job for EC rebalance
	if lom.Bck().Props.EC.Enabled {
		return filepath.SkipDir
	}
	tsi, err := cluster.HrwTarget(lom.Uname(), rj.smap)
	if err != nil {
		return err
	}
	if tsi.ID() == rj.m.t.SID() {
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
	roc, err := _getReader(lom)
	if err != nil {
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
func _getReader(lom *cluster.LOM) (roc cos.ReadOpenCloser, err error) {
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
	return lom.NewDeferROC()
}

func (rj *rebJogger) doSend(lom *cluster.LOM, tsi *cluster.Snode, roc cos.ReadOpenCloser) error {
	var (
		ack    = regularAck{rebID: rj.m.RebID(), daemonID: rj.m.t.SID()}
		o      = transport.AllocSend()
		opaque = ack.NewPack()
	)
	o.Hdr.Bck.Copy(lom.Bucket())
	o.Hdr.ObjName = lom.ObjName
	o.Hdr.Opaque = opaque
	o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs())
	o.Callback, o.CmplArg = rj.objSentCallback, lom
	rj.m.inQueue.Inc()
	return rj.m.dm.Send(o, roc, tsi)
}
