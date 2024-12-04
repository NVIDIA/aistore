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
	"github.com/NVIDIA/aistore/ec"
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

const initCapLomAcks = 128

type (
	Reb struct {
		smap      ratomic.Pointer[meta.Smap] // next smap (new that'll become current after rebalance)
		xreb      ratomic.Pointer[xs.Rebalance]
		dm        *bundle.DataMover
		pushes    *bundle.Streams // broadcast notifications
		filterGFN *prob.Filter
		ecClient  *http.Client
		stages    *nodeStages
		lomacks   [cos.MultiHashMapCount]*lomAcks
		awaiting  struct {
			targets meta.Nodes // targets for which we are waiting for
			ts      int64      // last time we have recomputed
			mtx     sync.Mutex
		}
		// (smap, xreb) + atomic state
		rebID atomic.Int64
		// quiescence
		lastrx atomic.Int64 // mono time
		// this state
		mu sync.Mutex
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
	reb.dm = bundle.NewDM(trname, reb.recvObj, cmn.OwtRebalance, dmExtra) // (compare with dm.Renew below)

	return reb
}

func (reb *Reb) regRecv() error {
	if err := reb.dm.RegRecv(); err != nil {
		return err
	}
	if err := transport.Handle(trnamePsh, reb.recvStageNtfn /*RecvObj*/); err != nil {
		debug.AssertNoErr(err)
		return err
	}
	return nil
}

func (reb *Reb) unregRecv() {
	reb.dm.UnregRecv()
	err := transport.Unhandle(trnamePsh)
	debug.AssertNoErr(err)
}

func (reb *Reb) _preempt(logHdr, oxid string) error {
	oxreb, err := xreg.GetXact(oxid)
	if err != nil {
		return err
	}
	if oxreb != nil && oxreb.Running() {
		oxreb.Abort(cmn.ErrXactRenewAbort)
		nlog.Warningln(logHdr, "[", cmn.ErrXactRenewAbort, oxreb.String(), "]", reb.dm.String())
	}
	for i := range 10 { // TODO: config
		if reb.dm.IsFree() {
			break
		}
		time.Sleep(time.Second)
		if i > 2 && i&1 == 1 {
			nlog.Warningln(logHdr, "preempt: polling for", reb.dm.String())
		}
	}
	return nil
}

func _preempt2(logHdr string, id int64) bool {
	entry := xreg.GetRunning(xreg.Flt{Kind: apc.ActRebalance})
	if entry == nil {
		return true
	}
	oxreb := entry.Get()
	if oid, err := xact.S2RebID(oxreb.ID()); err != nil /*unlikely*/ || oid >= id {
		return false // already running
	}

	s := oxreb.String()
	if oxreb.Running() {
		oxreb.Abort(cmn.ErrXactRenewAbort)
		nlog.Warningln(logHdr, "aborted _older_", s)
	} else {
		nlog.Warningln(logHdr, "found _older_", s)
	}
	return true
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
func (reb *Reb) RunRebalance(smap *meta.Smap, id int64, notif *xact.NotifXact, tstats cos.StatsUpdater, oxid string) {
	logHdr := reb.logHdr(id, smap, true /*initializing*/)
	// preempt
	if xact.IsValidRebID(oxid) {
		if err := reb._preempt(logHdr, oxid); err != nil {
			nlog.Errorln(logHdr, "failed to preempt:", err)
			return
		}
	}

	var (
		bmd   = core.T.Bowner().Get()
		rargs = &rebArgs{id: id, smap: smap, config: cmn.GCO.Get(), ecUsed: bmd.IsECUsed()}
	)
	if !reb.pingall(rargs, logHdr) {
		return
	}
	if err := reb.regRecv(); err != nil {
		if !_preempt2(logHdr, id) {
			nlog.Errorln(logHdr, "failed to preempt #2:", err)
			return
		}
		// sleep and retry just once
		time.Sleep(rargs.config.Timeout.MaxKeepalive.D())
		if err = reb.regRecv(); err != nil {
			nlog.Errorln(logHdr, err)
			return
		}
	}
	haveStreams := smap.HasPeersToRebalance(core.T.SID())
	if bmd.IsEmpty() {
		haveStreams = false
	}
	if !reb.initRenew(rargs, notif, logHdr, haveStreams) {
		reb.unregRecv()
		return
	}
	if !haveStreams {
		// cleanup and leave
		nlog.Infof("%s: nothing to do: %s, %s", logHdr, smap.StringEx(), bmd.StringEx())
		reb.stages.stage.Store(rebStageDone)
		reb.unregRecv()
		fs.RemoveMarker(fname.RebalanceMarker, tstats)
		fs.RemoveMarker(fname.NodeRestartedPrev, tstats)
		reb.xctn().Finish()
		return
	}

	nlog.Infoln(logHdr, "initializing")

	// abort all running `dtor.AbortRebRes` xactions (download, dsort, etl)
	xreg.AbortByNewReb(errors.New("reason: starting " + reb.xctn().Name()))

	// only one rebalance is running -----------------

	reb.lastrx.Store(0)

	if rargs.ecUsed {
		ec.ECM.OpenStreams(true /*with refc*/)
	}
	onGFN()

	tstats.SetFlag(cos.NodeAlerts, cos.Rebalancing)

	// run
	err := reb.run(rargs)
	if err == nil {
		errCnt := reb.rebWaitAck(rargs)
		if errCnt == 0 {
			nlog.Infoln(logHdr, "=> stage-fin")
		} else {
			nlog.Warningln(logHdr, "=> stage-fin", "[ num-errors:", errCnt, "]")
		}
	} else {
		nlog.Errorln(logHdr, "fail => stage-fin:", err)
	}
	reb.changeStage(rebStageFin)

	reb.fini(rargs, logHdr, err, tstats)
	tstats.ClrFlag(cos.NodeAlerts, cos.Rebalancing)

	offGFN()
	if rargs.ecUsed {
		// rebalance can open EC streams; it has has no authority, however, to close them - primary has
		// that's why all we do here is decrement ref-count back to what it was before this run
		ec.ECM.CloseStreams(true /*just refc*/)
	}
}

// To optimize goroutine creation:
//  1. One bucket case just calls a single rebalance worker depending on
//     whether a bucket is erasure coded (goroutine is not used).
//  2. Multi-bucket rebalance may start both non-EC and EC in parallel.
//     It then waits until everything finishes.
func (reb *Reb) run(rargs *rebArgs) error {
	reb.stages.stage.Store(rebStageTraverse)

	// No EC-enabled buckets - run only regular rebalance
	xid := xact.RebID2S(rargs.id)
	if !rargs.ecUsed {
		nlog.Infoln("starting", xid)
		return reb.runNoEC(rargs)
	}

	// In all other cases run both rebalances simultaneously
	group := &errgroup.Group{}
	group.Go(func() error {
		nlog.Infoln("starting non-EC", xid)
		return reb.runNoEC(rargs)
	})
	group.Go(func() error {
		nlog.Infoln("starting EC", xid)
		return reb.runEC(rargs)
	})
	return group.Wait()
}

func (reb *Reb) pingall(rargs *rebArgs, logHdr string) bool {
	if rargs.smap.Version == 0 {
		rargs.smap = core.T.Sowner().Get()
	}
	// whether other targets are up and running
	if errCnt := bcast(rargs, reb.pingTarget); errCnt > 0 {
		nlog.Errorln(logHdr, "not starting: ping err-s", errCnt)
		return false
	}
	if rargs.smap.Version == 0 {
		rargs.smap = core.T.Sowner().Get()
	}
	rargs.apaths = fs.GetAvail()
	return true
}

func (reb *Reb) initRenew(rargs *rebArgs, notif *xact.NotifXact, logHdr string, haveStreams bool) bool {
	rns := xreg.RenewRebalance(rargs.id)
	if rns.Err != nil {
		return false
	}
	if rns.IsRunning() {
		return false
	}
	xctn := rns.Entry.Get()

	notif.Xact = xctn
	xctn.AddNotif(notif)

	reb.mu.Lock()

	reb.stages.stage.Store(rebStageInit)
	xreb := xctn.(*xs.Rebalance)
	reb.setXact(xreb)
	reb.rebID.Store(rargs.id)

	// prior to opening streams:
	// not every change in Smap warants a different rebalance but this one (below) definitely does
	smap := core.T.Sowner().Get()
	if smap.CountActiveTs() != rargs.smap.CountActiveTs() {
		debug.Assert(smap.Version > rargs.smap.Version)
		err := fmt.Errorf("%s post-renew change %s => %s", xreb, rargs.smap.StringEx(), smap.StringEx())
		xctn.Abort(err)
		reb.mu.Unlock()
		nlog.Errorln(err)
		return false
	}
	if smap.Version != rargs.smap.Version {
		nlog.Warningln(logHdr, "post-renew change:", rargs.smap.StringEx(), "=>", smap.StringEx(), "- proceeding anyway")
	}

	// 3. init streams and data structures
	if haveStreams {
		dmExtra := bundle.Extra{
			RecvAck:     reb.recvAck,
			Config:      rargs.config,
			Compression: rargs.config.Rebalance.Compression,
			Multiplier:  rargs.config.Rebalance.SbundleMult,
		}
		if dm := reb.dm.Renew(trname, reb.recvObj, cmn.OwtRebalance, dmExtra); dm != nil {
			reb.dm = dm
		}
		reb.beginStreams(rargs.config)
	}

	if reb.awaiting.targets == nil {
		reb.awaiting.targets = make(meta.Nodes, 0, maxWackTargets)
	} else {
		reb.awaiting.targets = reb.awaiting.targets[:0]
	}
	acks := reb.lomAcks()
	for i := range len(acks) { // init lom acks
		acks[i] = &lomAcks{mu: &sync.Mutex{}, q: make(map[string]*core.LOM, initCapLomAcks)}
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
		nlog.Errorln("FATAL:", fatalErr, "WRITE:", writeErr)
		return false
	}

	// 5. ready - can receive objects
	reb.smap.Store(rargs.smap)
	reb.stages.cleanup()

	reb.mu.Unlock()

	nlog.Infoln(logHdr, "- running", reb.xctn())
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
}

func (reb *Reb) endStreams(err error) {
	swapped := reb.stages.stage.CAS(rebStageFin, rebStageFinStreams)
	debug.Assert(swapped)
	reb.dm.Close(err)
	reb.pushes.Close(err == nil)
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
		cnt   int
		sleep = rargs.config.Timeout.CplaneOperation.D()
		maxwt = rargs.config.Rebalance.DestRetryTime.D()
		xreb  = reb.xctn()
		smap  = rargs.smap
	)
	maxwt += time.Duration(int64(time.Minute) * int64(rargs.smap.CountTargets()/10))
	maxwt = min(maxwt, rargs.config.Rebalance.DestRetryTime.D()*2)
	reb.changeStage(rebStageWaitAck)
	logHdr := reb.logHdr(rargs.id, rargs.smap)

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
								nlog.Infoln("waiting for", lom.String(), "ACK from", tsi.StringEx())
								logged = true
								break
							}
						}
					}
				}
				lomack.mu.Unlock()
				if err := xreb.AbortErr(); err != nil {
					nlog.Infoln(logHdr, "abort wait-ack:", err)
					return
				}
			}
			if cnt == 0 {
				nlog.Infoln(logHdr, "received all ACKs")
				break
			}
			nlog.Warningln(logHdr, "waiting for", cnt, "ACKs")
			if err := xreb.AbortedAfter(sleep); err != nil {
				nlog.Infoln(logHdr, "abort wait-ack:", err)
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
		errCnt = bcast(rargs, reb.waitAcksExtended)
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

func (reb *Reb) fini(rargs *rebArgs, logHdr string, err error, tstats cos.StatsUpdater) {
	var (
		tag   string
		stats core.Stats
		qui   = &qui{rargs: rargs, reb: reb, logHdr: logHdr}
		xreb  = reb.xctn()
		cnt   = xreb.ErrCnt()
	)
	if cnt == 0 {
		nlog.Infoln(logHdr, "fini => quiesce")
	} else {
		nlog.Warningln(logHdr, "fini [", cnt, "] => quiesce")
	}

	// prior to closing streams
	ret := xreb.Quiesce(rargs.config.Transport.QuiesceTime.D(), qui.quicb)
	cnt = xreb.ErrCnt()

	// cleanup markers
	if ret != core.QuiAborted && ret != core.QuiTimeout {
		tag = "qui-aborted"
		if errM := fs.RemoveMarker(fname.RebalanceMarker, tstats); errM == nil {
			nlog.Infoln(logHdr, "removed marker ok")
		}
		_ = fs.RemoveMarker(fname.NodeRestartedPrev, tstats)
		if ret == core.QuiTimeout {
			tag = "qui-timeout"
		}
	}

	reb.endStreams(err)
	reb.filterGFN.Reset()

	xreb.ToStats(&stats)
	if stats.Objs > 0 || stats.OutObjs > 0 || stats.InObjs > 0 {
		s, e := jsoniter.MarshalIndent(&stats, "", " ")
		debug.AssertNoErr(e)
		nlog.Infoln(string(s))
	}
	reb.stages.stage.Store(rebStageDone)
	reb.stages.cleanup()

	reb.unregRecv()
	xreb.Finish()

	if ret != core.QuiAborted && ret != core.QuiTimeout && cnt == 0 {
		nlog.Infoln(logHdr, "done", xreb.String())
	} else {
		nlog.Infoln(logHdr, "finished with errors [", tag, cnt, "]", xreb.String())
	}
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
	uname := lom.UnamePtr()
	bname := cos.UnsafeBptr(uname)
	if rj.m.filterGFN.Lookup(*bname) {
		rj.m.filterGFN.Delete(*bname)
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
		rj.m.cleanupLomAck(lom)
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
	debug.Assert(ack.rebID != 0)
	o.Hdr.Bck.Copy(lom.Bucket())
	o.Hdr.ObjName = lom.ObjName
	o.Hdr.Opaque = opaque
	o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
	o.Callback, o.CmplArg = rj.objSentCallback, lom
	return rj.m.dm.Send(o, roc, tsi)
}
