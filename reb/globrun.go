// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
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
	trname = "reb"
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
		dm        *bundle.DM
		filterGFN *prob.Filter
		ecClient  *http.Client
		stages    *nodeStages
		lomacks   [cos.MultiHashMapCount]*lomAcks
		awaiting  struct {
			targets meta.Nodes // targets for which we are waiting for
			ts      int64      // last time we have recomputed
			mtx     sync.Mutex
		}
		lazydel lazydel
		// (smap, xreb) + atomic state
		rebID atomic.Int64
		// quiescence
		lastrx atomic.Int64 // mono time
		// this state
		mu sync.Mutex
	}
	ExtArgs struct {
		Tstats cos.StatsUpdater
		Notif  *xact.NotifXact
		Bck    *meta.Bck // advanced usage, limited scope
		Prefix string    // ditto
		Oxid   string    // oldRMD g[version]
		NID    int64     // newRMD version
	}
)

type (
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
		rargs *rebArgs
		opts  fs.WalkOpts
		ver   int64
	}
	// internal runtime context (compare with caller's ExtArgs{} above)
	rebArgs struct {
		smap   *meta.Smap
		config *cmn.Config
		xreb   *xs.Rebalance
		bck    *meta.Bck // advanced usage, limited scope
		apaths fs.MPI
		logHdr string
		prefix string // ditto, as in: traverse only bck[/prefix]
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
		RecvAck:     reb.recvAckNtfn,
		Config:      config,
		Compression: config.Rebalance.Compression,
		Multiplier:  config.Rebalance.SbundleMult,
	}
	reb.dm = bundle.NewDM(trname, reb.recvObj, cmn.OwtRebalance, dmExtra) // (compare with dm.Renew below)

	reb.lazydel.init()

	return reb
}

func (reb *Reb) _preempt(logHdr, oxid string) error {
	const (
		retries = 10 // TODO: config
	)
	oxreb, err := xreg.GetXact(oxid)
	if err != nil {
		return err
	}
	if oxreb != nil && oxreb.Running() {
		oxreb.Abort(cmn.ErrXactRenewAbort)
		nlog.Warningln(logHdr, "[", cmn.ErrXactRenewAbort, oxreb.String(), "]", reb.dm.String())
	}
	for i := range retries {
		if reb.dm.IsFree() {
			return nil
		}
		time.Sleep(time.Second)
		if i > 2 && i&1 == 1 {
			nlog.Warningln(logHdr, "preempt: polling for", reb.dm.String())
		}
	}
	// force
	reb.dm.Abort()
	reb.dm.UnregRecv()
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
func (reb *Reb) RunRebalance(smap *meta.Smap, extArgs *ExtArgs) {
	if reb.rebID.Load() == extArgs.NID {
		return
	}

	reb.lazydel.stop()

	logHdr := reb.logHdr(extArgs.NID, smap, true /*initializing*/)
	// preempt
	if xact.IsValidRebID(extArgs.Oxid) {
		if err := reb._preempt(logHdr, extArgs.Oxid); err != nil {
			nlog.Errorln(logHdr, "failed to preempt:", err)
			return
		}
	}

	var (
		bmd = core.T.Bowner().Get()
		// runtime context (scope: this uniquely ID-ed rebalance)
		rargs = &rebArgs{
			id:     extArgs.NID, // == newRMD.Version
			smap:   smap,
			config: cmn.GCO.Get(),
			bck:    extArgs.Bck,    // advanced usage
			prefix: extArgs.Prefix, // ditto
			logHdr: logHdr,
			ecUsed: bmd.IsECUsed(),
		}
	)
	if rargs.bck != nil && !rargs.bck.IsEmpty() {
		rargs.logHdr += "::" + rargs.bck.Cname(rargs.prefix)
	}
	if !_pingall(rargs) {
		return
	}
	if reb.rebID.Load() == extArgs.NID {
		return
	}
	if err := reb.dm.RegRecv(); err != nil {
		if !_preempt2(logHdr, extArgs.NID) {
			nlog.Errorln(logHdr, "failed to preempt #2:", err)
			return
		}
		// sleep and retry just once
		time.Sleep(rargs.config.Timeout.MaxKeepalive.D())
		if err = reb.dm.RegRecv(); err != nil {
			nlog.Errorln(logHdr, err)
			return
		}
	}
	haveStreams := smap.HasPeersToRebalance(core.T.SID())
	if bmd.IsEmpty() {
		haveStreams = false
	}
	if !reb.initRenew(rargs, extArgs.Notif, haveStreams) {
		reb.dm.UnregRecv()
		return
	}
	if !haveStreams {
		// cleanup and leave
		nlog.Infof("%s: nothing to do: %s, %s", logHdr, smap.StringEx(), bmd.StringEx())
		reb.stages.stage.Store(rebStageDone)
		reb.dm.UnregRecv()
		fs.RemoveMarker(fname.RebalanceMarker, extArgs.Tstats)
		fs.RemoveMarker(fname.NodeRestartedPrev, extArgs.Tstats)
		rargs.xreb.Finish()
		return
	}

	if extArgs.Bck == nil {
		nlog.Infoln(logHdr, "initializing")
	} else {
		nlog.Warningln(logHdr, "initializing - limited scope: [", extArgs.Bck.Cname(extArgs.Prefix), "]")
	}

	// abort all running `dtor.AbortRebRes` xactions (download, dsort, etl)
	xreg.AbortByNewReb(errors.New("reason: starting " + rargs.xreb.Name()))

	// only one rebalance is running -----------------

	reb.lastrx.Store(0)

	if rargs.ecUsed {
		ec.ECM.OpenStreams(true /*with refc*/)
	}
	onGFN()

	extArgs.Tstats.SetFlag(cos.NodeAlerts, cos.Rebalancing)

	// run
	go reb.lazydel.run(rargs.xreb, rargs.config, rargs.id)
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

	reb.fini(rargs, err, extArgs.Tstats)
	extArgs.Tstats.ClrFlag(cos.NodeAlerts, cos.Rebalancing)

	offGFN()
	if rargs.ecUsed {
		// rebalance can open EC streams; it has no authority, however, to close them - primary has
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
	if !rargs.ecUsed {
		return reb.runNoEC(rargs)
	}

	// In all other cases run both rebalances simultaneously
	group := &errgroup.Group{}
	group.Go(func() error {
		return reb.runNoEC(rargs)
	})
	group.Go(func() error {
		return reb.runEC(rargs)
	})
	return group.Wait()
}

func _pingall(rargs *rebArgs) bool {
	debug.Assert(rargs.smap.Version > 0) // validated in _runRe

	// whether other targets are up and running
	if errCnt := bcast(rargs, _pingTarget); errCnt > 0 {
		nlog.Errorln(rargs.logHdr, "not starting: ping err-s", errCnt)
		return false
	}
	rargs.apaths = fs.GetAvail()
	return true
}

func (reb *Reb) initRenew(rargs *rebArgs, notif *xact.NotifXact, haveStreams bool) bool {
	var ctlmsg string
	if rargs.bck != nil && !rargs.bck.IsEmpty() {
		ctlmsg = rargs.bck.Cname(rargs.prefix)
	}
	rns := xreg.RenewRebalance(rargs.id, ctlmsg)
	if rns.Err != nil {
		return false
	}
	if rns.IsRunning() {
		return false
	}
	xctn := rns.Entry.Get()
	rargs.xreb = xctn.(*xs.Rebalance)

	notif.Xact = rargs.xreb
	rargs.xreb.AddNotif(notif)

	reb.mu.Lock()

	reb.stages.stage.Store(rebStageInit)
	reb.setXact(rargs.xreb)
	reb.rebID.Store(rargs.id)

	// prior to opening streams:
	// not every change in Smap warants a different rebalance but this one (below) definitely does
	smap := core.T.Sowner().Get()
	if smap.CountActiveTs() != rargs.smap.CountActiveTs() {
		debug.Assert(smap.Version > rargs.smap.Version)
		err := fmt.Errorf("%s post-renew change %s => %s", rargs.xreb, rargs.smap.StringEx(), smap.StringEx())
		rargs.xreb.Abort(err)
		reb.mu.Unlock()
		nlog.Errorln(err)
		return false
	}
	if smap.Version != rargs.smap.Version {
		nlog.Warningln(rargs.logHdr, "post-renew change:", rargs.smap.StringEx(), "=>", smap.StringEx(), "- proceeding anyway")
	}

	// 3. init streams and data structures
	if haveStreams {
		dmExtra := bundle.Extra{
			RecvAck:     reb.recvAckNtfn,
			Config:      rargs.config,
			Compression: rargs.config.Rebalance.Compression,
			Multiplier:  rargs.config.Rebalance.SbundleMult,
		}
		if dm := reb.dm.Renew(trname, reb.recvObj, cmn.OwtRebalance, dmExtra); dm != nil {
			reb.dm = dm
		}
		if err := reb.beginStreams(rargs); err != nil {
			rargs.xreb.Abort(err)
			reb.mu.Unlock()
			nlog.Errorln(err)
			return false
		}
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
		reb.endStreams(err, rargs.logHdr)
		rargs.xreb.Abort(err)
		reb.mu.Unlock()
		nlog.Errorln("FATAL:", fatalErr, "WRITE:", writeErr)
		return false
	}

	// 5. ready - can receive objects
	reb.smap.Store(rargs.smap)
	reb.stages.cleanup()

	reb.mu.Unlock()

	nlog.Infoln(rargs.logHdr, "- running", rargs.xreb.String())
	return true
}

func (reb *Reb) beginStreams(rargs *rebArgs) error {
	if reb.stages.stage.Load() != rebStageInit {
		return fmt.Errorf("%s: cannot begin at stage %d", rargs.logHdr, reb.stages.stage.Load())
	}

	reb.dm.SetXact(rargs.xreb)
	reb.dm.Open()
	return nil
}

func (reb *Reb) endStreams(err error, loghdr string) {
	if !reb.stages.stage.CAS(rebStageFin, rebStageFinStreams) {
		nlog.Warningln(loghdr, "stage", reb.stages.stage.Load())
	}
	reb.dm.Close(err)
}

// when at least one bucket has EC enabled
func (reb *Reb) runEC(rargs *rebArgs) error {
	nlog.Infoln(rargs.logHdr, "start ec run")

	errCnt := bcast(rargs, reb.rxReady) // ignore timeout
	if err := rargs.xreb.AbortErr(); err != nil {
		nlog.Infoln(rargs.logHdr, "abort ec rx-ready", err, "num-fail", errCnt)
		return err
	}
	if errCnt > 0 {
		nlog.Errorln(rargs.logHdr, "ec rx-ready num-fail", errCnt) // unlikely
	}

	reb.runECjoggers(rargs)

	if err := rargs.xreb.AbortErr(); err != nil {
		nlog.Warningln(rargs.logHdr, "finish ec run, abort ec-joggers: [", err)
		return err
	}
	nlog.Infoln(rargs.logHdr, "finish ec run")
	return nil
}

// when not a single bucket has EC enabled
func (reb *Reb) runNoEC(rargs *rebArgs) error {
	nlog.Infoln(rargs.logHdr, "start no-ec run")

	errCnt := bcast(rargs, reb.rxReady) // ignore timeout
	if err := rargs.xreb.AbortErr(); err != nil {
		nlog.Infoln(rargs.logHdr, "abort rx-ready", err, "num-fail", errCnt)
		return err
	}
	if errCnt > 0 {
		nlog.Errorln(rargs.logHdr, "rx-ready num-fail:", errCnt) // unlikely
	}
	var (
		wg  = &sync.WaitGroup{}
		ver = rargs.smap.Version
	)
	for _, mi := range rargs.apaths {
		rl := &rebJogger{
			joggerBase: joggerBase{m: reb, xreb: rargs.xreb, wg: wg},
			rargs:      rargs,
			ver:        ver,
		}
		wg.Add(1)
		go rl.jog(mi)
	}
	wg.Wait()

	if err := rargs.xreb.AbortErr(); err != nil {
		nlog.Warningln(rargs.logHdr, "finish no-ec run, abort joggers: [", err, "]")
		return err
	}
	nlog.Infoln(rargs.logHdr, "finish no-ec run")
	return nil
}

func (reb *Reb) rebWaitAck(rargs *rebArgs) (errCnt int) {
	var (
		cnt   int
		sleep = rargs.config.Timeout.CplaneOperation.D()
		maxwt = rargs.config.Rebalance.DestRetryTime.D()
		xreb  = rargs.xreb
		smap  = rargs.smap
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
								nlog.Infoln(rargs.logHdr, "waiting for", lom.String(), "ACK from", tsi.StringEx())
								logged = true
								break
							}
						}
					}
				}
				lomack.mu.Unlock()

				if err := xreb.AbortErr(); err != nil {
					nlog.Infoln(rargs.logHdr, "abort wait-ack:", err)
					return
				}
			}
			if cnt == 0 {
				nlog.Infoln(rargs.logHdr, "received all ACKs")
				break
			}
			if err := xreb.AbortedAfter(sleep); err != nil {
				nlog.Infoln(rargs.logHdr, "abort wait-ack:", err)
				return
			}

			nlog.Warningln(rargs.logHdr, "waiting for", cnt, "ACKs")
			curwt += sleep
		}
		if cnt > 0 {
			nlog.Warningf("%s timed out waiting for %d ACK%s", rargs.logHdr, cnt, cos.Plural(cnt))
		}
		if xreb.IsAborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Get()
		if len(aPaths) > len(rargs.apaths) {
			nlog.Warningf("%s mountpath changes detected (%d, %d)", rargs.logHdr, len(aPaths), len(rargs.apaths))
		}

		// 8. synchronize
		nlog.Infof("%s poll targets for: stage=(%s or %s***)", rargs.logHdr, stages[rebStageFin], stages[rebStageWaitAck])
		errCnt = bcast(rargs, reb.waitAcksExtended)
		if xreb.IsAborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(rargs)
		if cnt == 0 || xreb.IsAborted() {
			break
		}
		nlog.Warningln(rargs.logHdr, "retransmitted", cnt, "keeping wack...")
	}

	return
}

func (reb *Reb) retransmit(rargs *rebArgs) (cnt int) {
	xreb := rargs.xreb
	if xreb.IsAborted() {
		return
	}
	var (
		rj = &rebJogger{
			joggerBase: joggerBase{
				m:    reb,
				xreb: rargs.xreb,
				wg:   &sync.WaitGroup{},
			},
			rargs: rargs,
		}
		loghdr = rargs.logHdr
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
					nlog.Errorln(loghdr, "stream term-ed:", err)
				} else {
					err = fmt.Errorf("%s: failed to retransmit %s => %s: %w", loghdr, lom, tsi.StringEx(), err)
					rj.xreb.AddErr(err)
				}
			}

			if xreb.IsAborted() {
				lomAck.mu.Unlock()
				return 0
			}
		}

		lomAck.mu.Unlock()
		if xreb.IsAborted() {
			return 0
		}
	}
	return
}

func (reb *Reb) fini(rargs *rebArgs, err error, tstats cos.StatsUpdater) {
	var (
		stats core.Stats
		qui   = &qui{rargs: rargs, reb: reb}
		xreb  = rargs.xreb
		cnt   = xreb.ErrCnt()
	)
	if cnt == 0 {
		nlog.Infoln(rargs.logHdr, "fini => quiesce")
	} else {
		nlog.Warningln(rargs.logHdr, "fini [", cnt, "] => quiesce")
	}

	// prior to closing streams
	ret := xreb.Quiesce(rargs.config.Transport.QuiesceTime.D(), qui.quicb)
	cnt = xreb.ErrCnt()

	// cleanup markers
	if ret != core.QuiAborted && ret != core.QuiTimeout {
		if errM := fs.RemoveMarker(fname.RebalanceMarker, tstats); errM == nil {
			nlog.Infoln(rargs.logHdr, "removed marker ok")
		}
		_ = fs.RemoveMarker(fname.NodeRestartedPrev, tstats)
	}

	reb.endStreams(err, rargs.logHdr)
	reb.filterGFN.Reset()

	xreb.ToStats(&stats)
	if stats.Objs > 0 || stats.OutObjs > 0 || stats.InObjs > 0 {
		s, e := jsoniter.MarshalIndent(&stats, "", " ")
		debug.AssertNoErr(e)
		nlog.Infoln(string(s))
	}
	reb.stages.stage.Store(rebStageDone)
	reb.stages.cleanup()

	reb.dm.UnregRecv()
	xreb.Finish()
	switch {
	case ret != core.QuiAborted && ret != core.QuiTimeout && cnt == 0:
		nlog.Infoln(rargs.logHdr, "done", xreb.String())
	case cnt == 0:
		nlog.Warningln(rargs.logHdr, "finished with errors: [ que =", ret, "]", xreb.String())
	default:
		nlog.Warningln(rargs.logHdr, "finished with errors: [ que =", ret, "errs =", cnt, "]", xreb.String())
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
	// limited scope
	if rj.rargs.bck != nil {
		rj.walkBck(rj.rargs.bck)
		return
	}
	// global
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

	// err
	if cmn.Rom.FastV(4, cos.SmoduleReb) || !cos.IsRetriableConnErr(err) {
		switch {
		case bundle.IsErrDestinationMissing(err):
			nlog.Errorf("%s: %v, %s", rj.xreb.Name(), err, rj.rargs.smap.StringEx())
		case cmn.IsErrStreamTerminated(err):
			rj.xreb.Abort(err)
			nlog.Errorln("stream term-ed: [", err, rj.xreb.Name(), "]")
		default:
			lom, ok := arg.(*core.LOM)
			debug.Assert(ok)
			nlog.Errorf("%s: %s failed to send %s: %v (%T)", core.T, rj.xreb.Name(), lom, err, err) // abort???
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
			nlog.Errorln(rj.rargs.logHdr, err)
			return err
		}
		return cmn.ErrSkip
	}
	// skip EC.Enabled bucket - leave the job for EC rebalance
	if lom.ECEnabled() {
		return filepath.SkipDir
	}
	// limited scope
	if rj.rargs.prefix != "" {
		debug.Assert(rj.rargs.bck != nil)
		if !cmn.ObjHasPrefix(lom.ObjName, rj.rargs.prefix) {
			//
			// TODO: unify via (fs.WalkBck => validateCb => cmn.DirHasOrIsPrefix)
			//
			i := strings.IndexByte(lom.ObjName, filepath.Separator)
			if i > 0 && !cmn.DirHasOrIsPrefix(lom.ObjName[:i], rj.rargs.prefix) {
				if cmn.Rom.FastV(4, cos.SmoduleReb) {
					nlog.Warningln(rj.rargs.logHdr, "skip-dir", lom.ObjName, "prefix", rj.rargs.prefix)
				}
				return filepath.SkipDir
			}
			return cmn.ErrSkip
		}
	}

	tsi, err := rj.rargs.smap.HrwHash2T(lom.Digest())
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
