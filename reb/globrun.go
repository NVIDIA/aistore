// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	ratomic "sync/atomic"
	"time"

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

// rebalance stage enum (see reb/stages.go for `isInStage` etc. helpers)
const (
	rebStageInactive = iota
	rebStageInit
	rebStageTraverse
	rebStagePostTraverse // post-traverse (post-Tx) barrier
	rebStageFin
	rebStageFinStreams
	rebStageDone
	rebStageAbort // one of targets aborts the rebalancing (never set, only sent)
)

type (
	Reb struct {
		smap      ratomic.Pointer[meta.Smap] // next smap (new that'll become current after rebalance)
		xreb      ratomic.Pointer[xs.Rebalance]
		dm        *bundle.DM
		filterGFN *prob.Filter
		ecClient  *http.Client
		stages    *nodeStages // (map[tid => stage]; my own stage)
		// (smap, xreb) + atomic state
		id atomic.Int64
		// quiescence
		lastrx atomic.Int64 // mono time
		// renewal and fini()
		mu sync.Mutex
	}
	ExtArgs struct {
		Tstats cos.StatsUpdater
		Notif  *xact.NotifXact
		Bck    *meta.Bck // advanced usage, limited scope
		Prefix string    // ditto
		Oxid   string    // oldRMD g[version]
		NID    int64     // newRMD version
		Flags  uint32    // xact.ArgsMsg.Flags
	}
)

const regOpaqueSize = 1 + cos.SizeofI64

type (
	rebJogger struct {
		rargs *rargs
		opts  fs.WalkOpts
		ver   int64
		wg    *sync.WaitGroup
	}
	// internal runtime context (lifecycle: a single reb.run())
	rargs struct {
		m      *Reb
		smap   *meta.Smap
		config *cmn.Config
		xreb   *xs.Rebalance
		bck    *meta.Bck           // advanced usage, limited scope
		nwp    *nwp                // num-workers parallelism (when not used read-and-transmit happens in joggers)
		avail  fs.MPI              // mountpaths
		logHdr string              // prefix "t[xyz] g[ID] smap V..." in log records
		prefix string              // ditto, as in: traverse only bck[/prefix]
		id     int64               // as in "g[id]"
		opaque [regOpaqueSize]byte // []byte{rebMsgRegular, rebID} => hdr.Opaque
		stats  rebStats            // observability: stage waiting times, counters via CtlMsg (`ais show job`)
		ecUsed bool
	}
)

var stages = map[uint32]string{
	rebStageInactive:     "<inactive>",
	rebStageInit:         "<init>",
	rebStageTraverse:     "<traverse>",
	rebStagePostTraverse: "<post-traverse>",
	rebStageFin:          "<fin>",
	rebStageFinStreams:   "<fin-streams>",
	rebStageDone:         "<done>",
	rebStageAbort:        "<abort>",
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

	return reb
}

// See README.md in this package for a sketch of execution flow and
// a note on _preempt() timeout.
const (
	preemptRetries = 16 // poll <= 16 seconds
)

func (*Reb) _preempt(logHdr, oxid string) error {
	const (
		tag = "preempt"
	)
	oxreb, err := xreg.GetXact(oxid)
	if err != nil {
		debug.AssertNoErr(err) // (can only be invalid xid)
		return err
	}
	if oxreb == nil || !oxreb.EndTime().IsZero() {
		return nil
	}
	oxreb.Abort(cmn.ErrXactRenewAbort)
	nlog.Warningln(logHdr, "[", cmn.ErrXactRenewAbort, oxreb.String(), "]")

	// reb.fini() always performs full cleanup, that's why we can just poll here
	// for valid end-time (not a sentinel indicating "almost done")
	smap1 := core.T.Sowner().Get()
	for i := range preemptRetries {
		time.Sleep(time.Second)
		if !oxreb.EndTime().IsZero() {
			return nil
		}

		if smap2 := core.T.Sowner().Get(); !smap1.SameTargets(smap2) {
			return fmt.Errorf("%s: Smap changed mid-wait %s => %s", tag, smap1.StringEx(), smap2.StringEx())
		}

		if i > 5 && i&1 == 1 {
			nlog.Warningf("%s: %s polling for %s ...", tag, logHdr, oxreb.String())
		}
	}
	return fmt.Errorf("%s: previous rebalance %q takes more than %v to finish", tag, oxreb.String(), preemptRetries*time.Second)
}

// Run() is the main method: serialized to execute one at a time (while possibly _preempting_
// currently running rebalance) and go through controlled enumerated stages.
//
// Prior to starting to run over this target's buckets (of user data), there's a certain startup
// phase that also entails constructing and opening a new data mover (DM), whereby
// (NewDM + RegRecv + Open) is an atomic generation-start transition.
//
// No other rebalance generation may observe or replace the DM between its registration and open;
// any failure (below) unregisters the same DM instance and zeros it before returning.
//
// A note on stage management:
//  1. Non-EC and EC rebalances run in parallel
//  2. Execution starts after the `Reb` sets the current stage to rebStageTraverse
//  3. Only EC rebalance changes the current stage
//  4. Global rebalance performs checks such as `stage > rebStageTraverse` or
//     `stage < rebStagePostTraverse`. Since all EC stages are between
//     `Traverse` and `PostTraverse` non-EC rebalance does not "notice" stage changes.
//
// See also: README.md in this package.
func (reb *Reb) Run(smap *meta.Smap, extArgs *ExtArgs) {
	if reb.rebID() == extArgs.NID {
		return
	}

	logHdr := reb.logHdr(extArgs.NID, smap, true /*initializing*/)
	// preempt
	if xact.IsValidRebID(extArgs.Oxid) {
		if err := reb._preempt(logHdr, extArgs.Oxid); err != nil {
			nlog.Errorln(logHdr, "failed to preempt:", err)
			return
		}
	}

	if extArgs.Bck != nil && extArgs.Bck.IsEmpty() {
		extArgs.Bck = nil
	}

	var (
		bmd = core.T.Bowner().Get()
		// runtime context (scope: this uniquely ID-ed rebalance)
		rargs = &rargs{
			m:      reb,
			id:     extArgs.NID, // == newRMD.Version
			smap:   smap,
			config: cmn.GCO.Get(),
			bck:    extArgs.Bck,    // advanced usage
			prefix: extArgs.Prefix, // ditto
			logHdr: logHdr,
			ecUsed: bmd.IsECUsed(),
		}
	)
	if rargs.bck != nil {
		debug.Assert(!rargs.bck.IsEmpty(), extArgs.Bck)
		rargs.logHdr += "::" + rargs.bck.Cname(rargs.prefix)
	}
	if !rargs.pingall() {
		return
	}
	if reb.rebID() == extArgs.NID {
		return
	}

	// transport.Hdr.Opaque: message kind + this rebalance ID (checked by recv)
	rargs.opaque[0] = rebMsgRegular
	binary.BigEndian.PutUint64(rargs.opaque[1:], uint64(rargs.id))

	haveStreams := smap.HasPeersToRebalance(core.T.SID())
	if bmd.IsEmpty() {
		haveStreams = false
	}
	if !reb.initRenew(rargs, extArgs, haveStreams) {
		return
	}
	if !haveStreams {
		debug.Assert(reb.dm == nil)
		// cleanup and leave
		nlog.Infof("%s: nothing to do: %s, %s", logHdr, smap.StringEx(), bmd.StringEx())
		reb.stages.stage.Store(rebStageDone)
		fs.RemoveMarker(fname.RebalanceMarker, extArgs.Tstats, false /*stopping*/)
		fs.RemoveMarker(fname.NodeRestartedPrev, extArgs.Tstats, false)
		rargs.xreb.Finish()
		return
	}

	debug.Assert(reb.dm != nil)
	debug.Assert(reb.dm.IsOpen())
	if extArgs.Bck == nil {
		nlog.Infoln(logHdr, "initializing")
	} else {
		nlog.Warningln(logHdr, "initializing - limited scope: [", extArgs.Bck.Cname(extArgs.Prefix), "]")
	}

	// abort all running xactions that have `AbortByReb: true`
	// in static descriptor table (xact.Table)
	xreg.AbortByNewReb(errors.New("reason: starting " + rargs.xreb.Name()))

	// only one rebalance is running -----------------

	reb.lastrx.Store(0)

	if rargs.ecUsed {
		ec.ECM.OpenStreams(true /*with refc*/)
	}
	onGFN()

	extArgs.Tstats.SetFlag(cos.NodeAlerts, cos.Rebalancing)

	// run: nwp workers (if any), lazy deletion, rebalance joggers (no-EC[, EC))
	if !rargs.ecUsed {
		reb.runNwp(rargs)
	}

	rargs.xreb.SetCtlMsgFn(rargs.ctlMsg) // TODO -- FIXME: pass via RenewRebalance()

	err := reb.run(rargs)

	if err == nil {
		// synchronize: wait for peers to finish traversing
		reb.changeStage(rargs, rebStagePostTraverse)
		nlog.Infof("%s: poll peers for stage >= %s", rargs.logHdr, stages[rebStagePostTraverse])
		errCnt := bcast(rargs, reb.waitPostTraverse)
		if err = rargs.xreb.AbortErr(); err != nil {
			nlog.Infoln(rargs.logHdr, "post-traverse abort", err, "num-fail", errCnt)
		} else if errCnt > 0 {
			nlog.Warningln(rargs.logHdr, "post-traverse num-fail", errCnt)
		}
	} else {
		nlog.Errorln(logHdr, "fail => stage-fin:", err)
	}
	reb.changeStage(rargs, rebStageFin)

	reb.fini(rargs, err, extArgs.Tstats)
	rargs.xreb.FinalCtlMsg()
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
func (reb *Reb) run(rargs *rargs) error {
	reb.stages.stage.Store(rebStageTraverse)
	rargs.stats.stage(rebStageTraverse)

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

func (reb *Reb) initRenew(rargs *rargs, extArgs *ExtArgs, haveStreams bool) bool {
	rns := xreg.RenewRebalance(rargs.id, &xreg.RebArgs{Bck: rargs.bck, Prefix: rargs.prefix, Flags: extArgs.Flags})
	if rns.Err != nil {
		return false
	}
	if rns.IsRunning() {
		return false
	}
	xctn := rns.Entry.Get()
	xreb, ok := xctn.(*xs.Rebalance)
	debug.Assert(ok)

	reb.mu.Lock() // ----------------------------
	origSmap, origStage := reb.smap.Load(), reb.stages.stage.Load()
	err := reb._renew(rargs, xreb, haveStreams)

	if err == nil {
		reb.mu.Unlock() // ok ------
		extArgs.Notif.Xact = xreb
		xreb.AddNotif(extArgs.Notif)
		nlog.Infoln(rargs.logHdr, "- running", xreb.String())
		return true
	}

	debug.Assert(reb.dm == nil)
	if reb.dm != nil { // (defensive; remove later)
		reb.dm.Close(err)
		reb.dm.UnregRecv()
		reb.dm = nil
	}
	if rargs.xreb == xreb {
		xreb.Abort(err)
		reb.setXact(nil)
	}
	reb.smap.Store(origSmap)
	reb.stages.stage.Store(origStage)
	reb.mu.Unlock() // fail ------

	nlog.Errorln(err)
	return false
}

func (reb *Reb) _renew(rargs *rargs, xreb *xs.Rebalance, haveStreams bool) error {
	if xreb.RebID() != rargs.id {
		return fmt.Errorf("reb-id mismatch: g[%d] != g[%d]", xreb.RebID(), rargs.id)
	}

	rargs.xreb = xreb

	// prior to opening streams:
	// not every change in Smap warants a different rebalance but this one (below) definitely does
	// check for post-renew change
	smap := core.T.Sowner().Get()
	if !smap.SameTargets(rargs.smap) {
		debug.Assert(smap.Version > rargs.smap.Version)
		return fmt.Errorf("%s post-renew change %s => %s", xreb, rargs.smap.StringEx(), smap.StringEx())
	}
	reb.smap.Store(rargs.smap)

	// 3. init streams and data structures
	reb.stages.stage.Store(rebStageInit)
	if haveStreams {
		extra := bundle.Extra{
			RecvAck:  reb.recvAckNtfn,
			Config:   rargs.config,
			Smap:     rargs.smap,
			XactConf: rargs.config.Rebalance.XactConf,
			OwnStats: true, // do not auto-increment In/OutObjs
		}
		debug.Assert(reb.dm == nil)
		reb.dm = bundle.NewDM(trname, reb.recvObj, cmn.OwtRebalance, extra)
		if err := reb.dm.RegRecv(); err != nil {
			reb.dm = nil
			return err
		}

		if err := reb.beginStreams(rargs); err != nil {
			reb.dm.UnregRecv()
			reb.dm = nil
			return err
		}
	}

	// 4. create persistent mark
	fatalErr, warnErr := fs.PersistMarker(fname.RebalanceMarker, true /*quiet*/)
	if fatalErr != nil {
		_, _ = reb.endStreams(rargs, fatalErr)
		return fatalErr
	}
	if warnErr != nil {
		nlog.Warningln(core.T.String(), xreb.Name(), "[ mark err:", warnErr, "]")
	}

	reb.setXact(xreb)
	reb.id.Store(rargs.id)

	// 5. ready - can receive objects
	reb.stages.cleanup()
	return nil
}

func (reb *Reb) beginStreams(rargs *rargs) error {
	if reb.stages.stage.Load() != rebStageInit {
		return fmt.Errorf("%s: cannot begin at stage %d", rargs.logHdr, reb.stages.stage.Load())
	}

	reb.dm.SetXact(rargs.xreb)
	reb.dm.Open()
	return nil
}

func (reb *Reb) endStreams(rargs *rargs, err error) (ok bool, stage uint32) {
	stage = reb.stages.stage.Load()
	ok = reb.stages.stage.CAS(rebStageFin, rebStageFinStreams)

	debug.Assert(reb.dm != nil || !ok, "rebalance fin-streams without DM")

	if reb.dm != nil {
		if ok {
			rargs.stats.stage(rebStageFinStreams)
		}
		reb.dm.Close(err)
		reb.dm.UnregRecv()
		reb.dm = nil
		if ok {
			rargs.stats.finalize()
		}
	}
	return
}

// when at least one bucket has EC enabled
func (reb *Reb) runEC(rargs *rargs) error {
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
func (reb *Reb) runNoEC(rargs *rargs) error {
	debug.Assert(rargs.m == reb)
	nlog.Infoln(rargs.logHdr, "start no-ec run")

	errCnt := bcast(rargs, reb.rxReady) // ignore timeout
	if err := rargs.xreb.AbortErr(); err != nil {
		nlog.Infoln(rargs.logHdr, "abort rx-ready", err, "num-fail", errCnt)
		return err
	}
	if errCnt > 0 {
		nlog.Errorln(rargs.logHdr, "rx-ready num-fail:", errCnt)
	}
	var (
		wg  = &sync.WaitGroup{}
		ver = rargs.smap.Version
	)
	for _, mi := range rargs.avail {
		rl := &rebJogger{
			rargs: rargs,
			ver:   ver,
			wg:    wg,
		}
		wg.Add(1)
		go rl.jog(mi)
	}
	wg.Wait()

	// drain workers
	if nwp := rargs.nwp; nwp != nil {
		close(nwp.workCh)
		nwp.wg.Wait()
		debug.Assert(len(rargs.nwp.workCh) == 0)
	}

	if err := rargs.xreb.AbortErr(); err != nil {
		nlog.Warningln(rargs.logHdr, "finish no-ec run, abort joggers: [", err, "]")
		return err
	}
	nlog.Infoln(rargs.logHdr, "finish no-ec run")
	return nil
}

func (reb *Reb) fini(rargs *rargs, err error, tstats cos.StatsUpdater) {
	var (
		stats core.Stats
		qui   = &qui{rargs: rargs, reb: reb}
		xreb  = rargs.xreb
		ecnt  = xreb.ErrCnt()
	)
	if ecnt == 0 {
		nlog.Infoln(rargs.logHdr, "fini => quiesce")
	} else {
		nlog.Warningln(rargs.logHdr, "fini [ errors:", ecnt, "] => quiesce")
	}

	// prior to closing streams
	que := xreb.Quiesce(rargs.config.Transport.QuiesceTime.D(), qui.quicb)
	ecnt = xreb.ErrCnt()

	// cleanup markers
	if que != core.QuiAborted && que != core.QuiTimeout {
		if fs.RemoveMarker(fname.RebalanceMarker, tstats, false /*stopping*/) {
			nlog.Infoln(rargs.logHdr, "removed marker ok")
		}
		_ = fs.RemoveMarker(fname.NodeRestartedPrev, tstats, false /*stopping*/)
	}

	reb.mu.Lock() // ---------------------------------------

	xctn := reb.xctn()
	debug.Assert(xctn != nil && xctn.ID() == xreb.ID())

	// Close and Rx-unregister data mover:
	// - inbound drain is bounded by transport.Quiescent (Config.Transport.QuiesceTime. default 10s) of inbound silence
	// - enforced by UnregRecv inside endStreams;
	// - Stage transitions to PostTraverse signal "no more new sends";
	// - the receiver-side quiesce in UnregRecv catches everything in flight at that moment
	ok, curStage := reb.endStreams(rargs, err)

	reb.filterGFN.Reset()

	xreb.ToStats(&stats)

	var finStats string
	if stats.Objs > 0 || stats.OutObjs > 0 || stats.InObjs > 0 {
		s, e := jsoniter.MarshalIndent(&stats, "", " ")
		debug.AssertNoErr(e)
		finStats = string(s)
	}
	reb.stages.stage.Store(rebStageDone)
	reb.stages.cleanup()

	xreb.Finish()

	xname := xreb.String()

	reb.setXact(nil)
	reb.smap.Store(nil)

	reb.mu.Unlock() // ---------------------------------------

	if !ok {
		nlog.Warningln(rargs.logHdr, "ended streams when curr. stage:", stages[curStage])
	}
	if finStats != "" {
		nlog.Infoln(finStats)
	}
	if nwp := rargs.nwp; nwp != nil {
		if a := nwp.chanFull.Load(); a > 0 {
			nlog.Warningln(rargs.logHdr, "work channel full (final)", a)
		}
	}
	switch {
	case que != core.QuiAborted && que != core.QuiTimeout && ecnt == 0:
		nlog.Infoln(rargs.logHdr, "done", xname)
	default:
		nlog.Warningln(rargs.logHdr, "finished [ que:", que, "errors:", ecnt, "]", xname)
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
		rj.opts.CTs = []string{fs.ObjCT}
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
	xreb := rj.rargs.xreb
	if err == nil {
		return xreb.IsAborted()
	}
	if xreb.IsAborted() {
		nlog.Infoln(xreb.Name(), "aborting traversal")
	} else {
		nlog.Errorln(core.T.String(), xreb.Name(), "failed to traverse", err)
	}
	return true
}

func (rj *rebJogger) visitObj(fqn string, de fs.DirEntry) error {
	xreb := rj.rargs.xreb
	if err := xreb.AbortErr(); err != nil {
		nlog.Infoln(xreb.Name(), "rj-walk-visit aborted", err)
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

	rargs := rj.rargs

	// limited scope
	if rargs.prefix != "" {
		debug.Assert(rargs.bck != nil)
		if !cmn.ObjHasPrefix(lom.ObjName, rargs.prefix) {
			//
			// TODO: unify via (fs.WalkBck => validateCb => cmn.DirHasOrIsPrefix)
			//
			i := strings.IndexByte(lom.ObjName, filepath.Separator)
			if i > 0 && !cmn.DirHasOrIsPrefix(lom.ObjName[:i], rargs.prefix) {
				if cmn.Rom.V(4, cos.ModReb) {
					nlog.Warningln(rargs.logHdr, "skip-dir", lom.ObjName, "prefix", rargs.prefix)
				}
				return filepath.SkipDir
			}
			return cmn.ErrSkip
		}
	}

	tsi, err := rargs.smap.HrwHash2T(lom.Digest())
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
	m := rargs.m
	if m.filterGFN.Lookup(*bname) {
		m.filterGFN.Delete(*bname)
		return cmn.ErrSkip
	}

	// nwp: delegate to a worker
	if nwp := rargs.nwp; nwp != nil {
		l, c := len(nwp.workCh), cap(nwp.workCh)
		nwp.chanFull.Check(l, c) // TODO: consider to-jogger "fallback" when full
		nwp.workCh <- wi{lom, tsi}
		return nil
	}

	// no workers: jogger does everything
	var roc cos.ReadOpenCloser
	if roc, err = getROC(lom); err != nil { // rlock, load, new roc
		return err
	}

	// transmit (unlock via transport completion => roc.Close)
	if err := rargs.doSend(lom, tsi, roc); err != nil {
		rargs.xreb.Abort(err) // NOTE: failure to send == abort
		return err
	}

	return nil
}

// takes rlock and keeps it _iff_ successful
func getROC(lom *core.LOM) (roc cos.ReadOpenCloser, err error) {
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
		if _, err = lom.ComputeSetCksum(true); err != nil {
			lom.Unlock(false)
			return
		}
	}
	debug.Assert(lom.Checksum() != nil, lom.String())
	return lom.NewDeferROC(true /*loaded*/)
}

///////////
// rargs //
///////////

func (rargs *rargs) pingall() bool {
	debug.Assert(rargs.smap.Version > 0) // validated in _runRe

	// whether other targets are up and running
	if errCnt := bcast(rargs, _pingTarget); errCnt > 0 {
		nlog.Errorln(rargs.logHdr, "not starting: ping err-s", errCnt)
		return false
	}
	rargs.avail = fs.GetAvail()
	return true
}

func (rargs *rargs) doSend(lom *core.LOM, tsi *meta.Snode, roc cos.ReadOpenCloser) error {
	debug.Assert(tsi.ID() != core.T.SID(), "unexpected local destination")
	var (
		m = rargs.m
		o = transport.AllocSend()
	)
	o.Hdr.Bck.Copy(lom.Bucket())
	o.Hdr.ObjName = lom.ObjName
	o.Hdr.Opaque = rargs.opaque[:]
	o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
	o.SentCB, o.CmplArg = rargs.objSentCallback, lom
	return m.dm.Send(o, roc, tsi)
}

// send completion
func (rargs *rargs) objSentCallback(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	lom, ok := arg.(*core.LOM)
	debug.Assert(ok)
	if err == nil {
		rargs.xreb.OutObjsAdd(1, hdr.ObjAttrs.Size)
		if ok {
			core.FreeLOM(lom)
		}
		return
	}

	// err
	if cmn.Rom.V(4, cos.ModReb) || !cos.IsErrRetriableConn(err) {
		if !rargs.xreb.IsAborted() {
			switch {
			case bundle.IsErrDestinationMissing(err):
				nlog.Errorf("%s: %v, %s", rargs.xreb.Name(), err, rargs.smap.StringEx())
			case cmn.IsErrStreamTerminated(err):
				rargs.xreb.Abort(err)
				nlog.Errorln("stream term-ed: [", err, rargs.xreb.Name(), "]")
			default:
				nlog.Errorf("%s: %s failed to send %s: %v (%T)", core.T, rargs.xreb.Name(), lom, err, err) // abort???
			}
		}
	}
	if ok {
		core.FreeLOM(lom)
	}
}
