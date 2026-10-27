// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
//
// - Support num-workers parallelism (nwp) - add runNwpCleanup(), clnWorker, and
//   `worker.do = worker.doCleanup`
//
// - Lifecycle helper extraction. Run() and RunCleanup() share preempt/refuse
//   detection, rargs construction, pingall, initRenew, marker cleanup.
//
// - Pre-initRenew refuse path produces no xaction record. RMD gets bumped,
//   but `ais show job <rebID>` will show nothing.
//
// - CLI: jobStartRebalance.Usage inline examples
// - Docs: "rebalance retracing its own steps" motivation; EC skip; features

type (
	clnStats struct {
		// visits
		visits atomic.Int64
		loads  atomic.Int64
		// skip
		skipBusy     atomic.Int64
		skipBusyLate atomic.Int64
		skipChanged  atomic.Int64
		skipFntl     atomic.Int64
		// keep despite
		keepPeerMissing atomic.Int64
		keepDiverged    atomic.Int64
		// remove
		removeMisplaced atomic.Int64
		removeDiverged  atomic.Int64
		// errors
		errLoad   atomic.Int64
		errHEAD   atomic.Int64
		errRemove atomic.Int64
	}
	clnArgs struct {
		rargs
		stats clnStats
		hdb   ratomic.Pointer[core.HdbBatcher] // the head-batcher of the run (ctlMsg reads it at any time)
		force bool                             // xact.ArgsMsg.Force (to remove diverged)
	}
	clnJogger struct {
		rebJogger
		clnArgs *clnArgs
	}

	// clnWorker struct { rebWorker }
)

// RunCleanup walks mountpaths and removes local copies of objects whose HRW
// target already has them. Piggy-backs on the rebalance lifecycle (xreg slot,
// markers, smap snapshot, abort plumbing) but is not a migration: no DM, no
// streams, no GFN, no cross-target post-traverse synchronization.
//
// Cleanup mode is the post-#288 / post-lomAcks-removal recovery tool: an
// operator-driven pass that reclaims source-side leftovers from a prior
// rebalance using head-batch (HeadObjT2T for a copy that is not on its HRW
// mountpath) as the per-LOM safety check.
//
// See also: 'ais space-cleanup' (recommended for routine use).
func (reb *Reb) RunCleanup(smap *meta.Smap, extArgs *ExtArgs, force bool) {
	if reb.rebID() == extArgs.NID {
		return
	}

	logHdr := reb.logHdrCleanup(extArgs.NID, smap, force)

	// refuse-on-prior: cleanup must NOT supersede a running rebalance.
	// Migration's _preempt aborts a stale prior generation; cleanup refuses instead.
	if xact.IsValidRebID(extArgs.Oxid) {
		oxreb, err := xreg.GetXact(extArgs.Oxid)
		if err != nil {
			debug.AssertNoErr(err) // (can only be invalid xid)
			nlog.Errorln(logHdr, err)
			return
		}
		if oxreb != nil && oxreb.EndTime().IsZero() {
			err := cmn.NewErrBusy("rebalance", oxreb.String())
			nlog.Errorln(logHdr, "refuse cleanup:", err)
			return
		}
	}

	// refuse on actively running resilver
	if marked := xreg.GetResilverMarked(); marked.Xact != nil {
		err := cmn.NewErrBusy("resilver", marked.Xact.Name())
		nlog.Errorln(logHdr, "refuse cleanup:", err)
		return
	}

	if extArgs.Bck != nil && extArgs.Bck.IsEmpty() {
		extArgs.Bck = nil
	}

	clnArgs := &clnArgs{
		rargs: rargs{
			m:      reb,
			id:     extArgs.NID, // == newRMD.Version
			smap:   smap,
			config: cmn.GCO.Get(),
			bck:    extArgs.Bck,    // advanced usage (limited scope)
			prefix: extArgs.Prefix, // ditto
			logHdr: logHdr,
		},
		force: force,
	}
	if clnArgs.bck != nil {
		debug.AssertFunc(func() bool { return !clnArgs.bck.IsEmpty() }, extArgs.Bck)
		clnArgs.logHdr += "::" + clnArgs.bck.Cname(clnArgs.prefix)
	}
	if !clnArgs.pingall() {
		return
	}
	if reb.rebID() == extArgs.NID {
		return
	}

	// initRenew with haveStreams=false: registers xaction, takes smap snapshot,
	// creates the RebalanceMarker - all without DM/streams.
	if !reb.initRenew(&clnArgs.rargs, extArgs, clnArgs.ctlMsg, false /*haveStreams*/) {
		return
	}

	if extArgs.Bck == nil {
		nlog.Infoln(logHdr, "initializing cleanup")
	} else {
		nlog.Warningln(logHdr, "initializing cleanup - limited scope: [", extArgs.Bck.Cname(extArgs.Prefix), "]")
	}

	// starting cleanup: set Rebalancing and clear a stale RebalanceInterrupted
	// left over from a previously aborted/interrupted run (or a node restart)
	extArgs.Tstats.SetClrFlag(cos.NodeAlerts, cos.Rebalancing, cos.RebalanceInterrupted)

	// walk mpaths with the cleanup visitor
	reb.runCleanup(clnArgs)

	reb.finiCleanup(clnArgs, extArgs.Tstats)
	extArgs.Tstats.ClrFlag(cos.NodeAlerts, cos.Rebalancing)
	clnArgs.xreb.FinalCtlMsg()
}

// format: "%s[g%d,v%d,mode=cleanup[-force]]"
func (*Reb) logHdrCleanup(rebID int64, smap *meta.Smap, force bool) string {
	var (
		sb cos.SB
		l  = 64
	)
	sb.Init(l)
	sb.WriteString(core.T.String())
	sb.WriteString("[g")
	sb.WriteInt64(rebID)
	sb.WriteUint8(',')
	if smap != nil {
		sb.WriteUint8('v')
		sb.WriteInt64(smap.Version)
	} else {
		sb.WriteString("v<???>")
	}
	if force {
		sb.WriteString(",mode=cleanup-force]")
	} else {
		sb.WriteString(",mode=cleanup]")
	}
	return sb.String()
}

func (reb *Reb) runCleanup(clnArgs *clnArgs) {
	debug.Assert(clnArgs.m == reb)
	nlog.Infoln(clnArgs.logHdr, "start cleanup run")

	var (
		wg  = &sync.WaitGroup{}
		ver = clnArgs.smap.Version
	)
	clnArgs.initHdb()
	for _, mi := range clnArgs.avail {
		cl := &clnJogger{
			rebJogger: rebJogger{rargs: &clnArgs.rargs, ver: ver, wg: wg},
			clnArgs:   clnArgs,
		}
		cl.opts.Callback = cl.visitObj
		wg.Add(1)
		go cl.rebJogger.jog(mi)
	}
	wg.Wait()

	var sb cos.SB
	sb.Init(128)
	st := clnArgs.hdb.Load().End()
	st.Str(&sb, '=')
	if err := clnArgs.xreb.AbortErr(); err != nil {
		nlog.Warningln(clnArgs.logHdr, "finish cleanup run, abort joggers: [", err, "]", sb.String())
	} else {
		nlog.Infoln(clnArgs.logHdr, "finish cleanup run", sb.String())
	}
}

func (reb *Reb) finiCleanup(clnArgs *clnArgs, tstats cos.StatsUpdater) {
	var (
		xreb = clnArgs.xreb
		ecnt = xreb.ErrCnt()
	)
	if !xreb.IsAborted() {
		// TODO: fix race condition where a stop request arrives after Quiesce() returns but before xreb.Finish().
		// The marker is already removed here, but the xaction is still running, so the stop is accepted.
		// Finish() then records it as aborted while the marker and Prometheus flag indicate success.
		if fs.RemoveMarker(fname.RebalanceMarker, tstats, false /*stopping*/) {
			nlog.Infoln(clnArgs.logHdr, "removed marker ok")
		}
		_ = fs.RemoveMarker(fname.NodeRestartedPrev, tstats, false /*stopping*/)
	} else {
		tstats.SetFlag(cos.NodeAlerts, cos.RebalanceInterrupted)
	}

	reb.mu.Lock()

	xctn := reb.xctn()
	debug.AssertFunc(func() bool { return xctn != nil && xctn.ID() == xreb.ID() })

	xreb.Finish()
	xname := xreb.String()

	reb.setXact(nil)
	reb.smap.Store(nil)

	reb.mu.Unlock()

	switch {
	case !xreb.IsAborted() && ecnt == 0:
		nlog.Infoln(clnArgs.logHdr, "done", xname)
	default:
		nlog.Warningln(clnArgs.logHdr, "finished [ aborted:", xreb.IsAborted(), "errors:", ecnt, "]", xname)
	}
}

///////////////
// clnJogger //
///////////////

func (j *clnJogger) visitObj(fqn string, de fs.DirEntry) error {
	xreb := j.rargs.xreb
	if err := xreb.AbortErr(); err != nil {
		return err
	}
	if de.IsDir() {
		return nil
	}
	lom := core.AllocLOM(fqn)
	err := j._lwalk(lom, fqn)
	core.FreeLOM(lom)

	if err == cmn.ErrSkip {
		err = nil
	}
	return err
}

func (j *clnJogger) _lwalk(lom *core.LOM, fqn string) error {
	if err := lom.InitFQN(fqn, nil); err != nil {
		if cmn.IsErrBucketLevel(err) {
			nlog.Errorln(j.rargs.logHdr, err)
			return err
		}
		return cmn.ErrSkip
	}

	stats := &j.clnArgs.stats
	stats.visits.Inc()

	// skip entire dir
	if lom.ECEnabled() {
		return filepath.SkipDir
	}

	rargs := j.rargs

	// limited scope (TODO: consider a shared helper w/ migration)
	if rargs.prefix != "" {
		debug.Assert(rargs.bck != nil)
		if !cmn.ObjHasPrefix(lom.ObjName, rargs.prefix) {
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

	tsi, err := rargs.smap.HrwObjHash2T(lom.Bck(), lom.Digest())
	if err != nil {
		rargs.xreb.Abort(err) // smap unusable — abort the whole xaction
		return err
	}
	if tsi.ID() == core.T.SID() {
		return cmn.ErrSkip
	}

	if !lom.IsHRW() {
		return j.clnArgs.verifyRemove(lom, tsi)
	}
	return j.clnArgs.verifyAdd(lom, tsi)
}

/////////////
// clnArgs //
/////////////

// create the head-batcher of the run
func (clnArgs *clnArgs) initHdb() {
	senders, err := xact.TuneNumWorkers(clnArgs.xreb.Name(), xact.NwpDflt, len(clnArgs.avail))
	if err != nil {
		nlog.Warningln(clnArgs.logHdr, err, "[ head-batch: one sender ]")
	}
	clnArgs.hdb.Store(core.NewHdbBatcher(&core.HdbArgs{
		Cb:      clnArgs.peerReply,
		Abort:   clnArgs.xreb,
		Smap:    clnArgs.smap,
		LogHdr:  clnArgs.logHdr,
		Mod:     cos.ModReb,
		Senders: max(senders, 1), // NwpNone or an error: one sender
	}))
}

// head-batch: queue a misplaced copy on its HRW mountpath
func (clnArgs *clnArgs) verifyAdd(lom *core.LOM, tsi *meta.Snode) error {
	if !clnArgs.snap(lom, nil) {
		return cmn.ErrSkip
	}
	clnArgs.hdb.Load().Add(lom, tsi)
	return nil
}

// head-batch callback: remove the local copy that the peer has
// - runs on a head-batch sender
// - must not call Add or End (see core/headbatch.go)
func (clnArgs *clnArgs) peerReply(ent *core.HdbEnt) {
	var (
		diverged error
		stats    = &clnArgs.stats
	)
	switch ent.Status {
	case apc.HdbSame:
	case apc.HdbDiverged:
		if !clnArgs.force {
			cnt := stats.keepDiverged.Inc()
			cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, "diverged:", ent.LIF.Cname(), "peer:", ent.Tsi.StringEx(), ent.Cause, "[ keep:", cnt, "]")
			return
		}
		diverged = errors.New(ent.Cause)
	case apc.HdbMissing:
		stats.keepPeerMissing.Inc()
		return
	case apc.HdbBusy: // the peer could not lock it
		if cmn.Rom.V(4, cos.ModReb) {
			nlog.Infoln(clnArgs.logHdr, "peer busy, keep:", ent.LIF.Cname(), "peer:", ent.Tsi.StringEx())
		}
		return
	default: // no answer or failed
		cnt := stats.errHEAD.Inc()
		cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, ent.Tsi.StringEx(), "head-batch(", ent.LIF.Cname(), ") returned", ent.Status, ent.Cause)
		return
	}
	lom, err := ent.LIF.LOM()
	if err != nil {
		stats.errLoad.Inc()
		return
	}
	clnArgs.remove(lom, ent.Unchanged, diverged)
	core.FreeLOM(lom)
}

// per-object HEAD: verify a misplaced copy and remove it
// returns nil (removed) or cmn.ErrSkip (kept)
func (clnArgs *clnArgs) verifyRemove(lom *core.LOM, tsi *meta.Snode) error {
	var (
		diverged error
		oa       cmn.ObjAttrs
		stats    = &clnArgs.stats
	)
	if !clnArgs.snap(lom, &oa) {
		return cmn.ErrSkip
	}
	if lom.IsCopy() {
		return cmn.ErrSkip
	}

	// check the expected location, request specific props to establish identity
	op, err := core.T.HeadObjT2T(lom, tsi,
		apc.GetPropsSize, apc.GetPropsChecksum, apc.GetPropsVersion, apc.GetPropsCustom, apc.GetPropsETag)
	if err != nil {
		if cmn.IsErrHTTPNotFound(err) {
			stats.keepPeerMissing.Inc()
		} else {
			cnt := stats.errHEAD.Inc()
			cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, tsi.StringEx(), "HEAD(", lom.Cname(), ") returned", err)
		}
		return cmn.ErrSkip
	}

	// identical?
	if eqErr := oa.CheckEq(op); eqErr != nil {
		if !clnArgs.force {
			cnt := stats.keepDiverged.Inc()
			cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, "diverged:", lom.Cname(), "peer:", tsi.StringEx(), eqErr, "[ keep:", cnt, "]")
			return cmn.ErrSkip
		}
		diverged = eqErr
	}

	unchanged := func(lom *core.LOM) bool { return lom.ObjAttrs().EqLocal(&oa) }
	if !clnArgs.remove(lom, unchanged, diverged) {
		return cmn.ErrSkip
	}
	return nil
}

// load the copy under the read lock
func (clnArgs *clnArgs) snap(lom *core.LOM, oa *cmn.ObjAttrs) bool {
	stats := &clnArgs.stats
	if !lom.TryLock(false) {
		stats.skipBusy.Inc()
		return false
	}
	err := lom.Load(false /*cache*/, true /*locked*/)
	if err == nil && oa != nil {
		oa.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
	}
	lom.Unlock(false)

	if err != nil {
		if !cos.IsNotExist(err) {
			stats.errLoad.Inc()
		}
		return false
	}
	stats.loads.Inc()

	if lom.IsLoadedUnresolvedFntl() {
		stats.skipFntl.Inc()
		return false
	}
	return true
}

// remove the copy under the write lock, unless it changed after the snap
func (clnArgs *clnArgs) remove(lom *core.LOM, unchanged func(*core.LOM) bool, diverged error) bool {
	stats := &clnArgs.stats
	if !lom.TryLock(true) {
		stats.skipBusyLate.Inc()
		return false
	}
	defer lom.Unlock(true)

	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		if !cos.IsNotExist(err) {
			stats.errLoad.Inc()
		}
		return false
	}
	if !unchanged(lom) {
		cnt := stats.skipChanged.Inc()
		cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, "changed while unlocked, keep:", lom.Cname(), "[ changed:", cnt, "]")
		return false
	}

	size := lom.Lsize()
	if errRm := lom.RemoveObj(); errRm != nil {
		cnt := stats.errRemove.Inc()
		cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, "remove failed:", lom.Cname(), errRm, "[ failures:", cnt, "]")
		return false
	}
	if diverged != nil {
		cnt := stats.removeDiverged.Inc()
		cmn.SparseWarn(cos.ModReb, cnt, clnArgs.logHdr, "force-removed diverged:", lom.Cname(), diverged, "[ forced:", cnt, "]")
	}

	clnArgs.xreb.ObjsAdd(1, size)
	stats.removeMisplaced.Inc()
	return true
}

// xreb.CtlMsg() callback (set via xreg.RebArgs; compare with reb/ctlmsg.go)
func (clnArgs *clnArgs) ctlMsg(sb *cos.SB) {
	if sb.Len() > 0 {
		sb.WriteString("; ")
	}

	sb.WriteString(core.T.String())
	if clnArgs.force {
		sb.WriteString(":cleanup-force")
	} else {
		sb.WriteString(":cleanup")
	}
	xreb := clnArgs.xreb
	if xreb.IsAborted() {
		sb.WriteString(" aborted")
	} else if xreb.IsDone() {
		sb.WriteString(" done")
	}

	s := &clnArgs.stats

	// visits
	sb.WriteString(" visits=")
	sb.WriteInt64(s.visits.Load())
	sb.WriteString(" loads=")
	sb.WriteInt64(s.loads.Load())

	// remove
	if v := s.removeMisplaced.Load(); v > 0 {
		sb.WriteString(" removed=")
		sb.WriteInt64(v)
	}
	if v := s.removeDiverged.Load(); v > 0 {
		sb.WriteString(" removed-diverged=")
		sb.WriteInt64(v)
	}

	// keep
	if v := s.keepPeerMissing.Load(); v > 0 {
		sb.WriteString(" keep-peer-missing=")
		sb.WriteInt64(v)
	}
	if v := s.keepDiverged.Load(); v > 0 {
		sb.WriteString(" keep-diverged=")
		sb.WriteInt64(v)
	}
	if v := s.skipBusy.Load(); v > 0 {
		sb.WriteString(" skip-busy=")
		sb.WriteInt64(v)
	}
	if v := s.skipBusyLate.Load(); v > 0 {
		sb.WriteString(" skip-busy-late=")
		sb.WriteInt64(v)
	}
	if v := s.skipChanged.Load(); v > 0 {
		sb.WriteString(" skip-changed=")
		sb.WriteInt64(v)
	}
	if v := s.skipFntl.Load(); v > 0 {
		sb.WriteString(" skip-fntl=")
		sb.WriteInt64(v)
	}

	// errors
	if ecnt := xreb.ErrCnt(); ecnt > 0 {
		sb.WriteString(" errs:")
		sb.WriteInt(ecnt)
	}
	// per-class error counters (worth showing separately because xreb.ErrCnt
	// may not include all of these — depending on which paths call AddErr)
	if v := s.errLoad.Load(); v > 0 {
		sb.WriteString(" err-load=")
		sb.WriteInt64(v)
	}
	if v := s.errHEAD.Load(); v > 0 {
		sb.WriteString(" err-head=")
		sb.WriteInt64(v)
	}
	if v := s.errRemove.Load(); v > 0 {
		sb.WriteString(" err-remove=")
		sb.WriteInt64(v)
	}

	// head-batch
	if hb := clnArgs.hdb.Load(); hb != nil {
		st := hb.Stats()
		st.Str(sb, '=')
	}
}
