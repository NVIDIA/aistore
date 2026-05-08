// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"

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
// - Throttling. Cleanup hits HeadObjT2T per LOM; N joggers across N targets.
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
		skipBusy atomic.Int64
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
	}
	clnJogger struct {
		rebJogger
		stats *clnStats
		force bool // xact.ArgsMsg.Force (to remove diverged)
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
// rebalance using HeadObjT2T as the per-LOM safety check.
//
// See also: 'ais space-cleanup' (recommended for routine use).
func (reb *Reb) RunCleanup(smap *meta.Smap, extArgs *ExtArgs, force bool) {
	if reb.rebID() == extArgs.NID {
		return
	}

	logHdr := reb.logHdrCleanup(extArgs.NID, smap)

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
	}
	if clnArgs.bck != nil {
		debug.Assert(!clnArgs.bck.IsEmpty(), extArgs.Bck)
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

	// walk mpaths with the cleanup visitor
	reb.runCleanup(clnArgs, force)

	reb.finiCleanup(clnArgs, extArgs.Tstats)
	clnArgs.xreb.FinalCtlMsg()
}

// format: "%s[g%d,v%d,mode=cleanup]"
func (*Reb) logHdrCleanup(rebID int64, smap *meta.Smap) string {
	var (
		sb cos.SB
		l  = 64
	)
	sb.Init(l)
	sb.WriteString(core.T.String())
	sb.WriteString("[g")
	sb.WriteString(strconv.FormatInt(rebID, 10))
	sb.WriteUint8(',')
	if smap != nil {
		sb.WriteUint8('v')
		sb.WriteString(strconv.FormatInt(smap.Version, 10))
	} else {
		sb.WriteString("v<???>")
	}
	sb.WriteString(",mode=cleanup]")
	return sb.String()
}

func (reb *Reb) runCleanup(clnArgs *clnArgs, force bool) {
	debug.Assert(clnArgs.m == reb)
	nlog.Infoln(clnArgs.logHdr, "start cleanup run")

	var (
		wg  = &sync.WaitGroup{}
		ver = clnArgs.smap.Version
	)
	for _, mi := range clnArgs.avail {
		cl := &clnJogger{
			rebJogger: rebJogger{rargs: &clnArgs.rargs, ver: ver, wg: wg},
			stats:     &clnArgs.stats,
			force:     force,
		}
		cl.opts.Callback = cl.visitObj
		wg.Add(1)
		go cl.rebJogger.jog(mi)
	}
	wg.Wait()

	if err := clnArgs.xreb.AbortErr(); err != nil {
		nlog.Warningln(clnArgs.logHdr, "finish cleanup run, abort joggers: [", err, "]")
	} else {
		nlog.Infoln(clnArgs.logHdr, "finish cleanup run")
	}
}

func (reb *Reb) finiCleanup(clnArgs *clnArgs, tstats cos.StatsUpdater) {
	var (
		xreb = clnArgs.xreb
		ecnt = xreb.ErrCnt()
	)
	if !xreb.IsAborted() {
		if fs.RemoveMarker(fname.RebalanceMarker, tstats, false /*stopping*/) {
			nlog.Infoln(clnArgs.logHdr, "removed marker ok")
		}
		_ = fs.RemoveMarker(fname.NodeRestartedPrev, tstats, false /*stopping*/)
	}

	reb.mu.Lock()

	xctn := reb.xctn()
	debug.Assert(xctn != nil && xctn.ID() == xreb.ID())

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

func (cl *clnJogger) visitObj(fqn string, de fs.DirEntry) error {
	xreb := cl.rargs.xreb
	if err := xreb.AbortErr(); err != nil {
		return err
	}
	if de.IsDir() {
		return nil
	}
	lom := core.AllocLOM(fqn)
	err := cl._lwalk(lom, fqn)
	core.FreeLOM(lom)

	if err == cmn.ErrSkip {
		err = nil
	}
	return err
}

func (cl *clnJogger) _lwalk(lom *core.LOM, fqn string) error {
	if err := lom.InitFQN(fqn, nil); err != nil {
		if cmn.IsErrBucketLevel(err) {
			nlog.Errorln(cl.rargs.logHdr, err)
			return err
		}
		return cmn.ErrSkip
	}
	cl.stats.visits.Inc()

	// skip entire dir
	if lom.ECEnabled() {
		return filepath.SkipDir
	}

	rargs := cl.rargs

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

	tsi, err := rargs.smap.HrwHash2T(lom.Digest())
	if err != nil {
		rargs.xreb.Abort(err) // smap unusable — abort the whole xaction
		return err
	}
	if tsi.ID() == core.T.SID() {
		return cmn.ErrSkip
	}

	// lock
	if !lom.TryLock(true) {
		cl.stats.skipBusy.Inc()
		return cmn.ErrSkip
	}
	defer lom.Unlock(true)

	if err := lom.Load(false /*cache*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return cmn.ErrSkip
		}
		cl.stats.errLoad.Inc()
		return cmn.ErrSkip
	}
	cl.stats.loads.Inc()

	// check the expected location, request specific props to establish identity
	op, err := core.T.HeadObjT2T(lom, tsi,
		apc.GetPropsSize, apc.GetPropsChecksum, apc.GetPropsVersion, apc.GetPropsCustom, apc.GetPropsETag)
	if err != nil {
		if cmn.IsErrHTTPNotFound(err) {
			cl.stats.keepPeerMissing.Inc()
		} else {
			cnt := cl.stats.errHEAD.Inc()
			sparseWarn(cnt, tsi.StringEx(), "HEAD(", lom.Cname(), ") returned", err)
		}
		return cmn.ErrSkip
	}

	// identical?
	if eqErr := lom.ObjAttrs().CheckEq(op); eqErr != nil {
		if !cl.force {
			cnt := cl.stats.keepDiverged.Inc()
			sparseWarn(cnt, cl.rargs.logHdr, "diverged:", lom.Cname(), "peer:", tsi.StringEx(), eqErr, "[ keep:", cnt, "]")
			return cmn.ErrSkip
		}
		cnt := cl.stats.removeDiverged.Inc()
		sparseWarn(cnt, cl.rargs.logHdr, "force-removing diverged:", lom.Cname(), eqErr, "[ forced:", cnt, "]")
	}

	// remove
	errRm := lom.RemoveObj()

	if errRm != nil {
		cnt := cl.stats.errRemove.Inc()
		sparseWarn(cnt, cl.rargs.logHdr, "remove failed:", lom.Cname(), errRm, "[ failures:", cnt, "]")
		return cmn.ErrSkip
	}

	cl.stats.removeMisplaced.Inc()
	return nil
}

/////////////
// clnArgs //
/////////////

// xreb.CtlMsg() callback (set via xreg.RebArgs)
func (clnArgs *clnArgs) ctlMsg(sb *cos.SB) {
	if sb.Len() > 0 {
		sb.WriteString("; ")
	}

	sb.WriteString(core.T.String())
	sb.WriteString(":cleanup")

	xreb := clnArgs.xreb
	if xreb.IsAborted() {
		sb.WriteString(" aborted")
	} else if xreb.IsDone() {
		sb.WriteString(" done")
	}

	s := &clnArgs.stats

	// visits
	sb.WriteString(" visits=")
	sb.WriteString(strconv.FormatInt(s.visits.Load(), 10))
	sb.WriteString(" loads=")
	sb.WriteString(strconv.FormatInt(s.loads.Load(), 10))

	// remove
	if v := s.removeMisplaced.Load(); v > 0 {
		sb.WriteString(" removed=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
	if v := s.removeDiverged.Load(); v > 0 {
		sb.WriteString(" removed-diverged=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}

	// keep
	if v := s.keepPeerMissing.Load(); v > 0 {
		sb.WriteString(" keep-peer-missing=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
	if v := s.keepDiverged.Load(); v > 0 {
		sb.WriteString(" keep-diverged=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
	if v := s.skipBusy.Load(); v > 0 {
		sb.WriteString(" skip-busy=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}

	// errors
	if ecnt := xreb.ErrCnt(); ecnt > 0 {
		sb.WriteString(" errs:")
		sb.WriteString(strconv.Itoa(ecnt))
	}
	// per-class error counters (worth showing separately because xreb.ErrCnt
	// may not include all of these — depending on which paths call AddErr)
	if v := s.errLoad.Load(); v > 0 {
		sb.WriteString(" err-load=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
	if v := s.errHEAD.Load(); v > 0 {
		sb.WriteString(" err-head=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
	if v := s.errRemove.Load(); v > 0 {
		sb.WriteString(" err-remove=")
		sb.WriteString(strconv.FormatInt(v, 10))
	}
}

//
// misc. utils
//

func sparseWarn(cnt int64, args ...any) {
	if cmn.Rom.V(5, cos.ModReb) || cnt <= 20 || (cnt <= 1000 && cnt%100 == 0) || cnt&(cnt-1) == 0 {
		nlog.Warningln(args...)
	}
}
