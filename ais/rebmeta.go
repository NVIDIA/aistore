// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

// Rebalance metadata is distributed to trigger global (a.k.a. cluster) rebalance.
// This (distribution) happens:
// - when a new target node joins cluster;
// - at startup, when cluster detects unfinished (aborted) rebalance;
// - when we remove a target while some bucket(s) are erasure-coded
//   (we then must redistribute slices);
// - upon bucket rename:
//    1. bucket is renamed (and the paths of the objects change)
//    2. rebalance must be started to redistribute the objects to the targets
//       depending on HRW;
// - when requested by user (`ais start rebalance` or REST API);
// - upon target node powercycle (and more).

type (
	// rebMD is revs (see metasync) which is distributed by primary proxy to
	// the targets. It is distributed when some kind of rebalance is required.
	rebMD struct {
		meta.RMD
	}

	// rmdOwner is used to keep the information about the rebalances. Currently
	// it keeps the Version of the latest rebalance.
	rmdOwner struct {
		cluID string
		fpath string
		sync.Mutex
		rmd         ratomic.Pointer[rebMD]
		interrupted atomic.Bool // when joining target reports interrupted rebalance
		starting    atomic.Bool // when starting up
	}

	rmdModifier struct {
		pre   func(ctx *rmdModifier, clone *rebMD)
		final func(ctx *rmdModifier, clone *rebMD)

		prev  *rebMD // pre-modification rmd
		cur   *rebMD // CoW clone
		rebID string // cluster-wide rebalance ID, "g[uuid]" in the logs

		cluID   string // cluster ID (== smap.UUID) - never changes
		p       *proxy
		smapCtx *smapModifier
		wait    bool
	}
)

// interface guard
var _ revs = (*rebMD)(nil)

// as revs
func (*rebMD) tag() string       { return revsRMDTag }
func (r *rebMD) version() int64  { return r.Version }
func (r *rebMD) marshal() []byte { return cos.MustMarshal(r) }
func (*rebMD) jit(p *proxy) revs { return p.owner.rmd.get() }
func (*rebMD) sgl() *memsys.SGL  { return nil }

func (r *rebMD) inc() { r.Version++ }

func (r *rebMD) clone() *rebMD {
	dst := &rebMD{}
	cos.CopyStruct(dst, r)
	return dst
}

func (r *rebMD) String() string {
	if r == nil {
		return "RMD <nil>"
	}
	if len(r.TargetIDs) == 0 && r.Resilver == "" {
		return fmt.Sprintf("RMD v%d[%s]", r.Version, r.CluID)
	}
	var s string
	if r.Resilver != "" {
		s = ", " + r.Resilver
	}
	return fmt.Sprintf("RMD v%d[%s, %v%s]", r.Version, r.CluID, r.TargetIDs, s)
}

//////////////
// rmdOwner //
//////////////

func newRMDOwner(config *cmn.Config) *rmdOwner {
	rmdo := &rmdOwner{fpath: filepath.Join(config.ConfigDir, fname.Rmd)}
	rmdo.put(&rebMD{})
	return rmdo
}

func (r *rmdOwner) persist(rmd *rebMD) error {
	return jsp.SaveMeta(r.fpath, rmd, nil /*wto*/)
}

func (r *rmdOwner) load() {
	rmd := &rebMD{}
	_, err := jsp.LoadMeta(r.fpath, rmd)
	if err == nil {
		r.put(rmd)
		return
	}
	if !os.IsNotExist(err) {
		nlog.Errorln("failed to load RMD:", err)
		nlog.Infoln("Warning: make sure to properly decommission previously deployed clusters, proceeding anyway...")
	}
}

func (r *rmdOwner) put(rmd *rebMD) { r.rmd.Store(rmd) }
func (r *rmdOwner) get() *rebMD    { return r.rmd.Load() }

func (r *rmdOwner) modify(ctx *rmdModifier) (clone *rebMD, err error) {
	r.Lock()
	clone, err = r.do(ctx)
	r.Unlock()

	if err == nil && ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return
}

const rmdFromAnother = `
%s: RMD v%d (cluster ID %q) belongs to a different cluster %q

-----------------
To troubleshoot:
1. first, make sure you are not trying to run two different clusters that utilize (or include) the same machine
2. remove possibly misplaced RMD from the %s (located at %s)
3. restart %s
-----------------`

func (r *rmdOwner) newClusterIntegrityErr(node, otherCID, haveCID string, version int64) (err error) {
	return fmt.Errorf(rmdFromAnother, node, version, haveCID, otherCID, node, r.fpath, node)
}

func (r *rmdOwner) do(ctx *rmdModifier) (clone *rebMD, err error) {
	ctx.prev = r.get()

	if r.cluID == "" {
		r.cluID = ctx.cluID
	}
	if ctx.smapCtx == nil {
		return
	}
	if r.cluID == "" {
		r.cluID = ctx.smapCtx.smap.UUID
	} else if r.cluID != ctx.smapCtx.smap.UUID {
		err := r.newClusterIntegrityErr("primary", ctx.smapCtx.smap.UUID, r.cluID, ctx.prev.Version)
		cos.ExitLog(err) // FATAL
	}

	clone = ctx.prev.clone()
	clone.TargetIDs = nil
	clone.Resilver = ""
	clone.CluID = r.cluID
	debug.Assert(cos.IsValidUUID(clone.CluID), clone.CluID)
	ctx.pre(ctx, clone) // `pre` callback

	if err = r.persist(clone); err == nil {
		r.put(clone)
	}
	ctx.cur = clone
	ctx.rebID = xact.RebID2S(clone.Version) // new rebID
	return
}

/////////////////
// rmdModifier //
/////////////////

func rmdInc(_ *rmdModifier, clone *rebMD) { clone.inc() }

// via `rmdModifier.final`
func rmdSync(m *rmdModifier, clone *rebMD) {
	debug.Assert(m.cur == clone)
	m.listen(nil)
	msg := &aisMsg{ActMsg: apc.ActMsg{Action: apc.ActRebalance}, UUID: m.rebID} // user-requested rebalance
	wg := m.p.metasyncer.sync(revsPair{m.cur, msg})
	if m.wait {
		wg.Wait()
	}
}

// see `receiveRMD` (upon termination, notify IC)
func (m *rmdModifier) listen(cb func(nl nl.Listener)) {
	nl := xact.NewXactNL(m.rebID, apc.ActRebalance, &m.smapCtx.smap.Smap, nil)
	nl = nl.WithCause(m.smapCtx.msg.Action)
	nl.SetOwner(equalIC)

	nl.F = m.log
	if cb != nil {
		nl.F = cb
	}
	err := m.p.notifs.add(nl)
	debug.AssertNoErr(err)
}

// deactivate or remove node from the cluster (as per msg.Action)
// called when rebalance is done
func (m *rmdModifier) postRm(nl nl.Listener) {
	var (
		p     = m.p
		tsi   = m.smapCtx.smap.GetNode(m.smapCtx.sid)
		sname = tsi.StringEx()
		xname = "rebalance[" + nl.UUID() + "]"
		smap  = p.owner.smap.get()
		warn  = "remove " + sname + " from the current " + smap.StringEx()
	)
	debug.Assert(nl.UUID() == m.rebID && tsi.IsTarget())

	if nl.ErrCnt() == 0 {
		nlog.Infoln("post-rebalance commit: ", warn)
		if _, err := p.rmNodeFinal(m.smapCtx.msg, tsi, m.smapCtx); err != nil {
			nlog.Errorln(err)
		}
		return
	}

	m.log(nl)
	nlerr := nl.Err()

	rmd := p.owner.rmd.get()
	if nlerr == cmn.ErrXactRenewAbort || nlerr.Error() == cmn.ErrXactRenewAbort.Error() || m.cur.Version < rmd.Version {
		nlog.Errorf("Warning: %s (%s) got renewed (interrupted) - will not %s (%s)", xname, m.smapCtx.smap, warn, rmd)
		return
	}
	if m.smapCtx.msg.Action != apc.ActRmNodeUnsafe && m.smapCtx.msg.Action != apc.ActDecommissionNode {
		nlog.Errorf("operation %q => %s (%s) failed - will not %s", m.smapCtx.msg.Action, xname, m.smapCtx.smap, warn)
		return
	}

	// go ahead to decommission anyway
	nlog.Errorf("given %q operation and despite [%v] - proceeding to %s", m.smapCtx.msg.Action, nlerr, warn)
	if _, err := p.rmNodeFinal(m.smapCtx.msg, tsi, m.smapCtx); err != nil {
		nlog.Errorln(err)
	}

	//
	// TODO: bcast targets to re-rebalance for the same `m.rebID` iff there isn't a new one that's running or about to run
	//
}

func (m *rmdModifier) log(nl nl.Listener) {
	debug.Assert(nl.UUID() == m.rebID)
	var (
		err  = nl.Err()
		abrt = nl.Aborted()
		name = "rebalance[" + nl.UUID() + "]"
	)
	switch {
	case err == nil && !abrt:
		nlog.InfoDepth(1, name, "done")
	case abrt:
		debug.Assert(err != nil, nl.String()+" - aborted w/ no errors")
		nlog.ErrorDepth(1, name, err)
	default:
		nlog.ErrorDepth(1, name, "failed:", err)
	}
}
