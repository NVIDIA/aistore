// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/sys"
)

// ==================== Node lifecycle management (primary proxy) ============================
//
// Main flow:
// * rmNode -> rmTargets -> mcastMaint -> [rebalance -> rmdModifier.postRm] -> rmNodesFinal.
//
// Unsafe removal enters directly at rmNodesFinal.
// Stop-maintenance: stopMaintenance -> mcastStopMaint -> optional rebalance.
//
// Maintenance, shutdown, and decommission are batch operations. The primary
// validates the entire batch before modifying Smap, then marks all requested
// nodes in one Smap transaction. When required, that transaction increments
// RMD once and starts one global rebalance.
//
// Two important helpers below, beginMembership/endMembership, admit exactly one
// administrative membership change at a time (to track it, note the
// post beginMembership flow: smap.modify => _rebPostRm => rmdModifier.listen).
//
// Rebalance completion resumes in rmdModifier.postRm (rebmeta.go), which
// performs the final daemon action and then either removes the nodes from Smap
// (decommission) or marks targets SnodeMaintPostReb (maintenance and shutdown).
// Without rebalance, finalization runs synchronously. Unsafe removal bypasses
// the marking and rebalance phase and proceeds directly to finalization.
//
// Stop-maintenance performs the reverse batch transition: it clears maintenance
// flags in one Smap transaction and starts at most one rebalance. Self-initiated
// DELETE /v1/cluster/daemon/{sid} also converges on the unregister path.
//
// Batch invariant: admission is all-or-nothing; each phase modifies Smap once
// for the entire batch, and a rebalance-triggering transition creates one RMD
// version and one global rebalance, regardless of the number of nodes.
//
// ===========================================================================================

const membershipTag = "cluster membership"

var errSmapNoChange = errors.New(membershipTag + ": no change") // mcastUnreg sentinel

func (p *proxy) beginMembership(action string) error {
	inflight := &p.primary().membershipTxn
	if !inflight.CAS(false, true) {
		return cmn.NewErrBusy(membershipTag, action)
	}
	if err := p.notifs.errRebRunning(action); err != nil {
		inflight.Store(false)
		return err
	}
	return nil
}

func (p *proxy) endMembership() {
	p.primary().membershipTxn.Store(false)
}

// gracefully remove node via apc.ActStartMaintenance, apc.ActDecommission, apc.ActShutdownNode
// +gen:payload apc.ActStartMaintenance={"action": "start-maintenance", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false}}
// +gen:payload apc.ActDecommissionNode={"action": "decommission-node", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false, "rm_user_data": true}}
// +gen:payload apc.ActShutdownNode={"action": "shutdown-node", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false}}
// +gen:payload apc.ActRmNodeUnsafe={"action": "remove-node-unsafe", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false}}
func (p *proxy) rmNode(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var opts apc.ActValRmNode
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	sids, errN := opts.GetIDs()
	if errN != nil {
		p.writeErr(w, r, errN)
		return
	}

	if err := p.beginMembership(msg.Action); err != nil {
		p.writeErr(w, r, err)
		return
	}
	defer p.endMembership()

	var (
		smap           = p.owner.smap.get()
		nodes          = make(meta.Nodes, 0, len(sids))
		snames         = make([]string, 0, len(sids))
		noPostReb      int
		activeSelected int // selected targets that are currently active
		hasTarget      bool
	)
	for _, sid := range sids {
		si := smap.GetNode(sid)
		if si == nil {
			err := &errNodeNotFound{si: p.si, smap: smap, msg: msg.Action, id: sid}
			p.writeErr(w, r, err, http.StatusNotFound)
			return
		}
		if p.SID() == sid {
			p.writeErrf(w, r, "%s is the current primary, cannot perform action %q on itself", p, msg.Action)
			return
		}

		inMaint := smap.InMaintOrDecomm(sid)
		if inMaint {
			// only (maintenance => decommission|shutdown) permitted
			sname := si.StringEx()
			switch msg.Action {
			case apc.ActDecommissionNode, apc.ActDecommissionCluster, apc.ActShutdownNode,
				apc.ActShutdownCluster, apc.ActRmNodeUnsafe:
				// defensive: admission is held, but self-join is not gated
				if err := p.notifs.errRebRunning(msg.Action + " " + sname); err != nil {
					p.writeErr(w, r, err)
					return
				}
				if !smap.InMaint(si) {
					nlog.Errorln("Warning: " + sname + " is currently being decommissioned")
				}
				// proceeding anyway
			default:
				debug.Assert(msg.Action == apc.ActStartMaintenance, msg.Action)
				switch {
				case si.Flags.IsSet(meta.SnodeDecomm):
					p.writeErrMsg(w, r, sname+" is currently being decommissioned")
					return
				case si.IsProxy() || si.InMaintPostReb():
					nlog.Warningln(p.String(), msg.Action, sname, "is already in maintenance mode - skipping")
					continue
				default:
					// SnodeMaint w/out SnodeMaintPostReb: cannot tell a finished (--no-rebalance) operation
					// from rebalance renewed by a concurrent self-join, or its listener aborted because
					// another target left (SIGTERM => rmSelf) the cluster. Either way, keep the node in maintenance.
					// See section "Incomplete Transitions" in docs/lifecycle_node.md.
					nlog.Warningln(p.String(), msg.Action, sname, "- post-rebalance not confirmed, proceeding anyway")
					noPostReb++
				}
			}
		}
		if si.IsTarget() {
			hasTarget = true
			if !inMaint {
				activeSelected++
			}
		}
		nodes = append(nodes, si)
		snames = append(snames, si.StringEx())
	}

	if len(nodes) == 0 {
		nlog.Warningln(p.String(), msg.Action, sids, "- all already in maintenance mode, nothing to do")
		return
	}

	nlog.Infof("%s: %s(%v) opts=%v", p, msg.Action, snames, opts)
	if hasTarget {
		// defensive: admission is held, but self-join is not gated
		if err := p.notifs.errRebRunning(msg.Action + " " + strings.Join(snames, ", ")); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}

	if noPostReb == len(nodes) {
		debug.Assert(msg.Action == apc.ActStartMaintenance)
		ecode, err := p.rmNodesFinal(msg, nodes, snames, nil)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, snames, err), ecode)
		}
		return
	}

	if msg.Action == apc.ActRmNodeUnsafe {
		if !opts.SkipRebalance {
			err := errors.New("unsafe must be unsafe")
			debug.AssertNoErr(err)
			p.writeErr(w, r, err)
			return
		}
		ecode, err := p.rmNodesFinal(msg, nodes, snames, nil)
		if err != nil {
			if cmn.IsErrBusy(err) {
				p.writeErr(w, r, err)
				return
			}
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, snames, err), ecode)
		}
		return
	}

	var (
		skipReb = opts.SkipRebalance || !cmn.GCO.Get().Rebalance.Enabled
		needReb = activeSelected > 0 && smap.CountActiveTs() > activeSelected
		reb     = !skipReb && activeSelected > 0
	)
	if skipReb && needReb {
		// migration could run but is suppressed by policy: the selected target(s) hold data
		// that will _not_ be migrated, and there are active targets to migrate it to
		nlog.Warningf("%s: %s reb=%t %v - executing %q _and_ not running global rebalance may lead to "+
			"a loss of data; to rebalance manually at a later time, run: `ais start rebalance`",
			p, msg.Action, reb, sids, msg.Action)
	} else {
		nlog.Infof("%s: %s reb=%t %v", p, msg.Action, reb, sids)
	}

	if reb {
		if err := p.canRebalance(smap, false /*cleanup mode*/); err != nil {
			p.writeErr(w, r, err)
			return
		}
		if err := p.checkRebCoexistence(msg, nil); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	rebID, err := p.rmTargets(nodes, snames, msg, reb)
	if err != nil {
		if cmn.IsErrBusy(err) {
			p.writeErr(w, r, err)
			return
		}
		p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, snames, err))
		return
	}
	if rebID != "" {
		writeXid(w, rebID)
	}
}

func (p *proxy) rmTargets(nodes meta.Nodes, snames []string, msg *apc.ActMsg, reb bool) (rebID string, err error) {
	var ctx *smapModifier
	if ctx, err = p.mcastMaint(msg, nodes, snames, reb); err != nil {
		return
	}
	if ctx.rmdCtx != nil {
		return ctx.rmdCtx.rebID, nil
	}
	if ctx.gfn { // stop early gfn when no rebalance was started
		actMsgExt := p.newAmsgActVal(apc.ActStopGFN, nil)
		for _, si := range nodes {
			if si.IsTarget() {
				actMsgExt.UUID = si.ID()
				break
			}
		}
		revs := revsPair{&smapX{Smap: meta.Smap{Version: ctx.nver}}, actMsgExt}
		_ = p.metasyncer.notify(false /*wait*/, revs) // async, failed-cnt always zero
	}
	_, err = p.rmNodesFinal(msg, nodes, snames, ctx)
	return "", err
}

func (p *proxy) mcastMaint(msg *apc.ActMsg, nodes meta.Nodes, snames []string, reb bool) (*smapModifier, error) {
	var flags cos.BitFlags
	switch msg.Action {
	case apc.ActDecommissionNode:
		flags = meta.SnodeDecomm
	case apc.ActShutdownNode, apc.ActStartMaintenance:
		flags = meta.SnodeMaint
	default:
		err := fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActDecommissionNode, apc.ActStartMaintenance, apc.ActShutdownNode})
		return nil, err
	}

	var (
		dummy = meta.Snode{Flags: flags}
		sids  = make([]string, 0, len(nodes))
	)
	for _, si := range nodes {
		sids = append(sids, si.ID())
	}
	nlog.Infof("%s mcast-maint: %s, %v reb=%t, nflags=%s", p, msg, snames, reb, dummy.Fl2S())

	ctx := &smapModifier{
		pre:     p._markMaint,
		post:    p._rebPostRm, // (rmdCtx.rmNode => p.rmNodeFinal when all done)
		final:   p._syncFinal,
		sids:    sids,
		flags:   flags,
		msg:     msg,
		skipReb: !reb,
	}
	for _, si := range nodes {
		if si.IsTarget() {
			if err := p._earlyGFN(ctx, si, msg.Action, false /*joining*/); err != nil {
				return nil, err
			}
			// the first target ID in a batch (if there's a batch) - is sufficient
			break
		}
	}
	err := p.owner.smap.modify(ctx)
	if err == nil {
		return ctx, nil
	}
	nlog.Warningln("mcast-maint:", err)
	if ctx.status != 0 {
		err = cmn.NewErrFailedTo(p, ctx.msg.Action, snames, err, ctx.status)
	}
	return nil, err
}

func (p *proxy) _markMaint(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot put %s in maintenance", strings.Join(ctx.sids, ", ")))
	}
	var hasTarget bool
	for _, sid := range ctx.sids {
		si := clone.GetNode(sid)
		if si == nil {
			ctx.status = http.StatusNotFound
			return &errNodeNotFound{si: p.si, smap: clone, msg: "cannot put node in maintenance", id: sid}
		}
		hasTarget = hasTarget || si.IsTarget()
	}
	if hasTarget {
		if err := p.notifs.errRebRunning(ctx.msg.Action + " " + strings.Join(ctx.sids, ", ")); err != nil {
			return err
		}
	}
	for _, sid := range ctx.sids {
		clone.setNodeFlags(sid, ctx.flags)
	}
	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infoln("_markMaint:", ctx.msg.Action, "nodes:", ctx.sids, "flags:", ctx.flags)
	}
	clone.staffIC()
	return nil
}

func (p *proxy) mcastMaintPostReb(msg *apc.ActMsg, nodes meta.Nodes, tnames []string) error {
	const tag = "mcast-maint-post-reb:"
	debug.Assert(msg.Action == apc.ActStartMaintenance || msg.Action == apc.ActShutdownNode, msg.Action)

	sids := make([]string, 0, len(nodes))
	for _, si := range nodes {
		debug.Assert(si.IsTarget(), si.StringEx())
		sids = append(sids, si.ID())
	}
	nlog.Infoln(p.String(), tag, msg.String(), tnames)

	ctx := &smapModifier{
		pre:   p._markMaintPostReb,
		final: p._syncFinal,
		sids:  sids,
		flags: meta.SnodeMaint | meta.SnodeMaintPostReb,
		msg:   msg,
	}
	err := p.owner.smap.modify(ctx)
	if err == nil {
		return nil
	}
	nlog.Warningln(tag, err)
	if ctx.status != 0 {
		err = cmn.NewErrFailedTo(p, ctx.msg.Action, tnames, err, ctx.status)
	}
	return err
}

func (p *proxy) _markMaintPostReb(ctx *smapModifier, clone *smapX) error {
	debug.Assert(ctx.flags == meta.SnodeMaint|meta.SnodeMaintPostReb, ctx.flags)
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, "cannot finalize maintenance for "+strings.Join(ctx.sids, ", "))
	}
	for _, sid := range ctx.sids {
		if clone.GetTarget(sid) == nil {
			nlog.Warningln(p.String(), ctx.msg.Action, meta.Tname(sid), "not present in", clone.StringEx(), "- skipping post-rebalance mark")
			continue
		}
		clone.setNodeFlags(sid, ctx.flags)
	}
	return nil
}

func (p *proxy) _rebPostRm(ctx *smapModifier, clone *smapX) {
	if ctx.skipReb {
		return
	}
	if !mustRebalance(ctx, clone) {
		return
	}
	rmdCtx := &rmdModifier{
		pre:     rmdInc,
		p:       p,
		smapCtx: ctx,
		wait:    true,
	}
	if _, err := p.owner.rmd.modify(rmdCtx); err != nil {
		debug.AssertNoErr(err)
		return
	}
	rmdCtx.listen(rmdCtx.postRm)
	ctx.rmdCtx = rmdCtx
}

func (p *proxy) _earlyGFN(ctx *smapModifier, si *meta.Snode, action string, joining bool) error {
	smap := p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		return newErrNotPrimary(p.si, smap, "cannot "+action+" "+si.StringEx())
	}
	if si.IsProxy() {
		return nil
	}
	if err := p.canRebalance(smap, false /*cleanup mode*/); err != nil {
		if err == errRebalanceDisabled {
			err = nil
		}
		return err
	}

	if smap.CountActiveTs() == 0 {
		return nil
	}
	if !joining && smap.CountActiveTs() == 1 {
		return nil
	}

	return p._notifyEarlyGFN(ctx, smap, si)
}

// keeping separate from _earlyGFN: stop-maintenance can rebalance while reactivating
// multiple targets when there are currently zero active targets
func (p *proxy) _notifyEarlyGFN(ctx *smapModifier, smap *smapX, tsi *meta.Snode) error {
	// Notify targets before publishing the updated Smap.
	actMsgExt := p.newAmsgActVal(apc.ActStartGFN, nil)
	actMsgExt.UUID = tsi.ID()
	revs := revsPair{&smapX{Smap: meta.Smap{Version: smap.Version}}, actMsgExt}
	if fcnt := p.metasyncer.notify(true /*wait*/, revs); fcnt > 0 {
		return fmt.Errorf("failed to notify early-gfn (%d)", fcnt)
	}
	ctx.gfn = true
	return nil
}

// rebalance's `can`: factors not including cluster map
func (p *proxy) canRebalance(smap *smapX, cleanup bool) error {
	if nlog.Stopping() {
		return p.errStopping()
	}
	if err := smap.validate(); err != nil {
		return err
	}
	if !smap.IsPrimary(p.si) {
		err := newErrNotPrimary(p.si, smap)
		debug.AssertNoErr(err)
		return err
	}

	// cluster startup handles rebalance elsewhere (see p.resumeReb), and so
	// all rebalance-triggering events (shutdown, decommission, maintenance, etc.)
	// are not permitted and will fail during startup.
	if err := p.pready(smap, true); err != nil {
		return err
	}

	// cleanup mode is an admin-requested local cleanup pass and intentionally
	// bypasses config.Rebalance.Enabled; the knob only disables regular rebalance
	if cleanup {
		return nil
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		return errRebalanceDisabled
	}
	return nil
}

// rebalance's `must`: compares previous and current (cloned, updated) Smap
// TODO: bmd.num-buckets == 0 would be an easy one to check
func mustRebalance(ctx *smapModifier, cur *smapX) bool {
	if !cmn.GCO.Get().Rebalance.Enabled {
		return false
	}
	if nlog.Stopping() {
		return false
	}
	prev := ctx.smap
	// no rebalance when no active targets (before or after this change)
	if prev.CountActiveTs() == 0 || cur.CountActiveTs() == 0 {
		return false
	}
	if ctx.interrupted || ctx.restarted {
		return true
	}

	// active <=> inactive transition
	debug.AssertFunc(func() bool { return prev.version() < cur.version() })
	for _, tsi := range cur.Tmap {
		// added an active one or activated previously inactive
		if !tsi.InMaintOrDecomm() && prev.GetActiveNode(tsi.ID()) == nil {
			return true
		}
	}
	for _, tsi := range prev.Tmap {
		// removed an active one or deactivated previously active
		if !tsi.InMaintOrDecomm() && cur.GetActiveNode(tsi.ID()) == nil {
			return true
		}
	}
	return false
}

func (p *proxy) _syncFinal(ctx *smapModifier, clone *smapX) {
	var (
		actMsgExt = p.newAmsg(ctx.msg, nil)
		pairs     = make([]revsPair, 0, 4)
		reb       = ctx.rmdCtx != nil && ctx.rmdCtx.rebID != ""
	)
	pairs = append(pairs, revsPair{clone, actMsgExt})
	if reb {
		debug.AssertFunc(func() bool { return ctx.rmdCtx.prev.version() < ctx.rmdCtx.cur.version() })
		actMsgExt.UUID = ctx.rmdCtx.rebID
		pairs = append(pairs, revsPair{ctx.rmdCtx.cur, actMsgExt})
	}
	debug.Assert(clone._sgl != nil)

	config, err := p.ensureConfigURLs()
	if err != nil {
		debug.AssertFunc(nlog.Stopping, err)
		return
	}
	if config == nil /*not updated - including anyway*/ {
		config, err = p.owner.config.get()
		if err != nil {
			debug.AssertFunc(nlog.Stopping, err)
			return
		}
	}

	pairs = append(pairs, revsPair{config, actMsgExt})
	wg := p.metasyncer.sync(pairs...)
	if ctx.rmdCtx != nil && ctx.rmdCtx.wait {
		wg.Wait()
	}
}

//
// post-rebalance or post no-rebalance - last step removing nodes
// (with msg.Action defining semantics)
//

func (p *proxy) rmNodesFinal(msg *apc.ActMsg, nodes meta.Nodes, snames []string, ctx *smapModifier) (int, error) {
	var (
		smap          = p.owner.smap.get()
		selected      = make(meta.Nodes, 0, len(nodes))
		selectedNames = make([]string, 0, len(nodes))
	)
	switch msg.Action {
	case apc.ActShutdownNode, apc.ActRmNodeUnsafe, apc.ActStartMaintenance, apc.ActDecommissionNode:
	default:
		return 0, fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActShutdownNode, apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActRmNodeUnsafe})
	}
	for _, si := range nodes {
		node := smap.GetNode(si.ID())
		if node == nil {
			// already gone (e.g. keepalive-removed) - nothing can do
			nlog.Warningln(p.String(), msg.Action, si.StringEx(), "not present in", smap.StringEx(), "- skipping")
			continue
		}
		selected = append(selected, node)
		selectedNames = append(selectedNames, node.StringEx())
	}
	if len(selected) == 0 {
		nlog.Warningf("cannot %s %v - none present in %s, nothing to do", msg.Action, snames, smap.StringEx())
		return 0, nil
	}

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
	args.smap = smap
	args.network = cmn.NetIntraControl
	args.selected = selected
	args.nodeCount = len(selected)
	args.timeout = cmn.Rom.CplaneOperation()
	nlog.InfoDepth(1, p.String(), msg.Action, selectedNames)
	results := p.bcastSelected(args)
	freeBcArgs(args)
	for _, res := range results {
		err := res.unwrap()
		if err == nil {
			continue
		}
		sname := res.si.StringEx()
		emsg := fmt.Sprintf("%s: (%s %s) final: %v - proceeding anyway...", p, msg, sname, err)
		switch msg.Action {
		case apc.ActShutdownNode, apc.ActDecommissionNode: // expecting EOF
			if !cos.IsAnyEOF(err) {
				nlog.Errorln(emsg)
			}
		case apc.ActRmNodeUnsafe:
			if cmn.Rom.V(4, cos.ModAIS) {
				nlog.Errorln(emsg)
			}
		default:
			nlog.Errorln(emsg)
		}
	}
	freeBcastRes(results)

	var (
		err   error
		ecode int
	)
	switch msg.Action {
	case apc.ActDecommissionNode, apc.ActRmNodeUnsafe:
		ecode, err = p.mcastUnreg(msg, selected, selectedNames)
	case apc.ActStartMaintenance, apc.ActShutdownNode:
		if ctx != nil && ctx.rmdCtx != nil && ctx.rmdCtx.rebID != "" {
			// final step executing shutdown and start-maintenance transaction:
			// setting target flags |= cluster.SnodeMaintPostReb
			var (
				targets = make(meta.Nodes, 0, len(selected))
				tnames  = make([]string, 0, len(selected))
			)
			for _, si := range selected {
				if si.IsTarget() {
					targets = append(targets, si)
					tnames = append(tnames, si.StringEx())
				}
			}
			if len(targets) > 0 {
				err = p.mcastMaintPostReb(msg, targets, tnames)
			}
		}
	}
	if err != nil {
		nlog.Errorf("%s: (%s %v) FATAL: failed to update %s: %v", p, msg, selectedNames, p.owner.smap.get(), err)
	}
	return ecode, err
}

func (p *proxy) mcastUnreg(msg *apc.ActMsg, nodes meta.Nodes, snames []string) (ecode int, err error) {
	sids := make([]string, 0, len(nodes))
	for _, si := range nodes {
		sids = append(sids, si.ID())
	}
	nlog.Infof("%s mcast-unreg: %s, %v", p, msg, snames)
	ctx := &smapModifier{
		pre:     p._unregNodesPre,
		final:   p._syncFinal,
		msg:     msg,
		sids:    sids,
		skipReb: true,
	}
	err = p.owner.smap.modify(ctx)
	if err != nil && errors.Is(err, errSmapNoChange) {
		err = nil
	}
	return ctx.status, err
}

func (p *proxy) _unregNodesPre(ctx *smapModifier, clone *smapX) error {
	const verb = "remove"
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot %s %v", verb, ctx.sids))
	}
	var (
		hasTarget bool
		present   int
	)
	for _, sid := range ctx.sids {
		si := clone.GetNode(sid)
		if si == nil {
			// already gone (e.g. keepalive-removed) - not an error when removing a batch
			continue
		}
		present++
		hasTarget = hasTarget || si.IsTarget()
	}
	if present == 0 {
		nlog.Warningln(p.String(), ctx.msg.Action, ctx.sids, "already not present in", clone.StringEx(), "- nothing to do")
		return errSmapNoChange
	}
	if ctx.msg.Action == apc.ActRmNodeUnsafe && hasTarget {
		if err := p.notifs.errRebRunning(ctx.msg.Action + " " + strings.Join(ctx.sids, ", ")); err != nil {
			return err
		}
	}
	var removedProxy bool
	for _, sid := range ctx.sids {
		node := clone.GetNode(sid)
		if node == nil {
			continue
		}
		if node.IsProxy() {
			clone.delProxy(sid)
			removedProxy = true
			nlog.Infof("%s %s (num proxies %d)", verb, node.StringEx(), clone.CountProxies())
		} else {
			clone.delTarget(sid)
			nlog.Infof("%s %s (num targets %d)", verb, node.StringEx(), clone.CountTargets())
		}
		p.rproxy.nodes.Delete(sid)
	}
	if removedProxy {
		clone.staffIC()
	}
	return nil
}

func (p *proxy) _reqHealthSelected(nodes meta.Nodes, smap *smapX, tout time.Duration, retry bool) sliceResults {
	debug.Assert(len(nodes) > 0)
	results := allocBcastRes(len(nodes))
	results = results[:len(nodes)]

	call := func(i int, si *meta.Snode) {
		res := allocCR()
		res.si = si
		_, res.status, res.err = p.reqHealth(si, tout, nil, smap, retry)
		results[i] = res
	}
	if len(nodes) == 1 {
		call(0, nodes[0])
		return results
	}

	wg := cos.NewClusterWaitGroup(sys.NumCPU(), len(nodes))
	for i, si := range nodes {
		wg.Add(1)
		go func(i int, si *meta.Snode) {
			call(i, si)
			wg.Done()
		}(i, si)
	}
	wg.Wait()
	return results
}

// +gen:payload apc.ActStopMaintenance={"action": "stop-maintenance", "value": {"sids": ["target_id1", "target_id2"]}}
func (p *proxy) stopMaintenance(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	const tag = "stop-maintenance:"
	var opts apc.ActValRmNode
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	sids, errN := opts.GetIDs()
	if errN != nil {
		p.writeErr(w, r, errN)
		return
	}

	if err := p.beginMembership(msg.Action); err != nil {
		p.writeErr(w, r, err)
		return
	}
	defer p.endMembership()

	var (
		tsi         *meta.Snode
		targetCount int
		smap        = p.owner.smap.get()
		nodes       = make(meta.Nodes, 0, len(sids))
		selectedIDs = make([]string, 0, len(sids))
		snames      = make([]string, 0, len(sids))
	)
	for _, sid := range sids {
		si := smap.GetNode(sid)
		if si == nil {
			err := &errNodeNotFound{si: p.si, smap: smap, msg: tag, id: sid}
			p.writeErr(w, r, err, http.StatusNotFound)
			return
		}

		switch {
		case si.Flags.IsSet(meta.SnodeDecomm):
			p.writeErrf(w, r, "%s: node %s is currently being decommissioned", tag, si.StringEx())
			return
		case smap.InMaint(si):
			// expected
		default:
			nlog.Warningln(tag, si.StringEx(), "is already active - skipping")
			continue
		}

		nodes = append(nodes, si)
		selectedIDs = append(selectedIDs, si.ID())
		if si.IsTarget() {
			targetCount++
			tsi = si
		}
		snames = append(snames, si.StringEx())
	}
	if len(nodes) == 0 {
		nlog.Warningln(tag, "all requested nodes:", sids, "are already active - nothing to do")
		return
	}

	if targetCount > 0 {
		// defensive: admission is held, but self-join is not gated
		if err := p.notifs.errRebRunning(msg.Action + " " + strings.Join(snames, ", ")); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	pname := p.String()
	nlog.Infoln(tag, pname, "[", msg.Action, snames, opts, "]")
	tout := cmn.Rom.CplaneOperation()
	results := p._reqHealthSelected(nodes, smap, tout, false /*retry pub-addr*/)
	retryNodes := make(meta.Nodes, 0, len(nodes))
	for _, res := range results {
		if res.err != nil {
			retryNodes = append(retryNodes, res.si)
		}
	}
	freeBcastRes(results)

	if len(retryNodes) > 0 {
		sleep, retries := tout/2, 4

		// give restarting nodes a full `tout` before the first retry
		// (initial startup grace plus the first retry interval)
		time.Sleep(sleep)
		for i := range retries {
			time.Sleep(sleep)
			results = p._reqHealthSelected(retryNodes, smap, tout, true /*retry pub-addr*/)
			retryNodes = retryNodes[:0]
			for _, res := range results {
				sname := res.si.StringEx()
				switch {
				case res.err == nil:
					nlog.Infoln(tag, pname, "=>", sname, "OK after", i+1, "attempt"+cos.Plural(i+1), "[", msg.Action, opts, "]")
				case res.status != http.StatusServiceUnavailable:
					p.writeErrf(w, r, "%s is unreachable: %v(%d)", sname, res.err, res.status)
					freeBcastRes(results)
					return
				default:
					retryNodes = append(retryNodes, res.si)
				}
			}
			freeBcastRes(results)
			l := len(retryNodes)
			if l == 0 {
				break
			}
			if i < retries-1 {
				nlog.Warningln(tag, "retrying node"+cos.Plural(l), "that returned 503:", retryNodes)
				sleep = min(sleep+time.Second, tout)
				continue
			}
			nlog.Warningln(tag, "timed out waiting for node"+cos.Plural(l), retryNodes, "to start - proceeding anyway")
		}
	}

	smap = p.owner.smap.get() // health checks above may take a while
	reb := targetCount > 0 && !opts.SkipRebalance && cmn.GCO.Get().Rebalance.Enabled &&
		!nlog.Stopping() && smap.CountActiveTs()+targetCount >= 2
	if reb {
		if err := p.canRebalance(smap, false /*cleanup mode*/); err != nil {
			p.writeErr(w, r, err)
			return
		}
		if err := p.checkRebCoexistence(msg, nodes); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}

	rebID, err := p.mcastStopMaint(msg, selectedIDs, snames, tsi, reb)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if rebID != "" {
		writeXid(w, rebID)
	}
}

func (p *proxy) mcastStopMaint(msg *apc.ActMsg, sids, snames []string, tsi *meta.Snode, reb bool) (rebID string, err error) {
	nlog.Infof("%s mcast-stopm: %s, %s, reb=%t", p, msg, snames, reb)
	ctx := &smapModifier{
		pre:     p._stopMaintPre,
		post:    p._stopMaintRMD,
		final:   p._syncFinal,
		sids:    sids,
		skipReb: !reb,
		msg:     msg,
		flags:   meta.SnodeMaint | meta.SnodeMaintPostReb, // to clear node flags
	}

	if reb {
		debug.Assert(tsi != nil)

		smap := p.owner.smap.get()
		if err := p._notifyEarlyGFN(ctx, smap, tsi); err != nil {
			return "", err
		}
	}

	err = p.owner.smap.modify(ctx)
	if err != nil {
		if ctx.status != 0 {
			err = cmn.NewErrFailedTo(p, msg.Action, snames, err, ctx.status)
		}
		return "", err
	}

	if ctx.rmdCtx != nil && ctx.rmdCtx.cur != nil {
		debug.AssertFunc(func() bool { return ctx.rmdCtx.cur.version() > ctx.rmdCtx.prev.version() && ctx.rmdCtx.rebID != "" })
		return ctx.rmdCtx.rebID, nil
	}

	if ctx.gfn { // stop timed GFN when no rebalance was started
		actMsgExt := p.newAmsgActVal(apc.ActStopGFN, nil)
		actMsgExt.UUID = tsi.ID()
		revs := revsPair{&smapX{Smap: meta.Smap{Version: ctx.nver}}, actMsgExt}
		_ = p.metasyncer.notify(false /*wait*/, revs) // async, failed-cnt always zero
	}
	return "", nil
}

func (p *proxy) _stopMaintPre(ctx *smapModifier, clone *smapX) error {
	const efmt = "cannot take %s out of maintenance:"
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf(efmt, strings.Join(ctx.sids, ", ")))
	}
	var hasTarget bool
	for _, sid := range ctx.sids {
		si := clone.GetNode(sid)
		if si == nil {
			ctx.status = http.StatusNotFound
			return &errNodeNotFound{si: p.si, smap: clone, msg: fmt.Sprintf(efmt, sid), id: sid}
		}
		hasTarget = hasTarget || si.IsTarget()
	}
	if hasTarget {
		if err := p.notifs.errRebRunning(ctx.msg.Action + " " + strings.Join(ctx.sids, ", ")); err != nil {
			return err
		}
	}
	var activateProxy bool
	for _, sid := range ctx.sids {
		node := clone.GetNode(sid)
		clone.clearNodeFlags(sid, ctx.flags)
		activateProxy = activateProxy || node.IsProxy()
	}
	if activateProxy {
		clone.staffIC()
	}
	return nil
}

func (p *proxy) _stopMaintRMD(ctx *smapModifier, clone *smapX) {
	if ctx.skipReb || !cmn.GCO.Get().Rebalance.Enabled || nlog.Stopping() {
		return
	}

	// valid use case: bringing two or more targets back when all targets were in maintenance
	prevActive := ctx.smap.CountActiveTs()
	curActive := clone.CountActiveTs()
	if curActive < 2 || curActive <= prevActive {
		return
	}

	rmdCtx := &rmdModifier{
		pre:     rmdInc,
		smapCtx: ctx,
		p:       p,
		wait:    true,
	}
	if _, err := p.owner.rmd.modify(rmdCtx); err != nil {
		debug.AssertNoErr(err)
		return
	}
	rmdCtx.listen(nil)
	ctx.rmdCtx = rmdCtx
}

//
// DELETE /v1/cluster (apc.ActSelfRemove)
//

// +gen:endpoint DELETE /v1/cluster/daemon/{daemon-id}
// Remove a node from the cluster by daemon ID.
// Used for self-initiated node removal (e.g., when a node loses all mountpaths).
func (p *proxy) httpcludel(w http.ResponseWriter, r *http.Request, isPub bool) {
	debug.AssertFunc(func() bool { return reqIsPub(r) == isPub })
	if isPub {
		p.writeErrMsg(w, r, "not expecting DELETE /v1/cluster via pub-net", http.StatusForbidden)
		return
	}

	apiItems, err := p.parseURL(w, r, apc.URLPathCluDaemon.L, 1, false)
	if err != nil {
		return
	}

	var (
		sid  = apiItems[0]
		smap = p.owner.smap.get()
		node = smap.GetNode(sid)
	)
	if node == nil {
		err = &errNodeNotFound{si: p.si, smap: smap, msg: "cannot remove", id: sid}
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if smap.IsPrimary(node) {
		p.writeErrMsg(w, r, "cannot remove primary proxy", http.StatusBadRequest)
		return
	}
	if p.forwardCP(w, r, nil, sid) {
		return
	}
	if !p.NodeStarted() {
		p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s is not ready yet (starting up)", p)
		return
	}

	// primary (and cluster) to start and finalize rebalancing status _prior_ to removing individual nodes
	if err := p.pready(smap, true); err != nil {
		p.writeErr(w, r, err, http.StatusServiceUnavailable)
		return
	}

	if ecode, err := p.checkIntra(r, false /*only primary*/); err != nil {
		err = fmt.Errorf("%w (action %q)", err, apc.ActSelfRemove)
		p.writeErr(w, r, err, ecode)
		return
	}

	senderID := r.Header.Get(apc.HdrSenderID)
	if senderID != sid {
		err = fmt.Errorf("expecting %s by %s, got a wrong node ID (%s != %s)", apc.ActSelfRemove, node.StringEx(), senderID, sid)
		p.writeErr(w, r, err)
		return
	}

	if ecode, err := p.mcastUnreg(&apc.ActMsg{Action: apc.ActSelfRemove}, meta.Nodes{node}, []string{node.StringEx()}); err != nil {
		p.writeErr(w, r, err, ecode)
	} else {
		v := &p.rproxy.removed
		v.mu.Lock()
		if v.m == nil {
			v.m = make(meta.NodeMap, 4)
		}
		v.m[node.ID()] = node
		v.mu.Unlock()
	}
}
