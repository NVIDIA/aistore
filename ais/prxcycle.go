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

// gracefully remove node via apc.ActStartMaintenance, apc.ActDecommission, apc.ActShutdownNode
// +gen:payload apc.ActStartMaintenance={"action": "start-maintenance", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false}}
// +gen:payload apc.ActDecommissionNode={"action": "decommission-node", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false, "rm_user_data": true}}
// +gen:payload apc.ActShutdownNode={"action": "shutdown-node", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false}}
// +gen:payload apc.ActRmNodeUnsafe={"action": "remove-node-unsafe", "value": {"sids": ["target_id1", "target_id2"], "skip_rebalance": false}}
func (p *proxy) rmNode(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		opts apc.ActValRmNode
		smap = p.owner.smap.get()
	)
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	sids, err := _rmNodeIDs(&opts)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	var (
		nodes           = make(meta.Nodes, 0, len(sids))
		seen            = make(map[string]struct{}, len(sids))
		hasTarget       bool
		hasActiveTarget bool
	)
	for _, sid := range sids {
		if _, ok := seen[sid]; ok {
			p.writeErrf(w, r, "node %q is specified more than once", sid)
			return
		}
		seen[sid] = struct{}{}
		si := smap.GetNode(sid)
		if si == nil {
			err := cos.NewErrNotFound(p, "node "+sid)
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
				if running, xid := p.notifs.isRebRunning(); running {
					p.writeErrf(w, r, "rebalance[%s] is currently running, please try (%s %s) later",
						xid, msg.Action, sname)
					return
				}
				if !smap.InMaint(si) {
					nlog.Errorln("Warning: " + sname + " is currently being decommissioned")
				}
				// proceeding anyway
			default:
				if smap.InMaint(si) {
					p.writeErrMsg(w, r, sname+" is already in maintenance mode")
				} else {
					p.writeErrMsg(w, r, sname+" is currently being decommissioned")
				}
				return
			}
		}
		if si.IsTarget() {
			hasTarget = true
			if !inMaint {
				hasActiveTarget = true
			}
		}
		nodes = append(nodes, si)
	}

	sname := snodeNames(nodes)
	nlog.Infof("%s: %s(%s) opts=%v", p, msg.Action, sname, opts)
	if hasTarget {
		if running, xid := p.notifs.isRebRunning(); running {
			p.writeErrf(w, r, "rebalance[%s] is currently running, please try (%s %s) later",
				xid, msg.Action, sname)
			return
		}
	}

	if msg.Action == apc.ActRmNodeUnsafe {
		if !opts.SkipRebalance {
			err := errors.New("unsafe must be unsafe")
			debug.AssertNoErr(err)
			p.writeErr(w, r, err)
			return
		}
		ecode, err := p.rmNodesFinal(msg, nodes, nil)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, sname, err), ecode)
		}
		return
	}

	reb := !opts.SkipRebalance && cmn.GCO.Get().Rebalance.Enabled && hasActiveTarget
	nlog.Infof("%s: %s reb=%t %v", p, msg.Action, reb, sids)

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
	rebID, err := p.rmTargets(nodes, msg, reb)
	if err != nil {
		p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, sname, err))
		return
	}
	if rebID != "" {
		writeXid(w, rebID)
	}
}

func (p *proxy) rmTargets(nodes meta.Nodes, msg *apc.ActMsg, reb bool) (rebID string, err error) {
	var ctx *smapModifier
	if ctx, err = p.mcastMaint(msg, nodes, reb); err != nil {
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
	_, err = p.rmNodesFinal(msg, nodes, ctx)
	return "", err
}

func (p *proxy) mcastMaint(msg *apc.ActMsg, nodes meta.Nodes, reb bool) (*smapModifier, error) {
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
	nlog.Infof("%s mcast-maint: %s, %s reb=%t, nflags=%s", p, msg, snodeNames(nodes), reb, dummy.Fl2S())

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
		err = cmn.NewErrFailedTo(p, ctx.msg.Action, snodeNames(nodes), err, ctx.status)
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
			return &errNodeNotFound{p.si, clone, "cannot put node in maintenance", sid}
		}
		hasTarget = hasTarget || si.IsTarget()
	}
	if hasTarget {
		if running, xid := p.notifs.isRebRunning(); running {
			return fmt.Errorf("rebalance[%s] is currently running, please try (%s %s) later",
				xid, ctx.msg.Action, strings.Join(ctx.sids, ", "))
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

func (p *proxy) mcastMaintPostReb(msg *apc.ActMsg, nodes meta.Nodes) error {
	const tag = "mcast-maint-post-reb:"
	debug.Assert(msg.Action == apc.ActStartMaintenance || msg.Action == apc.ActShutdownNode, msg.Action)

	sids := make([]string, 0, len(nodes))
	for _, si := range nodes {
		debug.Assert(si.IsTarget(), si.StringEx())
		sids = append(sids, si.ID())
	}
	nlog.Infoln(p.String(), tag, msg.String(), snodeNames(nodes))

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
		err = cmn.NewErrFailedTo(p, ctx.msg.Action, snodeNames(nodes), err, ctx.status)
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

	// early-GFN notification with an empty (version-only and not yet updated) Smap and
	// message(new target's ID)
	msg := p.newAmsgActVal(apc.ActStartGFN, nil)
	msg.UUID = si.ID()
	revs := revsPair{&smapX{Smap: meta.Smap{Version: smap.Version}}, msg}
	if fcnt := p.metasyncer.notify(true /*wait*/, revs); fcnt > 0 {
		return fmt.Errorf("failed to notify early-gfn (%d)", fcnt)
	}
	ctx.gfn = true // to undo if need be
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
	debug.Assert(prev.version() < cur.version())
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
		debug.Assert(ctx.rmdCtx.prev.version() < ctx.rmdCtx.cur.version())
		actMsgExt.UUID = ctx.rmdCtx.rebID
		pairs = append(pairs, revsPair{ctx.rmdCtx.cur, actMsgExt})
	}
	debug.Assert(clone._sgl != nil)

	config, err := p.ensureConfigURLs()
	if err != nil {
		debug.Assert(nlog.Stopping(), err)
		return
	}
	if config == nil /*not updated - including anyway*/ {
		config, err = p.owner.config.get()
		if err != nil {
			debug.Assert(nlog.Stopping(), err)
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

func (p *proxy) rmNodesFinal(msg *apc.ActMsg, nodes meta.Nodes, ctx *smapModifier) (int, error) {
	var (
		smap     = p.owner.smap.get()
		selected = make(meta.Nodes, 0, len(nodes))
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
	}
	if len(selected) == 0 {
		txt := "cannot \"" + msg.Action + "\""
		return http.StatusNotFound, &errNodeNotFound{p.si, smap, txt, snodeNames(nodes)}
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
	args.smap = smap
	args.network = cmn.NetIntraControl
	args.selected = selected
	args.nodeCount = len(selected)
	args.timeout = cmn.Rom.CplaneOperation()
	nlog.InfoDepth(1, p.String(), msg.Action, snodeNames(selected))
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
		ecode, err = p.mcastUnreg(msg, selected)
	case apc.ActStartMaintenance, apc.ActShutdownNode:
		if ctx != nil && ctx.rmdCtx != nil && ctx.rmdCtx.rebID != "" {
			// final step executing shutdown and start-maintenance transaction:
			// setting target flags |= cluster.SnodeMaintPostReb
			var targets meta.Nodes
			for _, si := range selected {
				if si.IsTarget() {
					targets = append(targets, si)
				}
			}
			if len(targets) > 0 {
				err = p.mcastMaintPostReb(msg, targets)
			}
		}
	}
	if err != nil {
		nlog.Errorf("%s: (%s %s) FATAL: failed to update %s: %v",
			p, msg, snodeNames(selected), p.owner.smap.get(), err)
	}
	return ecode, err
}

func (p *proxy) mcastUnreg(msg *apc.ActMsg, nodes meta.Nodes) (ecode int, err error) {
	sids := make([]string, 0, len(nodes))
	for _, si := range nodes {
		sids = append(sids, si.ID())
	}
	nlog.Infof("%s mcast-unreg: %s, %s", p, msg, snodeNames(nodes))
	ctx := &smapModifier{
		pre:     p._unregNodesPre,
		final:   p._syncFinal,
		msg:     msg,
		sids:    sids,
		skipReb: true,
	}
	err = p.owner.smap.modify(ctx)
	return ctx.status, err
}

func (p *proxy) _unregNodesPre(ctx *smapModifier, clone *smapX) error {
	const verb = "remove"
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot %s %s", verb, strings.Join(ctx.sids, ", ")))
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
		ctx.status = http.StatusNotFound
		return &errNodeNotFound{p.si, clone, "failed to " + verb, strings.Join(ctx.sids, ", ")}
	}
	if ctx.msg.Action == apc.ActRmNodeUnsafe && hasTarget {
		if running, xid := p.notifs.isRebRunning(); running {
			return fmt.Errorf("rebalance[%s] is currently running, please try (%s %s) later",
				xid, ctx.msg.Action, strings.Join(ctx.sids, ", "))
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

// +gen:payload apc.ActStopMaintenance={"action": "stop-maintenance", "value": {"sids": ["target_id1", "target_id2"]}}
func (p *proxy) stopMaintenance(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	const tag = "stop-maintenance:"
	var (
		opts apc.ActValRmNode
		smap = p.owner.smap.get()
	)
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	sids, err := _rmNodeIDs(&opts)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	var (
		nodes       = make(meta.Nodes, 0, len(sids))
		seen        = make(map[string]struct{}, len(sids))
		targetCount int
	)
	for _, sid := range sids {
		if _, ok := seen[sid]; ok {
			p.writeErrf(w, r, "node %q is specified more than once", sid)
			return
		}
		seen[sid] = struct{}{}
		si := smap.GetNode(sid)
		if si == nil {
			err := cos.NewErrNotFound(p, "node "+sid)
			p.writeErr(w, r, err, http.StatusNotFound)
			return
		}
		if !smap.InMaint(si) {
			p.writeErrf(w, r, "node %s is not in maintenance mode - nothing to do", si.StringEx())
			return
		}
		nodes = append(nodes, si)
		if si.IsTarget() {
			targetCount++
		}
	}
	if targetCount > 0 {
		if running, xid := p.notifs.isRebRunning(); running {
			p.writeErrf(w, r, "rebalance[%s] is currently running, please try (%s %s) later",
				xid, msg.Action, snodeNames(nodes))
			return
		}
	}
	pname := p.String()
	nlog.Infoln(tag, pname, "[", msg.Action, snodeNames(nodes), opts, "]")
	for _, si := range nodes {
		sname := si.StringEx()
		tout := cmn.Rom.CplaneOperation()
		if _, status, err := p.reqHealth(si, tout, nil, smap, false /*retry pub-addr*/); err != nil {
			sleep, retries := tout/2, 4

			// give a restarting node a full `tout` before the first retry
			// (initial startup grace plus the first retry interval)
			time.Sleep(sleep)
			for i := range retries {
				time.Sleep(sleep)
				_, status, err = p.reqHealth(si, tout, nil, smap, true /*retry pub-addr*/)
				if err == nil {
					nlog.Infoln(tag, pname, "=>", sname, "OK after", i+1, "attempt"+cos.Plural(i+1), "[", msg.Action, opts, "]")
					break
				}
				if status != http.StatusServiceUnavailable {
					p.writeErrf(w, r, "%s is unreachable: %v(%d)", sname, err, status)
					return
				}
				sleep = min(sleep+time.Second, tout)
			}
			if err != nil {
				debug.Assert(status == http.StatusServiceUnavailable)
				nlog.Errorf("%s: node %s takes unusually long time to start: %v(%d) - proceeding anyway",
					pname, sname, err, status)
			}
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

	rebID, err := p.mcastStopMaint(msg, nodes, reb)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if rebID != "" {
		writeXid(w, rebID)
	}
}

func _rmNodeIDs(opts *apc.ActValRmNode) ([]string, error) {
	sids := opts.Sids()
	switch {
	case len(sids) == 0:
		return nil, errors.New("missing node ID")
	case opts.DaemonID != "" && len(opts.DaemonIDs) != 0:
		return nil, errors.New("cannot specify both sid and sids")
	default:
		return sids, nil
	}
}

func snodeNames(nodes meta.Nodes) string {
	names := make([]string, 0, len(nodes))
	for _, si := range nodes {
		names = append(names, si.StringEx())
	}
	return strings.Join(names, ", ")
}

func (p *proxy) mcastStopMaint(msg *apc.ActMsg, nodes meta.Nodes, reb bool) (rebID string, err error) {
	sids := make([]string, 0, len(nodes))
	for _, si := range nodes {
		sids = append(sids, si.ID())
	}
	nlog.Infof("%s mcast-stopm: %s, %s, reb=%t", p, msg, snodeNames(nodes), reb)
	ctx := &smapModifier{
		pre:     p._stopMaintPre,
		post:    p._stopMaintRMD,
		final:   p._syncFinal,
		sids:    sids,
		skipReb: !reb,
		msg:     msg,
		flags:   meta.SnodeMaint | meta.SnodeMaintPostReb, // to clear node flags
	}
	err = p.owner.smap.modify(ctx)
	if err != nil && ctx.status != 0 {
		err = cmn.NewErrFailedTo(p, msg.Action, snodeNames(nodes), err, ctx.status)
	}
	if ctx.rmdCtx != nil && ctx.rmdCtx.cur != nil {
		debug.Assert(ctx.rmdCtx.cur.version() > ctx.rmdCtx.prev.version() && ctx.rmdCtx.rebID != "")
		rebID = ctx.rmdCtx.rebID
	}
	return
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
			return &errNodeNotFound{p.si, clone, fmt.Sprintf(efmt, sid), sid}
		}
		hasTarget = hasTarget || si.IsTarget()
	}
	if hasTarget {
		if running, xid := p.notifs.isRebRunning(); running {
			return fmt.Errorf("rebalance[%s] is currently running, please try (%s %s) later",
				xid, ctx.msg.Action, strings.Join(ctx.sids, ", "))
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
	debug.Assert(reqIsPub(r) == isPub)
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
		err = &errNodeNotFound{p.si, smap, "cannot remove", sid}
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

	if ecode, err := p.mcastUnreg(&apc.ActMsg{Action: apc.ActSelfRemove}, meta.Nodes{node}); err != nil {
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
