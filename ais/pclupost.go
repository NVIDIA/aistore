// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

// This source handles POST /v1/cluster/{operation}: admin-join, self-join, and slow keepalive.
//
// The handler is structured as a small sequential state machine carried by the `clupost` context.
// Phases are ordered:
//
// decode -> validate -> handshake -> resume-reb -> version -> admit -> dispatch
//
// Rules:
// - fast keepalive returns before `clupost` is created;
// - validation resolves node flags and the action for the current operation;
// - resumeReb may promote a restarted keepalive to self-join and update cluster action message;
// - _joinKalive runs under the Smap lock and decides whether Smap mutation is needed;
// - dispatch performs externally visible responses and metasync/rebalance work.

type clupost struct {
	p *proxy
	w http.ResponseWriter
	r *http.Request

	smap   *smapX
	config *cmn.Config

	nsi *meta.Snode
	msg *apc.ActMsg

	regReq cluMeta

	apiOp  string // handler path: version check, dispatch response, admission mode
	action string // cluster action message (apc.ActMsg) semantics

	nversStr    string
	nversParsed cos.Version

	flags cos.BitFlags // nsi.Flags snapshot, taken after flag resolution
}

// +gen:endpoint POST /v1/cluster/{operation}
// Handle cluster join operations and node keepalives.
func (p *proxy) httpclupost(w http.ResponseWriter, r *http.Request, isPub bool) {
	apiItems, err := p.parseURL(w, r, apc.URLPathClu.L, 1, true)
	if err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "httpclupost") {
		return
	}

	apiOp := apiItems[0]
	if len(apiItems) > 1 && apiOp != apc.Keepalive {
		p.writeErrURL(w, r)
		return
	}
	if p.settingNewPrimary.Load() {
		// ignore or fail
		if apiOp != apc.Keepalive {
			var s string
			if apiOp == apc.AdminJoin {
				s = " (retry in a few seconds)"
			}
			p.writeErr(w, r, errors.New("setting new primary - transitioning"+s), http.StatusServiceUnavailable)
		}
		return
	}

	var (
		smap   = p.owner.smap.get()
		config = cmn.GCO.Get()
	)
	// fast keepalive (the length guard above guarantees apiOp == Keepalive)
	if len(apiItems) > 1 {
		p.fastKaliveRsp(w, r, smap, config, apiItems[1] /*sid*/)
		return
	}

	c := &clupost{p: p, w: w, r: r, smap: smap, config: config, apiOp: apiOp}

	// decode (per-op) and validate; resolve action and node flags; check node version
	if c.decode(isPub) {
		return
	}
	if c.validate() {
		return
	}

	// handshake (admin-join) | duplicate check (self-join)
	if c.handshake() {
		return
	}

	// resume interrupted rebalance
	if c.resumeReb() {
		return
	}

	c.msg = &apc.ActMsg{Action: c.action, Name: c.nsi.ID()}

	// fast path: quick admission during cluster startup/restart // TODO: extra-checks for security
	p.owner.smap.mu.Lock()
	msync, added, err := c._joinKalive()
	p.owner.smap.mu.Unlock()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if c.apiOp == apc.SelfJoin && (!p.ClusterStarted() || added) {
		if !added {
			// motivation: preserve peer metadata even when it is already in local Smap
			nlog.Warningln(p.String(), "adding", c.nsi.StringEx(), "to startup reg.pool without local Smap add", c.smap.StringEx())
		}
		p.reg.mpl.Lock()
		p.reg.pool = append(p.reg.pool, c.regReq)
		p.reg.mpl.Unlock()
	}

	c.dispatch(msync)
}

// respond to fastKalive from nodes
func (p *proxy) fastKaliveRsp(w http.ResponseWriter, r *http.Request, smap *smapX, config *cmn.Config, sid string) {
	fast := p.readyToFastKalive.Load()
	if !fast {
		var (
			now       = mono.NanoTime()
			cfg       = config.Keepalive
			minUptime = max(cfg.Target.Interval.D(), cfg.Proxy.Interval.D()) << 1
		)
		if fast = p.keepalive.cluUptime(now) > minUptime; fast {
			p.readyToFastKalive.Store(true) // not resetting upon a change of primary
		}
	}
	if fast {
		var (
			senderID   = r.Header.Get(apc.HdrSenderID)
			senderSver = r.Header.Get(apc.HdrSenderSmapVer)
		)
		if senderID == sid && senderSver != "" && senderSver == smap.vstr {
			if si := smap.GetNode(sid); si != nil {
				now := p.keepalive.heardFrom(sid)

				// shared streams
				if si.IsTarget() {
					// (target kalive => primary)
					p.ec.recvKalive(p, r.Header, now, p.ec.timeout())
				} else {
					// (primary kalive response => non-primary)
					p.ec.respKalive(w.Header(), now, p.ec.timeout())
				}
				return
			}
		}
	}
	p.writeErr(w, r, errFastKalive, 0, Silent)
}

/////////////
// clupost //
/////////////

// decode the request body and resolve nsi (per operation)
func (c *clupost) decode(isPub bool) (stop bool) {
	p, w, r := c.p, c.w, c.r
	switch c.apiOp {
	case apc.Keepalive:
		if cmn.ReadJSON(w, r, &c.regReq) != nil {
			return true
		}
		c.nsi = c.regReq.SI
	case apc.AdminJoin: // (administrative join)
		debug.Assert(isPub)
		if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
			return true
		}
		if cmn.ReadJSON(w, r, &c.regReq.SI) != nil {
			return true
		}
		c.nsi = c.regReq.SI
		// must be reachable and must respond
		si, err := p._getSI(c.nsi)
		if err != nil {
			p.writeErrf(w, r, "%s: failed to obtain node info from %s: %v", p.si, c.nsi.StringEx(), err)
			return true
		}
		// NOTE: node ID and 3-networks configuration is obtained from the node itself
		*c.nsi = *si
	case apc.SelfJoin: // (auto-join at node startup)
		if cmn.ReadJSON(w, r, &c.regReq) != nil {
			return true
		}
		// NOTE: ditto
		c.nsi = c.regReq.SI
	default:
		p.writeErrURL(w, r)
		return true
	}
	return false
}

func (c *clupost) setActionCheckVer() (stop bool) {
	switch c.apiOp {
	case apc.AdminJoin:
		if c.nsi.IsProxy() {
			c.action = apc.ActAdminJoinProxy
		} else {
			c.action = apc.ActAdminJoinTarget
		}
	case apc.SelfJoin:
		p, w, r := c.p, c.w, c.r
		if err := checkNodeVer(p.String(), c.nsi.StringEx(), c.nversStr); err != nil {
			p.writeErr(w, r, err, http.StatusConflict)
			return true
		}
		p.noteNodeVersion(c.nsi, c.nversStr, c.nversParsed)
		debug.Assert(c.nsi.VerifyingKey == nil || cos.SupportsVersionAtLeast(c.nversStr, cos.Version{Major: 5, Minor: 1}))
		if cmn.IsV50Bridge() {
			c.nsi.VerifyingKey = nil
		}

		if c.nsi.IsProxy() {
			c.action = apc.ActSelfJoinProxy
		} else {
			c.action = apc.ActSelfJoinTarget
		}
	case apc.Keepalive:
		// slow keepalive only (fast path already returned via fastKaliveRsp)
		c.action = apc.ActKeepaliveUpdate
	default:
		debug.Assert(false, "unexpected op: ", c.apiOp)
		return true
	}

	return false
}

// validate the joining node; resolve msg.Action and node flags
func (c *clupost) validate() (stop bool) {
	p, w, r := c.p, c.w, c.r
	nsi := c.nsi
	if err := nsi.Validate(); err != nil {
		p.writeErr(w, r, err)
		return true
	}

	// parse the joining node's software version
	c.nversStr = r.Header.Get(apc.HdrNodeVersion)
	if c.nversStr != "" {
		nversParsed, ok := cos.ParseVersion(c.nversStr)
		if !ok {
			p.writeErrf(w, r, "%s joining %s: failed to parse '%s=%s'", p, c.nsi, apc.HdrNodeVersion, c.nversStr)
			return true
		}
		c.nversParsed = nversParsed
	}

	if c.setActionCheckVer() {
		return true
	}

	// more validation && non-electability
	if p.NodeStarted() {
		bmd := p.owner.bmd.get()
		if err := bmd.validateUUID(c.regReq.BMD, p.si, nsi, ""); err != nil {
			p.writeErr(w, r, err)
			return true
		}
	}
	// in re useIPv6 - joining node must have the same
	if _, err := cmn.ParseHost2IP(nsi.PubNet.Hostname, false /*local*/, g.netServ.control.useIPv6); err != nil {
		p.writeErrf(w, r, "%s: failed to %s %s: invalid hostname: %v", p.si, c.apiOp, nsi.StringEx(), err)
		return true
	}

	// node flags
	if osi := c.smap.GetNode(nsi.ID()); osi != nil {
		nsi.Flags = osi.Flags
	}
	if s := r.Header.Get(apc.HdrNodeFlags); s != "" {
		fl, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			p.writeErrf(w, r, "%s joining %s: failed to parse %s: %v", p, nsi, apc.HdrNodeFlags, err)
			return true
		}
		flags := cos.BitFlags(fl)
		if flags != 0 {
			nsi.Flags = nsi.Flags.Set(meta.SnodeNonElectable)
			// [NOTE]
			// - limiting support to 'non-electability'
			// - rest upon demand, including resetting non-electable -> electable
			if !nsi.IsProxy() || flags != meta.SnodeNonElectable {
				p.writeErrf(w, r, "%s joining %s: expecting only 'non-electable' bit (and only proxies), got %s=%s",
					p, nsi, apc.HdrNodeFlags, nsi.Fl2S())
				return true
			}
		}
	}

	c.flags = nsi.Flags
	return false
}

// handshake (admin-join) | duplicate check (self-join)
func (c *clupost) handshake() (stop bool) {
	p, w, r := c.p, c.w, c.r
	nsi := c.nsi
	switch c.apiOp {
	case apc.AdminJoin:
		// call the node with cluster-metadata included
		if ecode, err := c.adminJoinHandshake(); err != nil {
			p.writeErr(w, r, err, ecode)
			return true
		}
	case apc.SelfJoin:
		//
		// check for: a) different node, duplicate node ID, or b) same node, net-info change
		//
		if osi := c.smap.GetNode(nsi.ID()); osi != nil && !osi.Eq(nsi) {
			ok, err := c.confirmSnode(osi) // handshake (expecting nsi in response)
			if err != nil {
				if !cos.IsErrRetriableConn(err) {
					p.writeErrf(w, r, "failed to obtain node info: %v", err)
					return true
				}
				// starting up, not listening yet
				// NOTE [ref0417]
				// TODO: try to confirm asynchronously
			} else if !ok {
				p.writeErrf(w, r, "duplicate node ID %q (%s, %s)", nsi.ID(), osi.StringEx(), nsi.StringEx())
				return true
			}
			nlog.Warningf("%s: self-joining %s [err %v, confirmed %t]", p, nsi.StringEx(), err, ok)
		}
	}
	return false
}

// resume interrupted rebalance bookkeeping; promote a restarted keepalive to self-join
func (c *clupost) resumeReb() (stop bool) {
	if !c.config.Rebalance.Enabled {
		c.regReq.Flags = c.regReq.Flags.Clear(cos.RebalanceInterrupted)
		c.regReq.Flags = c.regReq.Flags.Clear(cos.NodeRestarted)
	}

	var (
		p           = c.p
		flags       = c.regReq.Flags
		interrupted = flags.IsSet(cos.RebalanceInterrupted)
		restarted   = flags.IsSet(cos.NodeRestarted)
	)
	if c.nsi.IsTarget() && (interrupted || restarted) {
		if a, b := p.ClusterStarted(), p.owner.rmd.starting.Load(); !a || b {
			// handle via rmd.starting + resumeReb
			if p.owner.rmd.interrupted.CAS(false, true) {
				nlog.Warningf("%s: will resume rebalance %s(%t, %t)", p, c.nsi.StringEx(), interrupted, restarted)
			}
		}
	}
	// when keepalive becomes a new join
	if restarted && c.apiOp == apc.Keepalive {
		c.apiOp = apc.SelfJoin

		// see rmdModifier.listen: ActSelfJoinTarget adds the joining target to reb-notifiers
		stop = c.setActionCheckVer()
	}

	return stop
}

// respond to the admitted (or not-updated) node and, when joining, broadcast the new Smap
func (c *clupost) dispatch(msync bool) {
	p, w, r := c.p, c.w, c.r
	if !msync {
		if c.apiOp == apc.AdminJoin {
			// TODO: respond !updated (NOP)
			p.writeJSON(w, r, apc.JoinNodeResult{DaemonID: c.nsi.ID()}, "")
		}
		return
	}

	// go ahead to join
	nlog.Infof("%s: %s(%q) %s (%s)", p, c.apiOp, c.action, c.nsi.StringEx(), c.regReq.Smap)

	if c.apiOp == apc.AdminJoin {
		rebID, err := c.mcastJoined()
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, apc.JoinNodeResult{DaemonID: c.nsi.ID(), RebalanceID: rebID}, "")
		return
	}

	if c.apiOp == apc.SelfJoin {
		// respond to the self-joining node with cluster-meta that does not include Smap
		md, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		c.setVerHdr() // primary => node: software-version exchange (case #2)
		p.writeJSON(w, r, md, path.Join(c.msg.Action, c.nsi.ID()))
	}

	go func() {
		if _, err := c.mcastJoined(); err != nil {
			nlog.Errorf("%s: failed to join %s: %v", p, c.nsi.StringEx(), err)
		}
	}()
}

// primary => node: stamp the response with our software version (see version-boundary enforcement)
func (c *clupost) setVerHdr() {
	c.w.Header().Set(apc.HdrNodeVersion, cmn.VersionAIStore)
}

// when joining manually: update the node with cluster meta that does not include Smap
// (the later gets finalized and metasync-ed upon success)
func (c *clupost) adminJoinHandshake() (int, error) {
	p, nsi := c.p, c.nsi
	cm, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
	if err != nil {
		return http.StatusInternalServerError, err
	}
	nlog.Infof("%s: %s %s => (%s)", p, c.apiOp, nsi.StringEx(), p.owner.smap.get().StringEx())

	cargs := allocCargs()
	{
		cargs.si = nsi
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathDaeAdminJoin.S, Body: cos.MustMarshal(cm)}
		cargs.timeout = cmn.Rom.CplaneOperation()
	}
	res := p.call(cargs, c.smap)
	err = res.err
	status := res.status
	if err != nil {
		if cos.IsErrRetriableConn(res.err) {
			err = fmt.Errorf("%s: failed to reach %s at %s:%s: %w",
				p.si, nsi.StringEx(), nsi.PubNet.Hostname, nsi.PubNet.Port, res.err)
		} else {
			err = res.errorf("%s: failed to %s %s: %v", p.si, c.apiOp, nsi.StringEx(), res.err)
		}
	} else {
		nversStr := res.header.Get(apc.HdrNodeVersion)
		if nversStr == "" {
			nlog.Warningf("%s: admin-joining %s did not advertise %s (pre-5.0?)",
				p, nsi.StringEx(), apc.HdrNodeVersion)
		}
	}
	freeCargs(cargs)
	freeCR(res)
	return status, err
}

func (c *clupost) confirmSnode(osi *meta.Snode) (bool, error) {
	p := c.p
	si, err := p._getSI(osi)
	if err != nil {
		return false, err
	}
	return c.nsi.Eq(si), nil
}

func (c *clupost) kalive(osi *meta.Snode) bool {
	p, nsi := c.p, c.nsi
	if !osi.Eq(nsi) {
		ok, err := c.confirmSnode(osi)
		if err != nil {
			nlog.Errorf("%s: %s(%s) failed to obtain node info: %v", p, nsi.StringEx(), nsi.PubNet.URL, err)
			return false
		}
		if !ok {
			nlog.Errorf("%s: %s(%s) is trying to keepalive with duplicate ID", p, nsi.StringEx(), nsi.PubNet.URL)
			return false
		}
		nlog.Warningf("%s: renewing registration %s (info changed!)", p, nsi.StringEx())
		return true // NOTE: update cluster map
	}

	p.keepalive.heardFrom(nsi.ID())
	return false
}

func (c *clupost) rereg(osi *meta.Snode) bool {
	p, nsi := c.p, c.nsi
	if !p.NodeStarted() {
		return true
	}
	if osi.Eq(nsi) && osi.Flags == nsi.Flags {
		nlog.Infoln(p.String(), "node", nsi.StringEx(), "is already _in_ - nothing to do")
		return false
	}

	// NOTE: also ref0417 (ais/earlystart)
	nlog.Warningf("%s: renewing %s(flags %s) => %s(flags %s)", p, osi.StringEx(), osi.Fl2S(), nsi.StringEx(), nsi.Fl2S())
	return true
}

// executes under p.owner.smap lock; may stamp response headers for accepted shortcut paths
func (c *clupost) _joinKalive() (msync, added bool, _ error) {
	var (
		p      = c.p
		nsi    = c.nsi
		apiOp  = c.apiOp
		flags  = c.flags
		regReq = &c.regReq
		msg    = c.msg
		smap   = p.owner.smap.get()
	)
	c.smap = smap
	if !smap.isPrimary(p.si) {
		return false, false, newErrNotPrimary(p.si, smap, "cannot "+apiOp+" "+nsi.StringEx())
	}

	var (
		keepalive = apiOp == apc.Keepalive
		osi       = smap.GetNode(nsi.ID())
	)
	if osi == nil {
		if keepalive {
			if err := checkNodeVer(p.String(), c.nsi.StringEx(), c.nversStr); err != nil {
				return false, false, err
			}
			nlog.Warningln(p.String(), "keepalive", nsi.StringEx(), "- adding back to the", smap.StringEx())
		}
	} else {
		if osi.Type() != nsi.Type() {
			err := fmt.Errorf("unexpected node type: osi=%s, nsi=%s, %s (%t)",
				osi.StringEx(), nsi.StringEx(), smap.StringEx(), keepalive)
			debug.AssertNoErr(err)
			return false, false, err
		}
		switch {
		case keepalive:
			msync = c.kalive(osi)
		case regReq.Flags.IsSet(cos.NodeRestarted):
			msync = true
		default:
			msync = c.rereg(osi)
		}
		if !msync {
			c.setVerHdr() // primary => node version exchange (case #1.a)
			return false, false, nil
		}
	}
	// check for cluster integrity errors (cie)
	if err := smap.validateUUID(p.si, regReq.Smap, nsi.StringEx(), 80 /* ciError */); err != nil {
		return false, false, err
	}

	var err error
	if keepalive {
		// whether IP is in use by a different node
		// (but only for keep-alive - the other two opcodes have been already checked via handshake)
		if _, err = smap.IsDupNet(nsi); err != nil {
			err = errors.New(p.String() + ": " + err.Error())
		}
	}
	// when cluster's starting up
	if a, b := p.ClusterStarted(), p.owner.rmd.starting.Load(); err == nil && (!a || b) {
		clone := smap.clone()
		// TODO [feature]: updated *nsi contents (e.g., different network) may not "survive" earlystart merge
		clone.putNode(nsi, flags, false /*silent*/)
		p.owner.smap.put(clone)
		if a {
			actMsgExt := p.newAmsg(msg, nil)
			_ = p.metasyncer.sync(revsPair{clone, actMsgExt})
		}
		c.setVerHdr() // primary => node version exchange (case #1.b)
		return false, true /*added => local Smap*/, nil
	}

	msync = err == nil
	return msync, false, err
}

func (c *clupost) mcastJoined() (string, error) {
	p := c.p
	ctx := &smapModifier{
		pre:         p._joinedPre,
		post:        p._joinedPost,
		final:       p._joinedFinal,
		nsi:         c.nsi,
		msg:         c.msg,
		flags:       c.flags,
		interrupted: c.regReq.Flags.IsSet(cos.RebalanceInterrupted),
		restarted:   c.regReq.Flags.IsSet(cos.NodeRestarted),
	}
	if err := p._earlyGFN(ctx, ctx.nsi, c.msg.Action, true /*joining*/); err != nil {
		return "", err
	}
	if err := p.owner.smap.modify(ctx); err != nil {
		debug.AssertNoErr(err)
		return "", err
	}
	// with rebalance
	if ctx.rmdCtx != nil && ctx.rmdCtx.cur != nil {
		debug.Assert(ctx.rmdCtx.rebID != "")
		return ctx.rmdCtx.rebID, nil // xid
	}

	// [NOTE]
	// one (arguably, cosmetic) side effect of not rebalancing is: markers and node state flags
	// e.g. when a node crashes (and then rejoins back again) in a cluster with rebalance disabled
	// the node's marker will state "restarted"
	// and will remain such until the cluster gets eventually rebalanced

	if ctx.gfn {
		actMsgExt := p.newAmsgActVal(apc.ActStopGFN, nil) // "stop-gfn" timed
		actMsgExt.UUID = ctx.nsi.ID()
		revs := revsPair{&smapX{Smap: meta.Smap{Version: ctx.nver}}, actMsgExt}
		_ = p.metasyncer.notify(false /*wait*/, revs) // async, failed-cnt always zero
	}
	return "", nil
}

func (p *proxy) _joinedPre(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot add %s", ctx.nsi))
	}
	clone.putNode(ctx.nsi, ctx.flags, true /*silent*/)
	if ctx.nsi.IsProxy() {
		clone.staffIC()
	}
	return nil
}

// RMD is always transmitted to provide for its (RMD's) replication -
// done under Smap lock to serialize with respect to new joins.
func (p *proxy) _joinedPost(ctx *smapModifier, clone *smapX) {
	if ctx.nsi.IsProxy() {
		return
	}
	if err := p.canRebalance(ctx.smap, false /*cleanup mode*/); err != nil {
		return
	}
	if !mustRebalance(ctx, clone) {
		return
	}
	// new RMD
	rmdCtx := &rmdModifier{
		pre: func(_ *rmdModifier, clone *rebMD) {
			clone.TargetIDs = []string{ctx.nsi.ID()}
			clone.inc()
		},
		smapCtx: ctx,
		p:       p,
		wait:    true,
	}
	if _, err := p.owner.rmd.modify(rmdCtx); err != nil {
		debug.AssertNoErr(err)
		return
	}
	rmdCtx.listen(nil)
	ctx.rmdCtx = rmdCtx // smap modifier to reference the rmd one directly
}

func (p *proxy) _joinedFinal(ctx *smapModifier, clone *smapX) {
	var (
		tokens    = p.authn.revokedTokenList()
		bmd       = p.owner.bmd.get()
		etlMD     = p.owner.etl.get()
		actMsgExt = p.newAmsg(ctx.msg, bmd)
		pairs     = make([]revsPair, 0, 6)
	)
	// when targets join as well (redundant?, minor)
	config, err := p.ensureConfigURLs()
	if config == nil /*not updated*/ && err == nil {
		config, err = p.owner.config.get()
	}
	if err != nil {
		nlog.Errorln(err)
		// proceed anyway
	} else if config != nil {
		pairs = append(pairs, revsPair{config, actMsgExt})
	}

	pairs = append(pairs, revsPair{clone, actMsgExt}, revsPair{bmd, actMsgExt})
	if etlMD != nil && etlMD.version() > 0 {
		pairs = append(pairs, revsPair{etlMD, actMsgExt})
	}

	reb := ctx.rmdCtx != nil && ctx.rmdCtx.rebID != ""
	if !reb {
		// replicate RMD across (existing nodes will drop it upon version comparison)
		rmd := p.owner.rmd.get()
		pairs = append(pairs, revsPair{rmd, actMsgExt})
	} else {
		debug.Assert(ctx.rmdCtx.prev.version() < ctx.rmdCtx.cur.version())
		actMsgExt.UUID = ctx.rmdCtx.rebID
		pairs = append(pairs, revsPair{ctx.rmdCtx.cur, actMsgExt})
	}

	if tokens != nil {
		pairs = append(pairs, revsPair{tokens, actMsgExt})
	}
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}
