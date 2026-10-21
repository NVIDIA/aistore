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

	jsoniter "github.com/json-iterator/go"
)

// This source handles POST /v1/cluster/{operation}: admin-join, self-join, and slow keepalive.
//
// The handler is structured as a small sequential state machine carried by the `clupost` context.
// Phases are ordered:
//
// decode -> pre-authenticate -> validate -> handshake -> resume-reb -> version -> admit -> dispatch
//
// Rules:
// - fast keepalive returns before `clupost` is created;
// - when configured, explicit self-joins and restarted keepalives are authenticated (see docs/auth_node_join.md);
// - validation resolves node flags and the action for the current operation;
// - resumeReb may promote a restarted keepalive to self-join and update cluster action message;
// - joinKalive runs under the Smap lock and decides whether Smap mutation is needed;
// - dispatch performs externally visible responses and metasync/rebalance work.

type clupost struct {
	p      *proxy
	w      http.ResponseWriter
	r      *http.Request
	smap   *smapX
	bmd    *bucketMD
	config *cmn.Config
	nsi    *meta.Snode
	msg    *apc.ActMsg
	joined *smapX      // Smap that joinStartup modified under lock (to log outside)
	osi    *meta.Snode // node with the same ID that nsi replaced (see putNode)

	regReq cluMeta
	body   []byte // exact request bytes retained for admission authentication

	apiOp  string // handler path: version check, dispatch response, admission mode
	action string // cluster action message (apc.ActMsg) semantics

	nversStr    string      // enforce/track software versioning
	nversParsed cos.Version // ditto

	flags        cos.BitFlags // nsi.Flags snapshot, taken after flag resolution
	joinVerified bool         // when joining node has been already verified

	// keepalive with changed net-info: confirmed outside Smap lock (see handshake, kalive)
	confirm struct {
		osi     *meta.Snode
		err     error
		smapVer int64
		ok      bool
	}
}

// +gen:endpoint POST /v1/cluster/{operation}
// Handle cluster join operations and node keepalives.
func (p *proxy) httpclupost(w http.ResponseWriter, r *http.Request, isPub bool) {
	debug.AssertFunc(func() bool { return reqIsPub(r) == isPub })

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
		bmd    = p.owner.bmd.get()
		config = cmn.GCO.Get()
	)
	// fast keepalive (the length guard above guarantees apiOp == Keepalive)
	if len(apiItems) > 1 {
		p.fastKaliveRsp(w, r, smap, config, apiItems[1] /*sid*/)
		return
	}

	c := &clupost{p: p, w: w, r: r, smap: smap, bmd: bmd, config: config, apiOp: apiOp}

	// decode (per-op) and validate; resolve action and node flags; check node version
	if c.decode(isPub) {
		return
	}
	if err := c.verify(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if c.validate() {
		return
	}

	// handshake (admin-join) | duplicate check (self-join) | net-info confirmation (keepalive)
	if c.handshake() {
		return
	}

	// resume interrupted rebalance
	if c.resumeReb() {
		return
	}

	c.msg = &apc.ActMsg{Action: c.action, Name: c.nsi.ID()}

	// fast path: quick admission during cluster startup/restart
	p.owner.smap.mu.Lock()
	msync, _, err := c.joinKalive()
	if err == nil && (c.apiOp == apc.SelfJoin || c.apiOp == apc.AdminJoin) && !p.ClusterStarted() {
		// retain peer metadata together with its Smap entry for primary startup
		primary := p.primary()
		primary.reg.mpl.Lock()
		primary.reg.pool = append(primary.reg.pool, c.regReq)
		primary.reg.mpl.Unlock()
	}
	p.owner.smap.mu.Unlock()
	if c.joined != nil {
		_logPutNode(c.nsi, c.osi, c.joined, false /*silent*/)
	}
	if err != nil {
		if errors.Is(err, errKaliveRejected) {
			herr := cmn.NewErrHTTP(r, err, http.StatusConflict)
			herr.TypeCode = tcodeKaliveRejected
			p.writeErr(w, r, herr)
			return
		}
		p.writeErr(w, r, err)
		return
	}
	c.dispatch(msync)
}

// respond to fastKalive from nodes
func (p *proxy) fastKaliveRsp(w http.ResponseWriter, r *http.Request, smap *smapX, config *cmn.Config, sid string) {
	var (
		primary = p.primary()
		fast    = primary.fastKalive.Load()
	)
	if !fast {
		var (
			now       = mono.NanoTime()
			cfg       = config.Keepalive
			minUptime = max(cfg.Target.Interval.D(), cfg.Proxy.Interval.D()) << 1
		)
		if fast = p.keepalive.cluUptime(now) > minUptime; fast {
			primary.fastKalive.Store(true) // not resetting upon a change of primary
		}
	}
	if fast {
		var (
			senderID   = r.Header.Get(apc.HdrSenderID)
			senderSver = r.Header.Get(apc.HdrSenderSmapVer)
		)
		if senderID == sid && senderSver != "" && senderSver == smap.vstr {
			if si := smap.GetNode(sid); si != nil {
				if !p.fastKaliveVer(r, si, smap) {
					p.writeErr(w, r, errFastKalive, 0, Silent)
					return
				}
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

// fast keepalive: check and record the sender's version
// - pre-5.2 senders omit the header: accept unless placement is in use (slow path then rejects)
func (p *proxy) fastKaliveVer(r *http.Request, si *meta.Snode, smap *smapX) bool {
	ver := r.Header.Get(apc.HdrNodeVersion)

	// fast path
	if ver == cmn.VersionAIStore && p.primary().nodeVer(si.ID()) == ver {
		return true // same as primary and already recorded (via sync.Map)
	}

	// slow (and unlikely) path
	if ver == "" {
		return !placementInUse(smap, p.owner.bmd.get())
	}
	parsed, ok := cos.ParseVersion(ver)
	if !ok || enforceVerBoundary(ver) {
		return false
	}
	if !cos.VersionAtLeast(ver, cmn.MinVerPlacement) && placementInUse(smap, p.owner.bmd.get()) {
		return false
	}
	p.noteNodeVersion(si, ver, parsed)
	return true
}

/////////////
// clupost //
/////////////

// decode the request body and resolve nsi (per operation)
func (c *clupost) decode(isPub bool) (stop bool) {
	p, w, r := c.p, c.w, c.r
	switch c.apiOp {
	case apc.Keepalive:
		body, err := cmn.ReadBytes(r)
		if err != nil {
			p.writeErr(w, r, err)
			return true
		}
		c.body = body
		if err := jsoniter.Unmarshal(body, &c.regReq); err != nil {
			cmn.WriteErrJSON(w, r, &c.regReq, err)
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
		c.regReq.JoinWeight = c.nsi.PlacementWeight()
		si, err := p._getSI(c.nsi)
		if err != nil {
			p.writeErrf(w, r, "%s: failed to obtain node info from %s: %v", p.si, c.nsi.StringEx(), err)
			return true
		}
		// NOTE: node ID and 3-networks configuration is obtained from the node itself
		*c.nsi = *si
	case apc.SelfJoin: // (auto-join at node startup)
		body, err := cmn.ReadBytes(r)
		if err != nil {
			p.writeErr(w, r, err)
			return true
		}
		c.body = body
		if err := jsoniter.Unmarshal(body, &c.regReq); err != nil {
			cmn.WriteErrJSON(w, r, &c.regReq, err)
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

func (c *clupost) verify() error {
	switch c.apiOp {
	case apc.SelfJoin: // TODO: support admin-join as well
		return c._verify()
	case apc.Keepalive:
		if c.regReq.Flags.IsSet(cos.NodeRestarted) { // TODO: handle `resumeReb` cos.RebalanceInterrupted mutation
			return c._verify()
		}
	}
	return nil
}

func (c *clupost) _verify() error {
	if len(c.p.joinSecret) == 0 || c.joinVerified {
		return nil
	}
	maxSkew := c.config.Auth.NodeJoinNonceWindow()
	if err := verifyNodeJoin(nodeJoinRequestHMACDomain, c.p.joinSecret, c.body, c.r.Header, maxSkew); err != nil {
		return cmn.NewErrHTTP(c.r, err, http.StatusUnauthorized)
	}
	c.joinVerified = true
	return nil
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
		if err := c.checkVer(); err != nil {
			c.noteRejectedVer()
			c.p.writeErr(c.w, c.r, err, http.StatusConflict)
			return true
		}
		debug.AssertFunc(func() bool {
			return c.nsi.VerifyingKey == nil || cos.VersionAtLeast(c.nversStr, cmn.MinVerIntraClusterAuth)
		})

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
	if c.regReq.JoinWeight < 0 || (c.regReq.JoinWeight > 0 && !nsi.IsTarget()) {
		err := fmt.Errorf("invalid requested placement weight %d for %s (expecting a positive target weight)", c.regReq.JoinWeight, nsi.StringEx())
		p.writeErr(w, r, newErrJoinRejected(r, err))
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
		if err := c.bmd.validateUUID(c.regReq.BMD, p.si, nsi, ""); err != nil {
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
		if nsi.Flags != osi.Flags {
			nlog.Warningln(p.String(), nsi.StringEx(), "reported flags", nsi.Fl2S(),
				"vs", osi.Fl2S(), "in", c.smap.String(), "- overriding")
		}
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
			// apc.HdrNodeFlags header currently supports only proxy non-electability
			if !nsi.IsProxy() || flags != meta.SnodeNonElectable {
				p.writeErrf(w, r, "%s joining %s: expecting only 'non-electable' bit (and only proxies), got %s=%q",
					p, nsi, apc.HdrNodeFlags, s)
				return true
			}
			nsi.Flags = nsi.Flags.Set(meta.SnodeNonElectable)
		}
	} else if c.apiOp == apc.AdminJoin && nsi.IsProxy() {
		nsi.Flags = nsi.Flags.Clear(meta.SnodeNonElectable)
	}

	c.flags = nsi.Flags
	return false
}

// handshake (admin-join) | duplicate check (self-join) | net-info confirmation (keepalive)
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
		if osi := c.smap.GetNode(nsi.ID()); osi != nil && !osi.EqNetID(nsi) {
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
	case apc.Keepalive:
		// resumeReb will promote this request to self-join; kalive will not use confirmation
		if c.config.Rebalance.Enabled && c.regReq.Flags.IsSet(cos.NodeRestarted) {
			break
		}
		if osi := c.smap.GetNode(nsi.ID()); osi != nil && !osi.EqNetID(nsi) {
			c.confirm.osi = osi
			c.confirm.smapVer = c.smap.Version
			c.confirm.ok, c.confirm.err = c.confirmSnode(osi) // intra-cluster GET(what=snode)
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
		switch c.apiOp {
		case apc.SelfJoin:
			if len(c.p.joinSecret) > 0 {
				c.signJoinResponse(nil)
			}
		case apc.AdminJoin:
			// TODO: respond !updated (NOP)
			p.writeJSON(w, r, apc.JoinNodeResult{DaemonID: c.nsi.ID()}, "")
		}
		return
	}

	// go ahead to join
	nlog.Infof("%s: %s(%q) %s (%s)", p, c.apiOp, c.action, c.nsi.StringEx(), c.regReq.Smap)

	if c.apiOp == apc.AdminJoin {
		// disallow admin-join when membership change is in progress, and vice versa
		if err := p.beginMembership(c.msg.Action); err != nil {
			p.writeErr(w, r, err)
			return
		}
		defer p.endMembership()

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
		c.setVerHdr()
		p.writeJoinJSON(w, r, md, path.Join(c.msg.Action, c.nsi.ID()), nodeJoinResponseHMACDomain)
	}

	go func() {
		if _, err := c.mcastJoined(); err != nil {
			nlog.Errorf("%s: failed to join %s: %v", p, c.nsi.StringEx(), err)
		}
	}()
}

func (c *clupost) signJoinResponse(body []byte) {
	debug.Assert(len(c.p.joinSecret) > 0) // bootstrap loaded the configured secret
	debug.Assert(c.apiOp == apc.SelfJoin) // only self-join responses use this proof

	// An empty body authenticates a successful no-op self-join response.
	signNodeJoin(nodeJoinResponseHMACDomain, c.p.joinSecret, body, c.w.Header())
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
	// TODO: authenticate admin-join with node_join_secret_path before exchanging cluster metadata.

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
		c.nversStr = res.header.Get(apc.HdrNodeVersion) // peer version (not the admin caller's)
		if err = c.checkVer(); err != nil {
			c.noteRejectedVer()
			status = http.StatusConflict
		} else {
			c.nversParsed, _ = cos.ParseVersion(c.nversStr) // validated by checkNodeVer
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
	return c.nsi.EqNetID(si), nil
}

func (c *clupost) kalive(osi *meta.Snode) (bool, error) {
	p, nsi := c.p, c.nsi

	if !osi.EqNetID(nsi) {
		confirmed := c.confirm.osi
		same := confirmed == osi
		// double check that !same is indeed !same
		if !same && confirmed != nil && c.confirm.smapVer != c.smap.Version {
			same = confirmed.ID() == osi.ID() && confirmed.Type() == osi.Type() &&
				confirmed.SameIncarnation(osi) && confirmed.NetEq(osi) == nil
		}
		if !same {
			return false, fmt.Errorf("%w: %s registration changed while confirming - retry with the next keepalive",
				errKaliveRejected, nsi.StringEx())
		}
		ok, err := c.confirm.ok, c.confirm.err
		if err != nil {
			return false, fmt.Errorf("%w: failed to obtain node info from %s(%s): %v",
				errKaliveRejected, osi.StringEx(), osi.ControlNet.URL, err)
		}
		if !ok {
			return false, fmt.Errorf("%w: duplicate node ID %q (%s, %s)",
				errKaliveRejected, nsi.ID(), osi.StringEx(), nsi.StringEx())
		}
		// (renewal)
		nlog.Warningln(p.String(), "renewing registration (net-info changed)", nsi.StringEx(), "[", osi.NetEq(nsi), "]")
		return true, nil // NOTE: update cluster map
	}

	debug.Func(func() {
		if len(osi.VerifyingKey) == 0 || len(nsi.VerifyingKey) == 0 || cos.CryptoEqual(osi.VerifyingKey, nsi.VerifyingKey) {
			return
		}
		ofp, _ := cos.NodeVerifyingKeyFingerprint(osi.VerifyingKey)
		nfp, _ := cos.NodeVerifyingKeyFingerprint(nsi.VerifyingKey)
		debug.Assertf(false, "not expecting verifying key change: %s(flags %s, fp %q) => %s(flags %s, fp %q)",
			osi.StringEx(), osi.Fl2S(), ofp, nsi.StringEx(), nsi.Fl2S(), nfp)
	})

	p.keepalive.heardFrom(nsi.ID())
	return false, nil
}

func (c *clupost) rejectKalive(err error) error {
	if cos.VersionAtLeast(c.nversStr, cmn.MinVerKaliveReject) { // introduced isErrKaliveRejected
		return err
	}
	// pre-5.2 nodes - log instead
	nlog.Errorln(c.p.String(), err)
	return nil
}

func (c *clupost) rereg(osi *meta.Snode) bool {
	p, nsi := c.p, c.nsi

	switch {
	case !p.NodeStarted():
		// early-start: do not optimize this reg
		return true
	case !cos.CryptoEqual(osi.VerifyingKey, nsi.VerifyingKey):
		// keypairs are ephemeral: a restart must renew the Smap entry even when
		// daemon ID, network identity, and flags are otherwise unchanged
		ofp, _ := cos.NodeVerifyingKeyFingerprint(osi.VerifyingKey)
		nfp, _ := cos.NodeVerifyingKeyFingerprint(nsi.VerifyingKey)
		nlog.Warningf("%s: %s(flags %s, fp %q) => %s(flags %s, fp %q) - restarted with a new verifying key",
			p, osi.StringEx(), osi.Fl2S(), ofp, nsi.StringEx(), nsi.Fl2S(), nfp)
		return true
	case !osi.EqNetID(nsi) || osi.Flags != nsi.Flags: // see apc.HdrNodeFlags handling above
		// also ref0417 (ais/earlystart)
		nlog.Warningf("%s: renewing %s(flags %s) => %s(flags %s)", p, osi.StringEx(), osi.Fl2S(), nsi.StringEx(), nsi.Fl2S())
		return true
	default:
		nlog.Infoln(p.String(), "node", nsi.StringEx(), "is already _in_ - nothing to do")
		return false
	}
}

// executes under p.owner.smap lock
func (c *clupost) joinKalive() (msync, added bool, err error) {
	p := c.p
	c.smap = p.owner.smap.get()
	c.bmd = p.owner.bmd.get()
	osi := c.smap.GetNode(c.nsi.ID())
	if err := c.admit(osi); err != nil {
		return false, false, err
	}
	if osi != nil {
		if osi.Type() != c.nsi.Type() {
			err := fmt.Errorf("unexpected node type: osi=%s, nsi=%s, %s (%t)",
				osi.StringEx(), c.nsi.StringEx(), c.smap.StringEx(), c.apiOp == apc.Keepalive)
			debug.AssertNoErr(err)
			return false, false, err
		}
		switch {
		case c.apiOp == apc.Keepalive:
			msync, err = c.kalive(osi)
			if err != nil {
				if err = c.rejectKalive(err); err != nil {
					return false, false, err
				}
			}
		case c.regReq.Flags.IsSet(cos.NodeRestarted):
			msync = true
		default:
			msync = c.rereg(osi)
		}
		if !msync {
			c.noteVersion()
			c.setVerHdr()
			return false, false, nil
		}
	}
	if err := c.smap.validateUUID(p.si, c.regReq.Smap, c.nsi.StringEx(), 80 /* ciError */); err != nil {
		return false, false, err
	}
	if c.apiOp == apc.Keepalive {
		// self-join and admin-join already checked via handshake
		if _, err := c.smap.IsDupNet(c.nsi); err != nil {
			if cos.VersionAtLeast(c.nversStr, cmn.MinVerKaliveReject) { // introduced isErrKaliveRejected
				return false, false, fmt.Errorf("%w: %v", errKaliveRejected, err)
			}
			return false, false, errors.New(p.String() + ": " + err.Error()) // pre-5.2
		}
	}
	started, starting := p.ClusterStarted(), p.owner.rmd.starting.Load()
	if !started || starting {
		if err := c.joinStartup(started); err != nil {
			return false, false, err
		}
		c.noteVersion()
		return false, true, nil
	}
	c.noteVersion()
	return true, false, nil
}

// validate node-join under the Smap lock:
// - reject if this proxy is no longer primary
// - self-join and absent keepalive (re-admission): verify join secret when configured (see joinVerified)
// - admin-join: use the peer version obtained by its handshake; join-secret verification is TODO
// - every request, including no-op re-registration: enforce version boundary and Smap/BMD placement
// - primary startup: defer unweighted targets (see startupWeights)
func (c *clupost) admit(osi *meta.Snode) error {
	p, nsi, smap := c.p, c.nsi, c.smap
	if !smap.isPrimary(p.si) {
		return newErrNotPrimary(p.si, smap, "cannot "+c.apiOp+" "+nsi.StringEx())
	}
	if c.regReq.JoinWeight < 0 || (c.regReq.JoinWeight > 0 && !nsi.IsTarget()) {
		return newErrJoinRejected(c.r, fmt.Errorf("invalid requested placement weight %d for %s", c.regReq.JoinWeight, nsi.StringEx()))
	}

	absent := c.apiOp == apc.Keepalive && osi == nil
	if c.apiOp == apc.SelfJoin || absent {
		if err := c._verify(); err != nil {
			return err
		}
	}

	if err := c.checkVer(); err != nil {
		c.noteRejectedVer()
		return cmn.NewErrHTTP(c.r, err, http.StatusConflict)
	}
	if _, err := c.placementWeight(smap); err != nil {
		return newErrJoinRejected(c.r, err)
	}
	if absent {
		nlog.Warningln(p.String(), "keepalive", nsi.StringEx(), "- adding back to the", smap.StringEx())
	}
	return nil
}

// resolve and validate admission weight without modifying the Smap or joining node
// - Smap assignment wins; otherwise the requested weight
// - primary startup: no mean and no validation (see startupWeights)
func (c *clupost) placementWeight(smap *smapX) (int64, error) {
	if !c.nsi.IsTarget() {
		return 0, nil
	}
	if old := smap.GetTarget(c.nsi.ID()); old != nil && old.PlacementWeight() > 0 {
		return old.PlacementWeight(), nil // primary assignment wins, even after shutdown or crash
	}
	w := c.regReq.JoinWeight
	if c.p.primary().startingUp && !c.p.ClusterStarted() {
		// startup: defer to startupWeights
		return w, nil
	}
	if !smap.IsWeighted() {
		return 0, nil
	}
	if w == 0 {
		w = smap.MeanPlacementWeight()
	}
	weights := make(map[string]int64, len(smap.Tmap)+1)
	for tid, tsi := range smap.Tmap {
		weights[tid] = tsi.PlacementWeight()
	}
	weights[c.nsi.ID()] = w
	if _, err := meta.CheckPlacementWeights(weights); err != nil {
		return 0, fmt.Errorf("cannot admit %s with placement weight %d: %w (check local placement.weight or omit it to use the cluster mean)",
			c.nsi.StringEx(), w, err)
	}
	return w, nil
}

// joining node => primary: version boundary and placement
func (c *clupost) checkVer() error {
	if err := checkNodeVer(c.p.String(), c.nsi.StringEx(), c.nversStr); err != nil {
		return err
	}
	return c.checkJoinPlacement(c.smap, c.bmd)
}

// record the version of an existing member that fails checkVer (see checkSameVersion, startupWeights)
func (c *clupost) noteRejectedVer() {
	if c.nversStr == "" || c.smap.GetNode(c.nsi.ID()) == nil {
		return
	}
	if parsed, ok := cos.ParseVersion(c.nversStr); ok && !enforceVerBoundary(c.nversStr) {
		c.p.noteNodeVersion(c.nsi, c.nversStr, parsed)
	}
}

// record version of an accepted join or slow keepalive
func (c *clupost) noteVersion() {
	c.p.noteNodeVersion(c.nsi, c.nversStr, c.nversParsed)
}

// under Smap lock
func (c *clupost) joinStartup(started bool) error {
	p := c.p
	clone := c.smap.clone()
	// TODO [feature]: updated *nsi contents (e.g., different network) may not "survive" earlystart merge
	weight, err := c.placementWeight(clone)
	if err != nil {
		return newErrJoinRejected(c.r, err)
	}
	c.osi = clone.putNode(c.nsi, c.flags)
	if weight > 0 {
		c.nsi.SetPlacementWeight(weight)
	}
	c.joined = clone
	p.owner.smap.put(clone)

	// - `started` - metasync is fine and expected
	// - `started == false && startingUp == false` - when still starting up but already reconciled the weights
	if started || !p.primary().startingUp {
		actMsgExt := p.newAmsg(c.msg, nil)
		_ = p.metasyncer.sync(revsPair{clone, actMsgExt})
	}
	c.setVerHdr()
	return nil
}

func (c *clupost) mcastJoined() (string, error) {
	p := c.p
	ctx := &smapModifier{
		pre:         c._joinedPre,
		post:        c._joinedPost,
		final:       c._joinedFinal,
		nsi:         c.nsi,
		nversStr:    c.nversStr,
		msg:         c.msg,
		flags:       c.flags,
		interrupted: c.regReq.Flags.IsSet(cos.RebalanceInterrupted),
		restarted:   c.regReq.Flags.IsSet(cos.NodeRestarted),
	}
	if err := p._earlyGFN(ctx, ctx.nsi, c.msg.Action, true /*joining*/); err != nil {
		return "", err
	}
	defer p.stopEarlyGFN(ctx, ctx.nsi.ID())
	if err := p.owner.smap.modify(ctx); err != nil {
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

	return "", nil
}

func (c *clupost) _joinedPre(ctx *smapModifier, clone *smapX) error {
	p := c.p
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot add %s", ctx.nsi))
	}
	if err := c.checkJoinPlacement(clone, p.owner.bmd.get()); err != nil {
		return cmn.NewErrHTTP(nil, err, http.StatusConflict)
	}
	weight, err := c.placementWeight(clone)
	if err != nil {
		return newErrJoinRejected(nil, err)
	}
	ctx.osi = clone.putNode(ctx.nsi, ctx.flags)
	if weight > 0 {
		ctx.nsi.SetPlacementWeight(weight)
	}
	if ctx.nsi.IsProxy() {
		clone.staffIC()
	}
	return nil
}

// RMD is always transmitted to provide for its (RMD's) replication -
// done under Smap lock to serialize with respect to new joins.
func (c *clupost) _joinedPost(ctx *smapModifier, clone *smapX) {
	p := c.p
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
	ctx.rmdCtx = rmdCtx // smap modifier to reference the rmd one directly
	_rebPost(ctx, clone)
}

func (c *clupost) _joinedFinal(ctx *smapModifier, clone *smapX) {
	p := c.p
	_logPutNode(ctx.nsi, ctx.osi, clone, true /*silent*/)

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
		debug.AssertFunc(func() bool { return ctx.rmdCtx.prev.version() < ctx.rmdCtx.cur.version() })
		actMsgExt.UUID = ctx.rmdCtx.rebID
		pairs = append(pairs, revsPair{ctx.rmdCtx.cur, actMsgExt})
	}

	if tokens != nil {
		pairs = append(pairs, revsPair{tokens, actMsgExt})
	}
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}

func _logPutNode(nsi, old *meta.Snode, smap *smapX, silent bool) {
	if nsi.IsProxy() && nsi.Flags.IsSet(meta.SnodeNonElectable) {
		nlog.Warningln(nsi.String(), "won't be electable")
	}
	if old != nil {
		nlog.Warningln("same ID", old.StringEx(), "vs (joining)", nsi.StringEx(), "->", smap.StringEx())
		return
	}
	if nsi.IsTarget() {
		if w := nsi.PlacementWeight(); w > 0 {
			nlog.Infoln("joining", nsi.StringEx(), "placement weight:", w)
		}
	}
	if !silent {
		nlog.Infoln("joined", nsi.String(), "->", smap.StringEx())
	}
}
