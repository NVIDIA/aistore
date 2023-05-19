// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

//
// v1/cluster handlers
//

func (p *proxy) clusterHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpcluget(w, r)
	case http.MethodPost:
		p.httpclupost(w, r)
	case http.MethodPut:
		p.httpcluput(w, r)
	case http.MethodDelete:
		p.httpcludel(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

//
// GET /v1/cluster - query cluster states and stats
//

func (p *proxy) httpcluget(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
		what  = query.Get(apc.QparamWhat)
	)
	// always allow as the flow involves intra-cluster redirect
	// (ref 1377 for more context)
	if what == apc.WhatOneXactStatus {
		p.ic.xstatusOne(w, r)
		return
	}

	if err := p.checkAccess(w, r, nil, apc.AceShowCluster); err != nil {
		return
	}

	switch what {
	case apc.WhatAllXactStatus:
		p.ic.xstatusAll(w, r, query)
	case apc.WhatQueryXactStats:
		p.xquery(w, r, what, query)
	case apc.WhatAllRunningXacts:
		p.xgetRunning(w, r, what, query)
	case apc.WhatNodeStats:
		p.qcluStats(w, r, what, query)
	case apc.WhatSysInfo:
		p.qcluSysinfo(w, r, what, query)
	case apc.WhatMountpaths:
		p.qcluMountpaths(w, r, what, query)
	case apc.WhatRemoteAIS:
		all, err := p.getRemAises(true /*refresh*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, all, what)
	case apc.WhatTargetIPs:
		// Return comma-separated IPs of the targets.
		// It can be used to easily fill the `--noproxy` parameter in cURL.
		var (
			smap = p.owner.smap.Get()
			buf  = bytes.NewBuffer(nil)
		)
		for _, si := range smap.Tmap {
			if buf.Len() > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(si.PubNet.Hostname)
			buf.WriteByte(',')
			buf.WriteString(si.ControlNet.Hostname)
			buf.WriteByte(',')
			buf.WriteString(si.DataNet.Hostname)
		}
		w.Write(buf.Bytes())

	case apc.WhatClusterConfig:
		config := cmn.GCO.Get()
		// hide secret
		c := config.ClusterConfig
		c.Auth.Secret = "**********"
		p.writeJSON(w, r, &c, what)
	case apc.WhatBMD, apc.WhatSmapVote, apc.WhatSnode, apc.WhatSmap:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)
	default:
		p.writeErrf(w, r, fmtUnknownQue, what)
	}
}

// apc.WhatQueryXactStats (NOTE: may poll for quiescence)
func (p *proxy) xquery(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	var xactMsg xact.QueryMsg
	if err := cmn.ReadJSON(w, r, &xactMsg); err != nil {
		return
	}
	xactMsg.Kind, _ = xact.GetKindName(xactMsg.Kind) // convert display name => kind
	body := cos.MustMarshal(xactMsg)

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathXactions.S, Body: body, Query: query}
	args.to = cluster.Targets

	var (
		config      = cmn.GCO.Get()
		onlyRunning = xactMsg.OnlyRunning != nil && *xactMsg.OnlyRunning
	)
	args.timeout = config.Client.Timeout.D() // quiescence
	if !onlyRunning {
		args.timeout = config.Client.TimeoutLong.D()
	}

	results := p.bcastGroup(args)
	freeBcArgs(args)
	targetResults, erred := p._tresRaw(w, r, results)
	if erred {
		return
	}
	if len(targetResults) == 0 {
		smap := p.owner.smap.get()
		if smap.CountActiveTs() > 0 {
			p.writeErrStatusf(w, r, http.StatusNotFound, "%q not found", xactMsg.String())
			return
		}
		err := cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
		glog.Warningf("%s: %v, %s", p, err, smap)
	}
	p.writeJSON(w, r, targetResults, what)
}

// apc.WhatAllRunningXacts
func (p *proxy) xgetRunning(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	var xactMsg xact.QueryMsg
	if err := cmn.ReadJSON(w, r, &xactMsg); err != nil {
		return
	}
	xactMsg.Kind, _ = xact.GetKindName(xactMsg.Kind) // convert display name => kind
	body := cos.MustMarshal(xactMsg)

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathXactions.S, Body: body, Query: query}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcArgs(args)

	uniqueKindIDs := cos.StrSet{}
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			return
		}
		if len(res.bytes) == 0 {
			continue
		}
		var (
			kindIDs []string
			err     = jsoniter.Unmarshal(res.bytes, &kindIDs)
		)
		debug.AssertNoErr(err)
		for _, ki := range kindIDs {
			uniqueKindIDs.Set(ki)
		}
	}
	freeBcastRes(results)
	p.writeJSON(w, r, uniqueKindIDs.ToSlice(), what)
}

func (p *proxy) qcluSysinfo(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	var (
		config  = cmn.GCO.Get()
		timeout = config.Client.Timeout.D()
	)
	proxyResults, err := p._sysinfo(r, timeout, cluster.Proxies, query)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	out := &apc.ClusterSysInfoRaw{}
	out.Proxy = proxyResults

	targetResults, err := p._sysinfo(r, timeout, cluster.Targets, query)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	out.Target = targetResults
	_ = p.writeJSON(w, r, out, what)
}

func (p *proxy) getRemAises(refresh bool) (*cluster.Remotes, error) {
	smap := p.owner.smap.get()
	si, errT := smap.GetRandTarget()
	if errT != nil {
		return nil, errT
	}
	q := url.Values{apc.QparamWhat: []string{apc.WhatRemoteAIS}}
	if refresh {
		q[apc.QparamClusterInfo] = []string{"true"} // handshake to check connectivity and get remote Smap
	}
	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Path:   apc.URLPathDae.S,
			Query:  q,
		}
		cargs.timeout = cmn.Timeout.MaxKeepalive()
		cargs.cresv = cresBA{} // -> cmn.BackendInfoAIS
	}
	var (
		v   *cluster.Remotes
		res = p.call(cargs)
		err = res.toErr()
	)
	if err == nil {
		v = res.v.(*cluster.Remotes)
	}
	freeCargs(cargs)
	freeCR(res)
	return v, err
}

func (p *proxy) _sysinfo(r *http.Request, timeout time.Duration, to int, query url.Values) (cos.JSONRawMsgs, error) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: r.Method, Path: apc.URLPathDae.S, Query: query}
	args.timeout = timeout
	args.to = to
	results := p.bcastGroup(args)
	freeBcArgs(args)
	sysInfoMap := make(cos.JSONRawMsgs, len(results))
	for _, res := range results {
		if res.err != nil {
			err := res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		sysInfoMap[res.si.ID()] = res.bytes
	}
	freeBcastRes(results)
	return sysInfoMap, nil
}

func (p *proxy) qcluStats(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	targetStats, erred := p._queryTs(w, r, query)
	if targetStats == nil || erred {
		return
	}
	out := &stats.ClusterRaw{}
	out.Target = targetStats
	out.Proxy = p.statsT.GetStats()
	out.Proxy.Snode = p.si
	_ = p.writeJSON(w, r, out, what)
}

func (p *proxy) qcluMountpaths(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	targetMountpaths, erred := p._queryTs(w, r, query)
	if targetMountpaths == nil || erred {
		return
	}
	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	_ = p.writeJSON(w, r, out, what)
}

// helper methods for querying targets

func (p *proxy) _queryTs(w http.ResponseWriter, r *http.Request, query url.Values) (cos.JSONRawMsgs, bool) {
	var (
		err  error
		body []byte
	)
	if r.Body != nil {
		body, err = cmn.ReadBytes(r)
		if err != nil {
			p.writeErr(w, r, err)
			return nil, true
		}
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: r.Method, Path: apc.URLPathDae.S, Query: query, Body: body}
	args.timeout = cmn.Timeout.MaxKeepalive()
	results := p.bcastGroup(args)
	freeBcArgs(args)
	return p._tresRaw(w, r, results)
}

func (p *proxy) _tresRaw(w http.ResponseWriter, r *http.Request, results sliceResults) (tres cos.JSONRawMsgs, erred bool) {
	tres = make(cos.JSONRawMsgs, len(results))
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			tres, erred = nil, true
			return
		}
		tres[res.si.ID()] = res.bytes
	}
	freeBcastRes(results)
	return
}

// POST /v1/cluster - handles joins and keepalives
func (p *proxy) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		regReq cluMeta
		nsi    *meta.Snode
		apiOp  string // one of: admin-join, self-join, keepalive
		action string // msg.Action, one: apc.ActSelfJoinProxy, ...
	)
	apiItems, err := p.apiItems(w, r, 1, false, apc.URLPathClu.L)
	if err != nil {
		return
	}
	apiOp = apiItems[0]
	// Ignore keepalive beat if the primary is in transition.
	if p.inPrimaryTransition.Load() && apiOp == apc.Keepalive {
		return
	}
	if p.forwardCP(w, r, nil, "httpclupost") {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}

	// unmarshal and validate
	switch apiOp {
	case apc.AdminJoin: // administrative join
		if cmn.ReadJSON(w, r, &regReq.SI) != nil {
			return
		}
		nsi = regReq.SI
		// must be reachable and must respond
		si, err := p.getDaemonInfo(nsi)
		if err != nil {
			p.writeErrf(w, r, "%s: failed to obtain node info from %s: %v", p.si, nsi.StringEx(), err)
			return
		}
		// NOTE: ID _and_ 3-networks configuration is obtained from the node itself;
		// as far as ID, `aisnode` either:
		// (a) loads existing one,
		// (b) gets it from command line or env (see `envDaemonID`), or
		// (c) generates a new one (see `genDaemonID`)
		// - in that exact sequence.
		*nsi = *si
	case apc.SelfJoin: // auto-join at node startup
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		nsi = regReq.SI
		if !p.ClusterStarted() {
			p.reg.mu.Lock()
			p.reg.pool = append(p.reg.pool, regReq)
			p.reg.mu.Unlock()
		}
	case apc.Keepalive: // keep-alive
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		nsi = regReq.SI
	default:
		debug.Assert(false, apiOp) // must be one of the (3) above
		p.writeErrURL(w, r)
		return
	}

	if err := nsi.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	// given node and operation, set msg.Action
	switch apiOp {
	case apc.AdminJoin:
		if nsi.IsProxy() {
			action = apc.ActAdminJoinProxy
		} else {
			action = apc.ActAdminJoinTarget
		}
	case apc.SelfJoin:
		if nsi.IsProxy() {
			action = apc.ActSelfJoinProxy
		} else {
			action = apc.ActSelfJoinTarget
		}
	case apc.Keepalive:
		action = apc.ActKeepaliveUpdate // (must be an extremely rare case)
	}

	// more validation && non-electability
	if p.NodeStarted() {
		bmd := p.owner.bmd.get()
		if err := bmd.validateUUID(regReq.BMD, p.si, nsi, ""); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	var (
		nonElectable bool
	)
	if nsi.IsProxy() {
		s := r.URL.Query().Get(apc.QparamNonElectable)
		if nonElectable, err = cos.ParseBool(s); err != nil {
			glog.Errorf("%s: failed to parse %s for non-electability: %v", p, s, err)
		}
	}
	if err := validateHostname(nsi.PubNet.Hostname); err != nil {
		p.writeErrf(w, r, "%s: failed to %s %s - (err: %v)", p.si, apiOp, nsi.StringEx(), err)
		return
	}

	// node flags
	smap := p.owner.smap.get()
	if osi := smap.GetNode(nsi.ID()); osi != nil {
		nsi.Flags = osi.Flags
	}
	if nonElectable {
		nsi.Flags = nsi.Flags.Set(meta.SnodeNonElectable)
	}

	// handshake | check dup
	if apiOp == apc.AdminJoin {
		// call the node with cluster-metadata included
		if errCode, err := p.adminJoinHandshake(nsi, apiOp); err != nil {
			p.writeErr(w, r, err, errCode)
			return
		}
	} else if apiOp == apc.SelfJoin {
		// check for dup node ID
		if osi := smap.GetNode(nsi.ID()); osi != nil && !osi.Equals(nsi) {
			duplicate, err := p.detectDuplicate(osi, nsi)
			if err != nil {
				p.writeErrf(w, r, "failed to obtain node info: %v", err)
				return
			}
			if duplicate {
				p.writeErrf(w, r, "duplicate node ID %q (%s, %s)", nsi.ID(), osi.StringEx(), nsi.StringEx())
				return
			}
			glog.Warningf("%s: self-joining %s with duplicate node ID %q", p, nsi.StringEx(), nsi.ID())
		}
	}

	if !cmn.GCO.Get().Rebalance.Enabled {
		regReq.RebInterrupted, regReq.Restarted = false, false
	}
	if nsi.IsTarget() && (regReq.RebInterrupted || regReq.Restarted) {
		if a, b := p.ClusterStarted(), p.owner.rmd.starting.Load(); !a || b {
			// handle via rmd.starting + resumeReb
			if p.owner.rmd.interrupted.CAS(false, true) {
				glog.Warningf("%s: will resume rebalance %s(%t, %t)", p, nsi.StringEx(), regReq.RebInterrupted, regReq.Restarted)
			}
		}
	}
	// when keepalive becomes a new join
	if regReq.Restarted && apiOp == apc.Keepalive {
		apiOp = apc.SelfJoin
	}

	p.owner.smap.mu.Lock()
	upd, err := p._joinKalive(nsi, regReq.Smap, apiOp, nsi.Flags, &regReq)
	p.owner.smap.mu.Unlock()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if !upd {
		if apiOp == apc.AdminJoin {
			// TODO: respond !updated (NOP)
			p.writeJSON(w, r, apc.JoinNodeResult{DaemonID: nsi.ID()}, "")
		}
		return
	}

	msg := &apc.ActMsg{Action: action, Name: nsi.ID()}
	glog.Infof("%s: %s(%q) %s (%s)", p, apiOp, action, nsi.StringEx(), regReq.Smap)

	if apiOp == apc.AdminJoin {
		rebID, err := p.mcastJoined(nsi, msg, nsi.Flags, &regReq)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, apc.JoinNodeResult{DaemonID: nsi.ID(), RebalanceID: rebID}, "")
		return
	}

	if apiOp == apc.SelfJoin {
		// respond to the self-joining node with cluster-meta that does not include Smap
		meta, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, meta, path.Join(msg.Action, nsi.ID()))
	}

	go p.mcastJoined(nsi, msg, nsi.Flags, &regReq)
}

// when joining manually: update the node with cluster meta that does not include Smap
// (the later gets finalized and metasync-ed upon success)
func (p *proxy) adminJoinHandshake(nsi *meta.Snode, apiOp string) (int, error) {
	cm, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
	if err != nil {
		return http.StatusInternalServerError, err
	}
	glog.Infof("%s: %s %s => (%s)", p, apiOp, nsi.StringEx(), p.owner.smap.get().StringEx())

	cargs := allocCargs()
	{
		cargs.si = nsi
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathDaeAdminJoin.S, Body: cos.MustMarshal(cm)}
		cargs.timeout = cmn.Timeout.CplaneOperation()
	}
	res := p.call(cargs)
	err = res.err
	status := res.status
	if err != nil {
		if cos.IsRetriableConnErr(res.err) {
			err = fmt.Errorf("%s: failed to reach %s at %s:%s: %w",
				p.si, nsi.StringEx(), nsi.PubNet.Hostname, nsi.PubNet.Port, res.err)
		} else {
			err = res.errorf("%s: failed to %s %s: %v", p.si, apiOp, nsi.StringEx(), res.err)
		}
	}
	freeCargs(cargs)
	freeCR(res)
	return status, err
}

// executes under lock
func (p *proxy) _joinKalive(nsi *meta.Snode, regSmap *smapX, apiOp string, flags cos.BitFlags, regReq *cluMeta) (upd bool, err error) {
	smap := p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		err = newErrNotPrimary(p.si, smap, "cannot "+apiOp+" "+nsi.StringEx())
		return
	}

	keepalive := apiOp == apc.Keepalive
	osi := smap.GetNode(nsi.ID())
	if osi == nil {
		if keepalive {
			glog.Warningf("%s keepalive %s: adding back to the %s", p, nsi.StringEx(), smap)
		}
	} else {
		if osi.Type() != nsi.Type() {
			err = fmt.Errorf("unexpected node type: osi=%s, nsi=%s, %s (%t)",
				osi.StringEx(), nsi.StringEx(), smap, keepalive)
			return
		}
		if keepalive {
			upd = p.kalive(nsi, osi)
		} else if regReq.Restarted {
			upd = true
		} else {
			upd = p.rereg(nsi, osi)
		}
		if !upd {
			return
		}
	}
	// check for cluster integrity errors (cie)
	if err = smap.validateUUID(p.si, regSmap, nsi.StringEx(), 80 /* ciError */); err != nil {
		return
	}
	// whether IP is in use by a different node
	if _, err = smap.IsDupNet(nsi); err != nil {
		err = errors.New(p.String() + ": " + err.Error())
	}

	// when cluster's starting up
	if a, b := p.ClusterStarted(), p.owner.rmd.starting.Load(); !a || b {
		clone := smap.clone()
		clone.putNode(nsi, flags, false /*silent*/)
		p.owner.smap.put(clone)
		upd = false
		return
	}

	upd = err == nil
	return
}

func (p *proxy) kalive(nsi, osi *meta.Snode) bool {
	if !osi.Equals(nsi) {
		duplicate, err := p.detectDuplicate(osi, nsi)
		if err != nil {
			glog.Errorf("%s: %s(%s) failed to obtain node info: %v", p, nsi.StringEx(), nsi.PubNet.URL, err)
			return false
		}
		if duplicate {
			glog.Errorf("%s: %s(%s) is trying to keepalive with duplicate ID", p, nsi.StringEx(), nsi.PubNet.URL)
			return false
		}
		glog.Warningf("%s: renewing registration %s (info changed!)", p, nsi.StringEx())
		return true // NOTE: update cluster map
	}

	p.keepalive.heardFrom(nsi.ID(), false /*reset*/)
	return false
}

func (p *proxy) rereg(nsi, osi *meta.Snode) bool {
	if !p.NodeStarted() {
		return true
	}
	if osi.Equals(nsi) {
		glog.Infof("%s: %s is already *in*", p, nsi.StringEx())
		return false
	}
	glog.Warningf("%s: renewing %s %+v => %+v", p, nsi.StringEx(), osi, nsi)
	return true
}

func (p *proxy) mcastJoined(nsi *meta.Snode, msg *apc.ActMsg, flags cos.BitFlags, regReq *cluMeta) (xid string, err error) {
	ctx := &smapModifier{
		pre:         p._joinedPre,
		post:        p._joinedPost,
		final:       p._joinedFinal,
		nsi:         nsi,
		msg:         msg,
		flags:       flags,
		interrupted: regReq.RebInterrupted,
		restarted:   regReq.Restarted,
	}
	if err = p._earlyGFN(ctx, ctx.nsi); err != nil {
		return
	}
	if err = p.owner.smap.modify(ctx); err != nil {
		debug.AssertNoErr(err)
		return
	}
	// with rebalance
	if ctx.rmdCtx != nil && ctx.rmdCtx.cur != nil {
		debug.Assert(ctx.rmdCtx.rebID != "")
		xid = ctx.rmdCtx.rebID
		return
	}
	// cleanup target state
	if ctx.restarted || ctx.interrupted {
		go p.cleanupMark(ctx)
	}
	if ctx.gfn {
		aisMsg := p.newAmsgActVal(apc.ActStopGFN, nil) // "stop-gfn" timed
		aisMsg.UUID = ctx.nsi.ID()
		revs := revsPair{&smapX{Smap: meta.Smap{Version: ctx.nver}}, aisMsg}
		_ = p.metasyncer.notify(false /*wait*/, revs) // async, failed-cnt always zero
	}
	return
}

func (p *proxy) _earlyGFN(ctx *smapModifier, si *meta.Snode /*being added or removed*/) error {
	smap := p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		return newErrNotPrimary(p.si, smap, fmt.Sprintf("cannot add %s", si))
	}
	if si.IsProxy() {
		return nil
	}
	if err := p.canRebalance(); err != nil {
		if err == errRebalanceDisabled {
			err = nil
		}
		return err
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

// calls t.cleanupMark
func (p *proxy) cleanupMark(ctx *smapModifier) {
	var (
		val = cleanmark{OldVer: ctx.smap.version(), NewVer: ctx.nver,
			Interrupted: ctx.interrupted, Restarted: ctx.restarted,
		}
		msg     = apc.ActMsg{Action: apc.ActCleanupMarkers, Value: &val}
		cargs   = allocCargs()
		timeout = cmn.Timeout.CplaneOperation()
		sleep   = timeout >> 1
	)
	{
		cargs.si = ctx.nsi
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		cargs.timeout = timeout
	}
	time.Sleep(sleep)
	for i := 0; i < 4; i++ { // retry
		res := p.call(cargs)
		err := res.err
		freeCR(res)
		if err == nil {
			break
		}
		if cos.IsRetriableConnErr(err) {
			time.Sleep(sleep)
			glog.Warningf("%s: %v (cleanmark #%d)", p, err, i+1)
			continue
		}
		glog.Error(err)
		break
	}
	freeCargs(cargs)
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
	if err := p.canRebalance(); err != nil {
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
		tokens = p.authn.revokedTokenList()
		bmd    = p.owner.bmd.get()
		etlMD  = p.owner.etl.get()
		aisMsg = p.newAmsg(ctx.msg, bmd)
		pairs  = make([]revsPair, 0, 5)
	)
	if config, err := p.owner.config.get(); err != nil {
		glog.Error(err)
	} else if config != nil {
		pairs = append(pairs, revsPair{config, aisMsg})
	}
	pairs = append(pairs, revsPair{clone, aisMsg}, revsPair{bmd, aisMsg})
	if etlMD != nil && etlMD.version() > 0 {
		pairs = append(pairs, revsPair{etlMD, aisMsg})
	}

	reb := ctx.rmdCtx != nil && ctx.rmdCtx.rebID != ""
	if !reb {
		// replicate RMD across (existing nodes will drop it upon version comparison)
		rmd := p.owner.rmd.get()
		pairs = append(pairs, revsPair{rmd, aisMsg})
	} else {
		debug.Assert(ctx.rmdCtx.prev.version() < ctx.rmdCtx.cur.version())
		aisMsg.UUID = ctx.rmdCtx.rebID
		pairs = append(pairs, revsPair{ctx.rmdCtx.cur, aisMsg})
	}

	if tokens != nil {
		pairs = append(pairs, revsPair{tokens, aisMsg})
	}
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}

func (p *proxy) _syncFinal(ctx *smapModifier, clone *smapX) {
	var (
		aisMsg = p.newAmsg(ctx.msg, nil)
		pairs  = make([]revsPair, 0, 2)
		reb    = ctx.rmdCtx != nil && ctx.rmdCtx.rebID != ""
	)
	pairs = append(pairs, revsPair{clone, aisMsg})
	if reb {
		debug.Assert(ctx.rmdCtx.prev.version() < ctx.rmdCtx.cur.version())
		aisMsg.UUID = ctx.rmdCtx.rebID
		pairs = append(pairs, revsPair{ctx.rmdCtx.cur, aisMsg})
	}
	debug.Assert(clone._sgl != nil)
	wg := p.metasyncer.sync(pairs...)
	if ctx.rmdCtx != nil && ctx.rmdCtx.wait {
		wg.Wait()
	}
}

/////////////////////
// PUT /v1/cluster //
/////////////////////

// - cluster membership, including maintenance and decommission
// - start/stop xactions
// - rebalance
// - cluster-wide configuration
// - cluster membership, xactions, rebalance, configuration
func (p *proxy) httpcluput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 0, true, apc.URLPathClu.L)
	if err != nil {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	if daemon.stopping.Load() {
		p.writeErr(w, r, fmt.Errorf("%s is stopping", p), http.StatusServiceUnavailable)
		return
	}
	if !p.NodeStarted() {
		p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s is not ready yet (starting up)", p)
		return
	}
	if len(apiItems) == 0 {
		p.cluputJSON(w, r)
	} else {
		p.cluputQuery(w, r, apiItems[0])
	}
}

func (p *proxy) cluputJSON(w http.ResponseWriter, r *http.Request) {
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	if msg.Action != apc.ActSendOwnershipTbl {
		// must be primary
		if p.forwardCP(w, r, msg, "") {
			return
		}
		if err := p.pready(nil); err != nil {
			p.writeErr(w, r, err, http.StatusServiceUnavailable)
			return
		}
	}
	switch msg.Action {
	case apc.ActSetConfig:
		toUpdate := &cmn.ConfigToUpdate{}
		if err := cos.MorphMarshal(msg.Value, toUpdate); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		query := r.URL.Query()
		if transient := cos.IsParseBool(query.Get(apc.ActTransient)); transient {
			p.setCluCfgTransient(w, r, toUpdate, msg)
		} else {
			p.setCluCfgPersistent(w, r, toUpdate, msg)
		}
	case apc.ActResetConfig:
		p.resetCluCfgPersistent(w, r, msg)

	case apc.ActShutdownCluster:
		args := allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		args.to = cluster.AllNodes
		_ = p.bcastGroup(args)
		freeBcArgs(args)
		// self
		p.termKalive(msg.Action)
		p.shutdown(msg.Action)
	case apc.ActDecommissionCluster:
		var (
			opts apc.ActValRmNode
			args = allocBcArgs()
		)
		if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			p.writeErr(w, r, err)
			return
		}
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		args.to = cluster.AllNodes
		_ = p.bcastGroup(args)
		freeBcArgs(args)
		// self
		p.termKalive(msg.Action)
		p.decommission(msg.Action, &opts)
	case apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActShutdownNode, apc.ActRmNodeUnsafe:
		p.rmNode(w, r, msg)
	case apc.ActStopMaintenance:
		p.stopMaintenance(w, r, msg)

	case apc.ActResetStats:
		errorsOnly := msg.Value.(bool)
		p.statsT.ResetStats(errorsOnly)
		args := allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		p.bcastReqGroup(w, r, args, cluster.AllNodes)
		freeBcArgs(args)
	case apc.ActXactStart:
		p.xstart(w, r, msg)
	case apc.ActXactStop:
		p.xstop(w, r, msg)
	case apc.ActSendOwnershipTbl:
		p.sendOwnTbl(w, r, msg)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxy) setCluCfgPersistent(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToUpdate, msg *apc.ActMsg) {
	ctx := &configModifier{
		pre:      _setConfPre,
		final:    p._syncConfFinal,
		msg:      msg,
		toUpdate: toUpdate,
		wait:     true,
	}

	// NOTE: critical cluster-wide config updates requiring restart (of the cluster)
	if toUpdate.Net != nil && toUpdate.Net.HTTP != nil {
		from, _ := jsoniter.Marshal(cmn.GCO.Get().Net.HTTP)
		to, _ := jsoniter.Marshal(toUpdate.Net.HTTP)
		whingeToUpdate("net.http", string(from), string(to))
	}
	if toUpdate.Auth != nil {
		from, _ := jsoniter.Marshal(cmn.GCO.Get().Auth)
		to, _ := jsoniter.Marshal(toUpdate.Auth)
		whingeToUpdate("config.auth", string(from), string(to))
	}

	// do
	if _, err := p.owner.config.modify(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

func whingeToUpdate(what, from, to string) {
	glog.Warningf("Updating cluster %s configuration: setting %s", what, to)
	glog.Warningf("Prior-to-update %s values: %s", what, from)
	glog.Errorln("Warning: this update MAY require cluster restart")
}

func (p *proxy) resetCluCfgPersistent(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	if err := p.owner.config.resetDaemonConfig(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	body := cos.MustMarshal(msg)

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: body}
	p.bcastReqGroup(w, r, args, cluster.AllNodes)
	freeBcArgs(args)
}

func (p *proxy) setCluCfgTransient(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToUpdate, msg *apc.ActMsg) {
	if err := p.owner.config.setDaemonConfig(toUpdate, true /* transient */); err != nil {
		p.writeErr(w, r, err)
		return
	}
	msg.Value = toUpdate
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodPut,
		Path:   apc.URLPathDae.S,
		Body:   cos.MustMarshal(msg),
		Query:  url.Values{apc.ActTransient: []string{"true"}},
	}
	p.bcastReqGroup(w, r, args, cluster.AllNodes)
	freeBcArgs(args)
}

func _setConfPre(ctx *configModifier, clone *globalConfig) (updated bool, err error) {
	if err = clone.Apply(ctx.toUpdate, apc.Cluster); err != nil {
		return
	}
	updated = true
	return
}

func (p *proxy) _syncConfFinal(ctx *configModifier, clone *globalConfig) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAmsg(ctx.msg, nil)})
	if ctx.wait {
		wg.Wait()
	}
}

func (p *proxy) xstart(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		xargs = xact.ArgsMsg{}
	)
	if err := cos.MorphMarshal(msg.Value, &xargs); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	xargs.Kind, _ = xact.GetKindName(xargs.Kind) // display name => kind
	// rebalance
	if xargs.Kind == apc.ActRebalance {
		p.rebalanceCluster(w, r)
		return
	}

	xargs.ID = cos.GenUUID() // common for all targets

	// cluster-wide resilver
	if xargs.Kind == apc.ActResilver && xargs.DaemonID != "" {
		p.resilverOne(w, r, msg, xargs)
		return
	}

	// all the rest `startable` (see xaction/api.go)
	body := cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs})
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathXactions.S, Body: body}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.toErr())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)
	smap := p.owner.smap.get()
	nl := xact.NewXactNL(xargs.ID, xargs.Kind, &smap.Smap, nil)
	p.ic.registerEqual(regIC{smap: smap, nl: nl})
	w.Write([]byte(xargs.ID))
}

func (p *proxy) xstop(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		xargs = xact.ArgsMsg{}
	)
	if err := cos.MorphMarshal(msg.Value, &xargs); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	xargs.Kind, _ = xact.GetKindName(xargs.Kind) // display name => kind
	body := cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs})
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathXactions.S, Body: body}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.toErr())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)
}

func (p *proxy) rebalanceCluster(w http.ResponseWriter, r *http.Request) {
	// note operational priority over config-disabled `errRebalanceDisabled`
	if err := p.canRebalance(); err != nil && err != errRebalanceDisabled {
		p.writeErr(w, r, err)
		return
	}
	smap := p.owner.smap.get()
	if smap.CountTargets() < 2 {
		p.writeErr(w, r, &errNotEnoughTargets{p.si, smap, 2})
		return
	}
	if na := smap.CountActiveTs(); na < 2 {
		glog.Warningf("%s: not enough active targets (%d) - proceeding to rebalance anyway", p, na)
	}
	rmdCtx := &rmdModifier{
		pre:     rmdInc,
		final:   rmdSync, // metasync new rmd instance
		p:       p,
		smapCtx: &smapModifier{smap: smap},
	}
	_, err := p.owner.rmd.modify(rmdCtx)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	debug.Assert(rmdCtx.rebID != "")
	w.Write([]byte(rmdCtx.rebID))
}

func (p *proxy) resilverOne(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, xargs xact.ArgsMsg) {
	smap := p.owner.smap.get()
	si := smap.GetTarget(xargs.DaemonID)
	if si == nil {
		p.writeErrf(w, r, "cannot resilver %v: node must exist and be a target", si)
		return
	}

	body := cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs})
	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathXactions.S, Body: body}
	}
	res := p.call(cargs)
	freeCargs(cargs)
	if res.err != nil {
		p.writeErr(w, r, res.toErr())
	} else {
		nl := xact.NewXactNL(xargs.ID, xargs.Kind, &smap.Smap, nil)
		p.ic.registerEqual(regIC{smap: smap, nl: nl})
		w.Write([]byte(xargs.ID))
	}
	freeCR(res)
}

func (p *proxy) sendOwnTbl(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		smap  = p.owner.smap.get()
		dstID string
	)
	if err := cos.MorphMarshal(msg.Value, &dstID); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	dst := smap.GetProxy(dstID)
	if dst == nil {
		p.writeErrf(w, r, "%s: unknown proxy node p[%s]", p.si, dstID)
		return
	}
	if !smap.IsIC(dst) {
		p.writeErrf(w, r, "%s: not an IC member", dst)
		return
	}
	if smap.IsIC(p.si) && !p.si.Equals(dst) {
		// node has older version than dst node handle locally
		if err := p.ic.sendOwnershipTbl(dst); err != nil {
			p.writeErr(w, r, err)
		}
		return
	}
	// forward
	var (
		err   error
		cargs = allocCargs()
	)
	{
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathClu.S, Body: cos.MustMarshal(msg)}
		cargs.timeout = apc.DefaultTimeout
	}
	for pid, psi := range smap.Pmap {
		if !smap.IsIC(psi) || pid == dstID {
			continue
		}
		cargs.si = psi
		res := p.call(cargs)
		if res.err != nil {
			err = res.toErr()
		}
		freeCR(res)
	}
	if err != nil {
		p.writeErr(w, r, err)
	}
	freeCargs(cargs)
}

// gracefully remove node via apc.ActStartMaintenance, apc.ActDecommission, apc.ActShutdownNode
func (p *proxy) rmNode(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		opts apc.ActValRmNode
		smap = p.owner.smap.get()
	)
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	si := smap.GetNode(opts.DaemonID)
	if si == nil {
		err := cos.NewErrNotFound("%s: node %s", p.si, opts.DaemonID)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	var inMaint bool
	if smap.InMaintOrDecomm(si) {
		// only (maintenance => decommission|shutdown) permitted
		sname := si.StringEx()
		switch msg.Action {
		case apc.ActDecommissionNode, apc.ActDecommissionCluster,
			apc.ActShutdownNode, apc.ActShutdownCluster, apc.ActRmNodeUnsafe:
			onl := true
			flt := nlFilter{Kind: apc.ActRebalance, OnlyRunning: &onl}
			if nl := p.notifs.find(flt); nl != nil {
				p.writeErrf(w, r, "rebalance[%s] is currently running, please try (%s %s) later",
					nl.UUID(), msg.Action, si.StringEx())
				return
			}
			if !smap.InMaint(si) {
				glog.Errorln("Warning: " + sname + " is currently being decommissioned")
			}
			inMaint = true
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
	if p.SID() == opts.DaemonID {
		p.writeErrf(w, r, "%s is the current primary, cannot perform action %q on itself", p, msg.Action)
		return
	}

	glog.Infof("%s: %s(%s) %v", p, msg.Action, si.StringEx(), opts)

	switch {
	case si.IsProxy():
		if _, err := p.mcastMaint(msg, si, false /*reb*/, false /*maintPostReb*/); err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err))
			return
		}
		errCode, err := p.rmNodeFinal(msg, si, nil)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err), errCode)
		}
	case msg.Action == apc.ActRmNodeUnsafe: // target unsafe
		if !opts.SkipRebalance {
			err := errors.New("unsafe must be unsafe")
			debug.AssertNoErr(err)
			p.writeErr(w, r, err)
			return
		}
		errCode, err := p.rmNodeFinal(msg, si, nil)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err), errCode)
		}
	default: // target
		reb := !opts.SkipRebalance && cmn.GCO.Get().Rebalance.Enabled && !inMaint
		if reb {
			if err := p.canRebalance(); err != nil {
				p.writeErr(w, r, err)
				return
			}
			if err := p.beginRmTarget(si, msg); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		rebID, err := p.rmTarget(si, msg, reb)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err))
			return
		}
		if rebID != "" {
			w.Write(cos.UnsafeB(rebID))
		}
	}
}

func (p *proxy) rmTarget(si *meta.Snode, msg *apc.ActMsg, reb bool) (rebID string, err error) {
	var ctx *smapModifier
	if ctx, err = p.mcastMaint(msg, si, reb, false /*maintPostReb*/); err != nil {
		return
	}
	if !reb {
		_, err = p.rmNodeFinal(msg, si, ctx)
	} else if ctx.rmdCtx != nil {
		rebID = ctx.rmdCtx.rebID
		if rebID == "" && ctx.gfn { // stop early gfn
			aisMsg := p.newAmsgActVal(apc.ActStopGFN, nil)
			aisMsg.UUID = si.ID()
			revs := revsPair{&smapX{Smap: meta.Smap{Version: ctx.nver}}, aisMsg}
			_ = p.metasyncer.notify(false /*wait*/, revs) // async, failed-cnt always zero
		}
	}
	return
}

func (p *proxy) mcastMaint(msg *apc.ActMsg, si *meta.Snode, reb, maintPostReb bool) (ctx *smapModifier, err error) {
	var flags cos.BitFlags
	switch msg.Action {
	case apc.ActDecommissionNode:
		flags = meta.SnodeDecomm
	case apc.ActShutdownNode, apc.ActStartMaintenance:
		flags = meta.SnodeMaint
		if maintPostReb {
			debug.Assert(si.IsTarget())
			flags |= meta.SnodeMaintPostReb
		}
	default:
		err = fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActDecommissionNode, apc.ActStartMaintenance, apc.ActShutdownNode})
		return
	}
	ctx = &smapModifier{
		pre:     p._markMaint,
		post:    p._rebPostRm, // (rmdCtx.rmNode => p.rmNodeFinal when all done)
		final:   p._syncFinal,
		sid:     si.ID(),
		flags:   flags,
		msg:     msg,
		skipReb: !reb,
	}
	if err = p._earlyGFN(ctx, si); err != nil {
		return
	}
	if err = p.owner.smap.modify(ctx); err != nil {
		debug.AssertNoErr(err)
		return
	}
	return
}

func (p *proxy) _markMaint(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot put %s in maintenance", ctx.sid))
	}
	clone.setNodeFlags(ctx.sid, ctx.flags)
	clone.staffIC()
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
	rmdCtx.listen(rmdCtx.rmNode)
	ctx.rmdCtx = rmdCtx
}

func (p *proxy) stopMaintenance(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		opts apc.ActValRmNode
		smap = p.owner.smap.get()
	)
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	si := smap.GetNode(opts.DaemonID)
	if si == nil {
		err := cos.NewErrNotFound("%s: node %s", p.si, opts.DaemonID)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if !smap.InMaint(si) {
		p.writeErrf(w, r, "node %s is not in maintenance mode - nothing to do", si.StringEx())
		return
	}
	timeout := cmn.GCO.Get().Timeout.CplaneOperation.D()
	if _, status, err := p.Health(si, timeout, nil); err != nil {
		sleep, retries := timeout/2, 5
		time.Sleep(sleep)
		for i := 0; i < retries; i++ {
			time.Sleep(sleep)
			_, status, err = p.Health(si, timeout, nil)
			if err == nil {
				break
			}
			if status != http.StatusServiceUnavailable {
				p.writeErrf(w, r, "%s is unreachable: %v(%d)", si, err, status)
				return
			}
		}
		if err != nil {
			debug.Assert(status == http.StatusServiceUnavailable)
			glog.Errorf("%s: node %s takes unusually long time to start: %v(%d) - proceeding anyway",
				p.si, si, err, status)
		}
	}

	rebID, err := p.mcastStopMaint(msg, &opts)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if rebID != "" {
		w.Write(cos.UnsafeB(rebID))
	}
}

func (p *proxy) cluputQuery(w http.ResponseWriter, r *http.Request, action string) {
	if p.forwardCP(w, r, &apc.ActMsg{Action: action}, "") {
		return
	}
	if action != apc.ActAttachRemAis && action != apc.ActDetachRemAis {
		if err := p.pready(nil); err != nil {
			p.writeErr(w, r, err, http.StatusServiceUnavailable)
			return
		}
	}
	switch action {
	case apc.Proxy:
		// cluster-wide: designate a new primary proxy administratively
		p.cluSetPrimary(w, r)
	case apc.ActSetConfig: // set-config via query parameters and "?n1=v1&n2=v2..."
		var (
			query    = r.URL.Query()
			toUpdate = &cmn.ConfigToUpdate{}
			msg      = &apc.ActMsg{Action: action}
		)
		if err := toUpdate.FillFromQuery(query); err != nil {
			p.writeErrf(w, r, err.Error())
			return
		}
		if transient := cos.IsParseBool(query.Get(apc.ActTransient)); transient {
			p.setCluCfgTransient(w, r, toUpdate, msg)
		} else {
			p.setCluCfgPersistent(w, r, toUpdate, msg)
		}
	case apc.ActAttachRemAis, apc.ActDetachRemAis:
		p.attachDetachRemAis(w, r, action, r.URL.Query())
	}
}

func (p *proxy) attachDetachRemAis(w http.ResponseWriter, r *http.Request, action string, query url.Values) {
	what := query.Get(apc.QparamWhat)
	if what != apc.WhatRemoteAIS {
		p.writeErr(w, r, fmt.Errorf(fmtUnknownQue, what))
		return
	}
	if !p.ClusterStarted() {
		const fmerr = "(config-backends modifying) remote cluster: (%t, %s)"
		var timeout time.Duration
		for {
			time.Sleep(cmn.Timeout.MaxKeepalive())
			timeout += cmn.Timeout.MaxKeepalive()
			config := cmn.GCO.Get()
			if p.ClusterStarted() {
				break
			}
			if timeout > config.Timeout.Startup.D()/2 {
				p.writeErr(w, r, fmt.Errorf("%s: failed to attach "+fmerr, p, p.ClusterStarted(), config))
				return
			}
			glog.Errorf("%s: waiting to attach "+fmerr, p, p.ClusterStarted(), config)
		}
	}
	ctx := &configModifier{
		pre:   p._remaisConf,
		final: p._syncConfFinal,
		msg:   &apc.ActMsg{Action: action},
		query: query,
		hdr:   r.Header,
		wait:  true,
	}
	newConfig, err := p.owner.config.modify(ctx)
	if err != nil {
		p.writeErr(w, r, err)
	} else if newConfig != nil {
		go p._remais(&newConfig.ClusterConfig, false)
	}
}

// the flow: attach/detach remais => modify cluster config => _remaisConf as the pre phase
// of the transaction
func (p *proxy) _remaisConf(ctx *configModifier, config *globalConfig) (bool, error) {
	var (
		aisConf cmn.BackendConfAIS
		action  = ctx.msg.Action
		v       = config.Backend.Get(apc.AIS)
	)
	if v == nil {
		if action == apc.ActDetachRemAis {
			return false, fmt.Errorf("%s: remote cluster config is empty", p.si)
		}
		aisConf = make(cmn.BackendConfAIS)
	} else {
		aisConf = cmn.BackendConfAIS{}
		cos.MustMorphMarshal(v, &aisConf)
	}

	alias := ctx.hdr.Get(apc.HdrRemAisAlias)
	if action == apc.ActDetachRemAis {
		if _, ok := aisConf[alias]; !ok {
			return false,
				cmn.NewErrFailedTo(p, action, "remote cluster", errors.New("not found"), http.StatusNotFound)
		}
		delete(aisConf, alias)
		if len(aisConf) == 0 {
			aisConf = nil // unconfigure
		}
	} else {
		debug.Assert(action == apc.ActAttachRemAis)
		u := ctx.hdr.Get(apc.HdrRemAisURL)
		detail := fmt.Sprintf("remote cluster [alias %s => %v]", alias, u)

		// validation rules:
		// rule #1: no two remote ais clusters can share the same alias (TODO: allow configuring multiple URLs per)
		for a, urls := range aisConf {
			if a != alias {
				continue
			}
			errmsg := fmt.Sprintf("%s: %s is already attached", p.si, detail)
			if !cos.StringInSlice(u, urls) {
				return false, errors.New(errmsg)
			}
			glog.Warning(errmsg + " - proceeding anyway")
		}
		// rule #2: aliases and UUIDs are two distinct non-overlapping sets
		p.remais.mu.RLock()
		for _, remais := range p.remais.A {
			debug.Assert(remais.Alias != alias)
			if alias == remais.UUID {
				p.remais.mu.RUnlock()
				return false, fmt.Errorf("%s: alias %q cannot be equal UUID of an already attached cluster [%s => %s]",
					p.si, alias, remais.Alias, remais.UUID)
			}
		}
		p.remais.mu.RUnlock()

		parsed, err := url.ParseRequestURI(u)
		if err != nil {
			return false, cmn.NewErrFailedTo(p, action, detail, err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return false, cmn.NewErrFailedTo(p, action, detail, errors.New("invalid URL scheme"))
		}
		glog.Infof("%s: %s %s", p, action, detail)
		aisConf[alias] = []string{u}
	}
	config.Backend.Set(apc.AIS, aisConf)

	return true, nil
}

func (p *proxy) mcastStopMaint(msg *apc.ActMsg, opts *apc.ActValRmNode) (rebID string, err error) {
	ctx := &smapModifier{
		pre:     p._stopMaintPre,
		post:    p._newRMD,
		final:   p._syncFinal,
		sid:     opts.DaemonID,
		skipReb: opts.SkipRebalance,
		msg:     msg,
		flags:   meta.SnodeMaint | meta.SnodeMaintPostReb, // to clear node flags
	}
	err = p.owner.smap.modify(ctx)
	if ctx.rmdCtx != nil && ctx.rmdCtx.cur != nil {
		debug.Assert(ctx.rmdCtx.cur.version() > ctx.rmdCtx.prev.version() && ctx.rmdCtx.rebID != "")
		rebID = ctx.rmdCtx.rebID
	}
	return
}

func (p *proxy) _stopMaintPre(ctx *smapModifier, clone *smapX) error {
	const efmt = "cannot take %s out of maintenance"
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf(efmt, ctx.sid))
	}
	node := clone.GetNode(ctx.sid)
	if node == nil {
		ctx.status = http.StatusNotFound
		return &errNodeNotFound{fmt.Sprintf(efmt, ctx.sid), ctx.sid, p.si, clone}
	}
	clone.clearNodeFlags(ctx.sid, ctx.flags)
	if node.IsProxy() {
		clone.staffIC()
	}
	return nil
}

func (p *proxy) _newRMD(ctx *smapModifier, clone *smapX) {
	// e.g., taking node out of maintenance w/ no rebalance
	if ctx.skipReb {
		return
	}
	if !mustRebalance(ctx, clone) {
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

func (p *proxy) bmodSync(ctx *bmdModifier, clone *bucketMD) {
	debug.Assert(clone._sgl != nil)
	msg := p.newAmsg(ctx.msg, clone, ctx.txnID)
	wg := p.metasyncer.sync(revsPair{clone, msg})
	if ctx.wait {
		wg.Wait()
	}
}

func (p *proxy) cluSetPrimary(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 1, false, apc.URLPathCluProxy.L)
	if err != nil {
		return
	}
	npid := apiItems[0]
	if p.forwardCP(w, r, nil, "designate new primary proxy '"+npid+"'") {
		return
	}
	smap := p.owner.smap.get()
	psi := smap.GetProxy(npid)
	if psi == nil {
		p.writeErrf(w, r, "new primary proxy %s is not present in the %s", npid, smap.StringEx())
		return
	}
	if npid == p.SID() {
		debug.Assert(p.SID() == smap.Primary.ID()) // must be forwardCP-ed
		glog.Warningf("Request to set primary to %s(self) - nothing to do", npid)
		return
	}
	if smap.InMaintOrDecomm(psi) {
		var err error
		if smap.InMaint(psi) {
			err = fmt.Errorf("%s cannot become the new primary as it's currently under maintenance", psi)
		} else {
			err = fmt.Errorf("%s cannot become the new primary as it's currently being decommissioned", psi)
		}
		debug.AssertNoErr(err)
		p.writeErr(w, r, err, http.StatusServiceUnavailable)
		return
	}

	// (I.1) Prepare phase - inform other nodes.
	urlPath := apc.URLPathDaeProxy.Join(npid)
	q := url.Values{}
	q.Set(apc.QparamPrepare, "true")
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: urlPath, Query: q}

	cluMeta, errM := p.cluMeta(cmetaFillOpt{skipSmap: true})
	if errM != nil {
		p.writeErr(w, r, errM)
		return
	}
	args.req.Body = cos.MustMarshal(cluMeta)

	args.to = cluster.AllNodes
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := res.errorf("node %s failed to set primary %s in the prepare phase", res.si, npid)
		p.writeErr(w, r, err)
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	// (I.2) Prepare phase - local changes.
	p.inPrimaryTransition.Store(true)
	defer p.inPrimaryTransition.Store(false)

	err = p.owner.smap.modify(&smapModifier{pre: func(_ *smapModifier, clone *smapX) error {
		clone.Primary = psi
		p.metasyncer.becomeNonPrimary()
		return nil
	}})
	debug.AssertNoErr(err)

	// (II) Commit phase.
	q.Set(apc.QparamPrepare, "false")
	args = allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: urlPath, Query: q}
	args.to = cluster.AllNodes
	results = p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		if res.si.ID() == npid {
			cos.ExitLogf("commit phase failure: new primary %q returned %v", npid, res.err)
		} else {
			glog.Errorf("Commit phase failure: %s returned err %v when setting primary = %s",
				res.si.ID(), res.err, npid)
		}
	}
	freeBcastRes(results)
}

/////////////////////////////////////////
// DELET /v1/cluster - self-unregister //
/////////////////////////////////////////

func (p *proxy) httpcludel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 1, false, apc.URLPathCluDaemon.L)
	if err != nil {
		return
	}
	var (
		sid  = apiItems[0]
		smap = p.owner.smap.get()
		node = smap.GetNode(sid)
	)
	if node == nil {
		err = &errNodeNotFound{"cannot remove", sid, p.si, smap}
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
	if err := p.pready(smap); err != nil {
		p.writeErr(w, r, err, http.StatusServiceUnavailable)
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	if err := p.isIntraCall(r.Header, false /*from primary*/); err != nil {
		err = fmt.Errorf("expecting intra-cluster call for self-initiated removal, got %w", err)
		p.writeErr(w, r, err)
		return
	}
	cid := r.Header.Get(apc.HdrCallerID)
	if cid != sid {
		err = fmt.Errorf("expecting self-initiated removal (%s != %s)", cid, sid)
		p.writeErr(w, r, err)
		return
	}
	if errCode, err := p.mcastUnreg(&apc.ActMsg{Action: "self-initiated-removal"}, node); err != nil {
		p.writeErr(w, r, err, errCode)
	}
}

// post-rebalance or post no-rebalance - last step removing a node
// (with msg.Action defining semantics)
func (p *proxy) rmNodeFinal(msg *apc.ActMsg, si *meta.Snode, ctx *smapModifier) (int, error) {
	var (
		smap    = p.owner.smap.get()
		node    = smap.GetNode(si.ID())
		timeout = cmn.Timeout.CplaneOperation()
	)
	if node == nil {
		txt := "cannot \"" + msg.Action + "\""
		return http.StatusNotFound, &errNodeNotFound{txt, si.ID(), p.si, smap}
	}

	var (
		err     error
		errCode int
		cargs   = allocCargs()
		body    = cos.MustMarshal(msg)
		sname   = node.StringEx()
	)
	cargs.si, cargs.timeout = node, timeout
	switch msg.Action {
	case apc.ActShutdownNode, apc.ActRmNodeUnsafe, apc.ActStartMaintenance, apc.ActDecommissionNode:
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: body}
	default:
		return 0, fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActShutdownNode, apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActRmNodeUnsafe})
	}

	glog.Infof("%s: %s %s", p, msg.Action, sname)
	res := p.call(cargs)
	err = res.unwrap()
	freeCargs(cargs)
	freeCR(res)

	if err != nil {
		emsg := fmt.Sprintf("%s: (%s %s) final: %v - proceeding anyway...", p, msg, sname, err)
		switch msg.Action {
		case apc.ActShutdownNode, apc.ActDecommissionNode: // expecting EOF
			if !cos.IsEOF(err) {
				glog.Error(emsg)
			}
		case apc.ActRmNodeUnsafe:
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Error(emsg)
			}
		default:
			glog.Error(emsg)
		}
		err = nil // NOTE: proceeding anyway
	}

	switch msg.Action {
	case apc.ActDecommissionNode, apc.ActRmNodeUnsafe:
		errCode, err = p.mcastUnreg(msg, node)
	case apc.ActStartMaintenance, apc.ActShutdownNode:
		if ctx != nil && ctx.rmdCtx != nil && ctx.rmdCtx.rebID != "" {
			// final step executing shutdown and start-maintenance transaction:
			// setting si.Flags |= cluster.SnodeMaintPostReb
			// (compare w/ rmTarget --> p.mcastMaint above)
			_, err = p.mcastMaint(msg, node, false /*reb*/, true /*maintPostReb*/)
		}
	}
	if err != nil {
		glog.Errorf("%s: (%s %s) FATAL: failed to update %s: %v", p, msg, sname, p.owner.smap.get(), err)
	}
	return errCode, err
}

func (p *proxy) mcastUnreg(msg *apc.ActMsg, si *meta.Snode) (errCode int, err error) {
	ctx := &smapModifier{
		pre:     p._unregNodePre,
		final:   p._syncFinal,
		msg:     msg,
		sid:     si.ID(),
		skipReb: true,
	}
	err = p.owner.smap.modify(ctx)
	return ctx.status, err
}

func (p *proxy) _unregNodePre(ctx *smapModifier, clone *smapX) error {
	const verb = "remove"
	sid := ctx.sid
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot cancel %s %s", verb, sid))
	}
	node := clone.GetNode(sid)
	if node == nil {
		ctx.status = http.StatusNotFound
		return &errNodeNotFound{"failed to " + verb, sid, p.si, clone}
	}
	if node.IsProxy() {
		clone.delProxy(sid)
		glog.Infof("%s %s (num proxies %d)", verb, node.StringEx(), clone.CountProxies())
		clone.staffIC()
	} else {
		clone.delTarget(sid)
		glog.Infof("%s %s (num targets %d)", verb, node.StringEx(), clone.CountTargets())
	}
	p.rproxy.nodes.Delete(ctx.sid)
	return nil
}

// rebalance's `can`: factors not including cluster map
func (p *proxy) canRebalance() (err error) {
	if daemon.stopping.Load() {
		return fmt.Errorf("%s is stopping", p)
	}
	smap := p.owner.smap.get()
	if err = smap.validate(); err != nil {
		return
	}
	if !smap.IsPrimary(p.si) {
		err = newErrNotPrimary(p.si, smap)
		debug.AssertNoErr(err)
		return
	}
	// NOTE: cluster startup handles rebalance elsewhere (see p.resumeReb), and so
	// all rebalance-triggering events (shutdown, decommission, maintenance, etc.)
	// are not permitted and will fail during startup.
	if err = p.pready(smap); err != nil {
		return
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		err = errRebalanceDisabled
	}
	return
}

// rebalance's `must`: compares previous and current (cloned, updated) Smap
// TODO: bmd.num-buckets == 0 would be an easy one to check
func mustRebalance(ctx *smapModifier, cur *smapX) bool {
	if !cmn.GCO.Get().Rebalance.Enabled {
		return false
	}
	if daemon.stopping.Load() {
		return false
	}
	prev := ctx.smap
	if prev.CountActiveTs() == 0 {
		return false
	}
	if cur.CountActiveTs() < 2 {
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
