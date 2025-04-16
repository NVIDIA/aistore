// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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

// (compare w/ httpdaeget)
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
	case apc.WhatBackends:
		config := cmn.GCO.Get()
		out := make([]string, 0, len(config.Backend.Providers))
		for b := range config.Backend.Providers {
			out = append(out, b)
		}
		p.writeJSON(w, r, out, what)
	case apc.WhatRemoteAIS:
		all, err := p.getRemAisVec(true /*refresh*/)
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
		w.Header().Set(cos.HdrContentLength, strconv.Itoa(buf.Len()))
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
	args.to = core.Targets

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
	resRaw, erred := p._tresRaw(w, r, results)
	if erred {
		return
	}
	if len(resRaw) == 0 {
		smap := p.owner.smap.get()
		if smap.CountActiveTs() > 0 {
			p.writeErrStatusf(w, r, http.StatusNotFound, "%q not found", xactMsg.String())
			return
		}
		err := cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
		nlog.Warningf("%s: %v, %s", p, err, smap)
	}

	// TODO: if voteInProgress snap and append xele, or else

	p.writeJSON(w, r, resRaw, what)
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
	args.to = core.Targets
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
	proxyResults, err := p._sysinfo(r, timeout, core.Proxies, query)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	out := &apc.ClusterSysInfoRaw{}
	out.Proxy = proxyResults

	targetResults, err := p._sysinfo(r, timeout, core.Targets, query)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	out.Target = targetResults
	p.writeJSON(w, r, out, what)
}

func (p *proxy) getRemAisVec(refresh bool) (*meta.RemAisVec, error) {
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
		cargs.timeout = cmn.Rom.MaxKeepalive()
		cargs.cresv = cresjGeneric[meta.RemAisVec]{}
	}
	var (
		v   *meta.RemAisVec
		res = p.call(cargs, smap)
		err = res.toErr()
	)
	if err == nil {
		v = res.v.(*meta.RemAisVec)
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
	p.writeJSON(w, r, out, what)
}

func (p *proxy) qcluMountpaths(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	targetMountpaths, erred := p._queryTs(w, r, query)
	if targetMountpaths == nil || erred {
		return
	}
	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	p.writeJSON(w, r, out, what)
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
	args.timeout = cmn.Rom.MaxKeepalive()
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
	apiItems, err := p.parseURL(w, r, apc.URLPathClu.L, 1, true)
	if err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "httpclupost") {
		return
	}

	var (
		nsi    *meta.Snode
		action string
		regReq cluMeta
		smap   = p.owner.smap.get()
		config = cmn.GCO.Get()
		apiOp  = apiItems[0]
	)
	if len(apiItems) > 1 && apiOp != apc.Keepalive {
		p.writeErrURL(w, r)
		return
	}
	if p.settingNewPrimary.Load() {
		// ignore of fail
		if apiOp != apc.Keepalive {
			var s string
			if apiOp == apc.AdminJoin {
				s = " (retry in a few seconds)"
			}
			p.writeErr(w, r, errors.New("setting new primary - transitioning"+s), http.StatusServiceUnavailable)
		}
		return
	}

	switch apiOp {
	case apc.Keepalive:
		// fast path
		if len(apiItems) > 1 {
			p.fastKaliveRsp(w, r, smap, config, apiItems[1] /*sid*/)
			return
		}

		// slow path
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		nsi = regReq.SI
	case apc.AdminJoin: // (administrative join)
		if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
			return
		}
		if cmn.ReadJSON(w, r, &regReq.SI) != nil {
			return
		}
		nsi = regReq.SI
		// must be reachable and must respond
		si, err := p._getSI(nsi)
		if err != nil {
			p.writeErrf(w, r, "%s: failed to obtain node info from %s: %v", p.si, nsi.StringEx(), err)
			return
		}
		// NOTE: node ID and 3-networks configuration is obtained from the node itself
		*nsi = *si
	case apc.SelfJoin: // (auto-join at node startup)
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		// NOTE: ditto
		nsi = regReq.SI
		if !p.ClusterStarted() {
			p.reg.mu.Lock()
			p.reg.pool = append(p.reg.pool, regReq)
			p.reg.mu.Unlock()
		}
	default:
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
	if _, err := cmn.ParseHost2IP(nsi.PubNet.Hostname); err != nil {
		p.writeErrf(w, r, "%s: failed to %s %s: invalid hostname: %v", p.si, apiOp, nsi.StringEx(), err)
		return
	}

	// node flags
	if osi := smap.GetNode(nsi.ID()); osi != nil {
		nsi.Flags = osi.Flags
	}
	if s := r.Header.Get(apc.HdrNodeFlags); s != "" {
		fl, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			p.writeErrf(w, r, "%s joining %s: failed to parse %s: %v", p, nsi, apc.HdrNodeFlags, err)
			return
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
				return
			}
		}
	}

	// handshake | check dup
	switch apiOp {
	case apc.AdminJoin:
		// call the node with cluster-metadata included
		if ecode, err := p.adminJoinHandshake(smap, nsi, apiOp); err != nil {
			p.writeErr(w, r, err, ecode)
			return
		}
	case apc.SelfJoin:
		//
		// check for: a) different node, duplicate node ID, or b) same node, net-info change
		//
		if osi := smap.GetNode(nsi.ID()); osi != nil && !osi.Eq(nsi) {
			ok, err := p._confirmSnode(osi, nsi) // handshake (expecting nsi in response)
			if err != nil {
				if !cos.IsRetriableConnErr(err) {
					p.writeErrf(w, r, "failed to obtain node info: %v", err)
					return
				}
				// starting up, not listening yet
				// NOTE [ref0417]
				// TODO: try to confirm asynchronously
			} else if !ok {
				p.writeErrf(w, r, "duplicate node ID %q (%s, %s)", nsi.ID(), osi.StringEx(), nsi.StringEx())
				return
			}
			nlog.Warningf("%s: self-joining %s [err %v, confirmed %t]", p, nsi.StringEx(), err, ok)
		}
	}

	if !config.Rebalance.Enabled {
		regReq.Flags = regReq.Flags.Clear(cos.RebalanceInterrupted)
		regReq.Flags = regReq.Flags.Clear(cos.NodeRestarted)
	}
	interrupted, restarted := regReq.Flags.IsSet(cos.RebalanceInterrupted), regReq.Flags.IsSet(cos.NodeRestarted)
	if nsi.IsTarget() && (interrupted || restarted) {
		if a, b := p.ClusterStarted(), p.owner.rmd.starting.Load(); !a || b {
			// handle via rmd.starting + resumeReb
			if p.owner.rmd.interrupted.CAS(false, true) {
				nlog.Warningf("%s: will resume rebalance %s(%t, %t)", p, nsi.StringEx(), interrupted, restarted)
			}
		}
	}
	// when keepalive becomes a new join
	if restarted && apiOp == apc.Keepalive {
		apiOp = apc.SelfJoin
	}

	msg := &apc.ActMsg{Action: action, Name: nsi.ID()}

	p.owner.smap.mu.Lock()
	upd, err := p._joinKalive(nsi, regReq.Smap, apiOp, nsi.Flags, &regReq, msg)
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

	// go ahead to join
	nlog.Infof("%s: %s(%q) %s (%s)", p, apiOp, action, nsi.StringEx(), regReq.Smap)

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
		md, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, md, path.Join(msg.Action, nsi.ID()))
	}

	go p.mcastJoined(nsi, msg, nsi.Flags, &regReq)
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
			callerID   = r.Header.Get(apc.HdrCallerID)
			callerSver = r.Header.Get(apc.HdrCallerSmapVer)
		)
		if callerID == sid && callerSver != "" && callerSver == smap.vstr {
			if si := smap.GetNode(sid); si != nil {
				now := p.keepalive.heardFrom(sid)

				if si.IsTarget() {
					p._recvActiveEC(r.Header, now)
				} else {
					p._respActiveEC(w.Header(), now)
				}
				return
			}
		}
	}
	p.writeErr(w, r, errFastKalive, 0, Silent)
}

// when joining manually: update the node with cluster meta that does not include Smap
// (the later gets finalized and metasync-ed upon success)
func (p *proxy) adminJoinHandshake(smap *smapX, nsi *meta.Snode, apiOp string) (int, error) {
	cm, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
	if err != nil {
		return http.StatusInternalServerError, err
	}
	nlog.Infof("%s: %s %s => (%s)", p, apiOp, nsi.StringEx(), p.owner.smap.get().StringEx())

	cargs := allocCargs()
	{
		cargs.si = nsi
		cargs.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathDaeAdminJoin.S, Body: cos.MustMarshal(cm)}
		cargs.timeout = cmn.Rom.CplaneOperation()
	}
	res := p.call(cargs, smap)
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
func (p *proxy) _joinKalive(nsi *meta.Snode, regSmap *smapX, apiOp string, flags cos.BitFlags, regReq *cluMeta, msg *apc.ActMsg) (upd bool, _ error) {
	smap := p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		return false, newErrNotPrimary(p.si, smap, "cannot "+apiOp+" "+nsi.StringEx())
	}

	var (
		keepalive = apiOp == apc.Keepalive
		osi       = smap.GetNode(nsi.ID())
	)
	if osi == nil {
		if keepalive {
			nlog.Warningln(p.String(), "keepalive", nsi.StringEx(), "- adding back to the", smap.StringEx())
		}
	} else {
		if osi.Type() != nsi.Type() {
			err := fmt.Errorf("unexpected node type: osi=%s, nsi=%s, %s (%t)",
				osi.StringEx(), nsi.StringEx(), smap.StringEx(), keepalive)
			debug.AssertNoErr(err)
			return false, err
		}
		switch {
		case keepalive:
			upd = p.kalive(nsi, osi)
		case regReq.Flags.IsSet(cos.NodeRestarted):
			upd = true
		default:
			upd = p.rereg(nsi, osi)
		}
		if !upd {
			return false, nil // ==> nothing to do
		}
	}
	// check for cluster integrity errors (cie)
	if err := smap.validateUUID(p.si, regSmap, nsi.StringEx(), 80 /* ciError */); err != nil {
		return false, err
	}

	var (
		err error
	)
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
		return false, nil
	}

	upd = err == nil
	return upd, err
}

func (p *proxy) _confirmSnode(osi, nsi *meta.Snode) (bool, error) {
	si, err := p._getSI(osi)
	if err != nil {
		return false, err
	}
	return nsi.Eq(si), nil
}

func (p *proxy) kalive(nsi, osi *meta.Snode) bool {
	if !osi.Eq(nsi) {
		ok, err := p._confirmSnode(osi, nsi)
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

func (p *proxy) rereg(nsi, osi *meta.Snode) bool {
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

func (p *proxy) mcastJoined(nsi *meta.Snode, msg *apc.ActMsg, flags cos.BitFlags, regReq *cluMeta) (string, error) {
	ctx := &smapModifier{
		pre:         p._joinedPre,
		post:        p._joinedPost,
		final:       p._joinedFinal,
		nsi:         nsi,
		msg:         msg,
		flags:       flags,
		interrupted: regReq.Flags.IsSet(cos.RebalanceInterrupted),
		restarted:   regReq.Flags.IsSet(cos.NodeRestarted),
	}
	if err := p._earlyGFN(ctx, ctx.nsi, msg.Action, true /*joining*/); err != nil {
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

func (p *proxy) _earlyGFN(ctx *smapModifier, si *meta.Snode, action string, joining bool) error {
	smap := p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		return newErrNotPrimary(p.si, smap, "cannot "+action+" "+si.StringEx())
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
		tokens    = p.authn.revokedTokenList()
		bmd       = p.owner.bmd.get()
		etlMD     = p.owner.etl.get()
		actMsgExt = p.newAmsg(ctx.msg, bmd)
		pairs     = make([]revsPair, 0, 5)
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

func (p *proxy) _syncFinal(ctx *smapModifier, clone *smapX) {
	var (
		actMsgExt = p.newAmsg(ctx.msg, nil)
		pairs     = make([]revsPair, 0, 2)
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
	if config != nil /*updated*/ {
		pairs = append(pairs, revsPair{config, actMsgExt})
	}

	wg := p.metasyncer.sync(pairs...)
	if ctx.rmdCtx != nil && ctx.rmdCtx.wait {
		wg.Wait()
	}
}

/////////////////////
// PUT /v1/cluster //
/////////////////////

// - cluster membership, including maintenance and decommission
// - rebalance
// - set-primary
// - cluster-wide configuration
// - start/stop xactions
// - logs...
func (p *proxy) httpcluput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathClu.L, 0, true)
	if err != nil {
		return
	}

	// admin access - all actions
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}

	if nlog.Stopping() {
		p.writeErr(w, r, p.errStopping(), http.StatusServiceUnavailable)
		return
	}
	if !p.NodeStarted() {
		p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s is not ready yet (starting up)", p)
		return
	}
	if len(apiItems) == 0 {
		p.cluputMsg(w, r)
	} else {
		p.cluputItems(w, r, apiItems)
	}
}

func (p *proxy) cluputMsg(w http.ResponseWriter, r *http.Request) {
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	// must be primary to execute all the rest actions
	if p.forwardCP(w, r, msg, "") {
		return
	}

	// not just 'cluster-started' - must be ready to rebalance as well
	// with two distinct exceptions
	withRR := (msg.Action != apc.ActShutdownCluster && msg.Action != apc.ActXactStop)
	if err := p.pready(nil, withRR); err != nil {
		p.writeErr(w, r, err, http.StatusServiceUnavailable)
		return
	}

	switch msg.Action {
	case apc.ActSetConfig:
		toUpdate := &cmn.ConfigToSet{}
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
	case apc.ActRotateLogs:
		p.rotateLogs(w, r, msg)

	case apc.ActShutdownCluster:
		args := allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		args.to = core.AllNodes
		_ = p.bcastGroup(args)
		freeBcArgs(args)

		// self
		p.termKalive(msg.Action)
		go func() {
			time.Sleep(cmn.Rom.CplaneOperation())
			p.shutdown(msg.Action)
		}()
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
		args.to = core.AllNodes
		_ = p.bcastGroup(args)
		freeBcArgs(args)

		// self
		p.termKalive(msg.Action)
		go func() {
			time.Sleep(cmn.Rom.CplaneOperation())
			p.decommission(msg.Action, &opts)
		}()
	case apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActShutdownNode, apc.ActRmNodeUnsafe:
		p.rmNode(w, r, msg)
	case apc.ActStopMaintenance:
		p.stopMaintenance(w, r, msg)

	case apc.ActResetStats:
		errorsOnly := msg.Value.(bool)
		p.statsT.ResetStats(errorsOnly)
		args := allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		args.to = core.AllNodes
		p.bcastAndRespond(w, r, args)
		freeBcArgs(args)

	case apc.ActClearLcache:
		if tid := msg.Name; tid != "" {
			err := cmn.NewErrNotImpl("drop in-memory metadata cache for a single node", tid) // TODO but can wait
			p.writeErr(w, r, err, http.StatusNotImplemented)
			return
		}
		args := allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
		args.to = core.Targets
		p.bcastAndRespond(w, r, args)
		freeBcArgs(args)

	case apc.ActXactStart:
		p.xstart(w, r, msg)
	case apc.ActXactStop:
		p.xstop(w, r, msg)

	case apc.ActReloadBackendCreds:
		if msg.Name != "" {
			normp := apc.NormalizeProvider(msg.Name)
			if !apc.IsCloudProvider(normp) {
				p.writeErrf(w, r, "cannot reload %q creds: not a Cloud provider", msg.Name)
				return
			}
			config := cmn.GCO.Get()
			if config.Backend.Get(normp) == nil {
				p.writeErr(w, r, &cmn.ErrMissingBackend{Provider: msg.Name})
				return
			}
			msg.Name = normp
		}
		p.reloadCreds(w, r, msg)

	// internal
	case apc.ActBumpMetasync:
		p.msyncForceAll(w, r, msg)

	// fail
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxy) setCluCfgPersistent(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToSet, msg *apc.ActMsg) {
	ctx := &configModifier{
		pre:      _setConfPre,
		final:    p._syncConfFinal,
		msg:      msg,
		toUpdate: toUpdate,
		wait:     true,
	}
	config := cmn.GCO.Get()
	// NOTE: critical cluster-wide config updates requiring restart (of the cluster)
	if toUpdate.Net != nil && toUpdate.Net.HTTP != nil {
		from, _ := jsoniter.Marshal(config.Net.HTTP)
		to, _ := jsoniter.Marshal(toUpdate.Net.HTTP)
		whingeToUpdate("net.http", string(from), string(to))

		// complementary
		if toUpdate.Net.HTTP.UseHTTPS != nil {
			use := *toUpdate.Net.HTTP.UseHTTPS
			if config.Net.HTTP.UseHTTPS != use {
				if toUpdate.Proxy == nil {
					toUpdate.Proxy = &cmn.ProxyConfToSet{}
				}
				switchHTTPS(toUpdate.Proxy, &config.Proxy, use)
			}
		}
	}
	if toUpdate.Auth != nil {
		from, _ := jsoniter.Marshal(config.Auth)
		to, _ := jsoniter.Marshal(toUpdate.Auth)
		whingeToUpdate("config.auth", string(from), string(to))
	}
	if toUpdate.Tracing != nil {
		from, _ := jsoniter.Marshal(config.Tracing)
		to, _ := jsoniter.Marshal(toUpdate.Tracing)
		whingeToUpdate("config.tracing", string(from), string(to))
	}

	// do
	if _, err := p.owner.config.modify(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

// switch http => https, or vice versa
func switchHTTPS(toCfg *cmn.ProxyConfToSet, fromCfg *cmn.ProxyConf, use bool) {
	toScheme, fromScheme := "http", "https"
	if use {
		toScheme, fromScheme = "https", "http"
	}
	f := func(to *string, from string) *string {
		if to == nil && strings.HasPrefix(from, fromScheme) {
			s := strings.Replace(from, fromScheme, toScheme, 1)
			to = apc.Ptr(s)
		}
		return to
	}
	toCfg.PrimaryURL = f(toCfg.PrimaryURL, fromCfg.PrimaryURL)
	toCfg.OriginalURL = f(toCfg.OriginalURL, fromCfg.OriginalURL)
	toCfg.DiscoveryURL = f(toCfg.DiscoveryURL, fromCfg.DiscoveryURL)

	nlog.Errorln("Warning: _prior_ to restart make sure to remove all copies of cluster maps")
}

func whingeToUpdate(what, from, to string) {
	nlog.Warningf("Updating cluster %s configuration: setting %s", what, to)
	nlog.Warningf("Prior-to-update %s values: %s", what, from)
	nlog.Errorln("Warning: this update MAY require cluster restart")
}

func (p *proxy) resetCluCfgPersistent(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	if err := p.owner.config.resetDaemonConfig(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	body := cos.MustMarshal(msg)

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: body}
	args.to = core.AllNodes
	p.bcastAndRespond(w, r, args)
	freeBcArgs(args)
}

func (p *proxy) rotateLogs(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	nlog.Flush(nlog.ActRotate)
	body := cos.MustMarshal(msg)
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: body}
	args.to = core.AllNodes
	p.bcastAndRespond(w, r, args)
	freeBcArgs(args)
}

func (p *proxy) setCluCfgTransient(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToSet, msg *apc.ActMsg) {
	co := p.owner.config
	co.Lock()
	err := setConfig(toUpdate, true /* transient */)
	co.Unlock()
	if err != nil {
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
	args.to = core.AllNodes
	p.bcastAndRespond(w, r, args)
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

// xstart: rebalance, resilver, other "startables" (see xaction/api.go)
func (p *proxy) xstart(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var xargs xact.ArgsMsg
	if msg.Value != nil {
		if err := cos.MorphMarshal(msg.Value, &xargs); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
	}
	xargs.Kind, _ = xact.GetKindName(xargs.Kind) // display name => kind

	// rebalance
	if xargs.Kind == apc.ActRebalance {
		if !xargs.Bck.IsEmpty() {
			// NOTE: limiting the scope of rebalance to a given bucket[/prefix] (advanced usage)
			b := (*meta.Bck)(&xargs.Bck)
			if _, present := p.owner.bmd.get().Get(b); !present {
				if b.IsRemote() {
					p.writeErr(w, r, cmn.NewErrRemoteBckNotFound(&xargs.Bck))
				} else {
					p.writeErr(w, r, cmn.NewErrBckNotFound(&xargs.Bck))
				}
				return
			}
		} else if msg.Name != "" {
			p.writeErrf(w, r, "invalid limited-scope %q: (n/a bucket, %q prefix)", apc.ActRebalance, msg.Name)
			return
		}
		p.rebalanceCluster(w, r, msg)
		return
	}

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathXactions.S}

	switch {
	case xargs.Kind == apc.ActBlobDl:
		// validate; select one target
		args.smap = p.owner.smap.get()
		tsi, err := p.blobdl(args.smap, &xargs, msg)
		if err != nil {
			freeBcArgs(args)
			p.writeErr(w, r, err)
			return
		}
		args._selected(tsi)
		args.req.Body = cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs, Name: msg.Name})
	case xargs.Kind == apc.ActResilver && xargs.DaemonID != "":
		args.smap = p.owner.smap.get()
		tsi := args.smap.GetTarget(xargs.DaemonID)
		if tsi == nil {
			err := &errNodeNotFound{p.si, args.smap, "cannot resilver", xargs.DaemonID}
			p.writeErr(w, r, err)
			return
		}
		args._selected(tsi)
		args.req.Body = cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs})
	default:
		// all targets, one common UUID for all
		args.to = core.Targets
		xargs.ID = cos.GenUUID()
		args.req.Body = cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs})
	}

	results := p.bcastGroup(args)
	freeBcArgs(args)

	for _, res := range results {
		if res.err == nil {
			if xargs.Kind == apc.ActResilver && xargs.DaemonID != "" {
				// - UUID assigned by the selected target (see above)
				// - not running notif listener for blob downloads - may reconsider
				xargs.ID = string(res.bytes)
			}
			continue
		}
		p.writeErr(w, r, res.toErr())
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	if xargs.ID != "" {
		smap := p.owner.smap.get()
		nl := xact.NewXactNL(xargs.ID, xargs.Kind, &smap.Smap, nil)
		p.ic.registerEqual(regIC{smap: smap, nl: nl})
		writeXid(w, xargs.ID)
	}
}

func (a *bcastArgs) _selected(tsi *meta.Snode) {
	nmap := make(meta.NodeMap, 1)
	nmap[tsi.ID()] = tsi
	a.nodes = []meta.NodeMap{nmap}
	a.to = core.SelectedNodes
}

func (p *proxy) blobdl(smap *smapX, xargs *xact.ArgsMsg, msg *apc.ActMsg) (tsi *meta.Snode, err error) {
	bck := meta.CloneBck(&xargs.Bck)
	if err := bck.Init(p.owner.bmd); err != nil {
		return nil, err
	}
	if err := cmn.ValidateRemoteBck(apc.ActBlobDl, &xargs.Bck); err != nil {
		return nil, err
	}
	objName := msg.Name
	tsi, _, err = smap.HrwMultiHome(xargs.Bck.MakeUname(objName))
	return tsi, err
}

func (p *proxy) xstop(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var xargs xact.ArgsMsg
	if err := cos.MorphMarshal(msg.Value, &xargs); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}

	xargs.Kind, _ = xact.GetKindName(xargs.Kind) // display name => kind

	// note: of all xaction kinds only rebalance can have a "valid rebalance ID" (see `cos.GenUUID`)
	// make an exception for rebalance: assign its kind to reinforce maintenance check below
	if xargs.Kind == "" && xact.IsValidRebID(xargs.ID) {
		xargs.Kind = apc.ActRebalance
	}

	// (lso + tco) special
	p.lstca.abort(&xargs)

	if xargs.Kind == apc.ActRebalance {
		// unless forced:
		// disallow aborting rebalance during
		// critical (meta.SnodeMaint => meta.SnodeMaintPostReb) and (meta.SnodeDecomm => removed) transitions
		if err := p._checkMaint(&xargs); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}

	body := cos.MustMarshal(apc.ActMsg{Action: msg.Action, Value: xargs})
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathXactions.S, Body: body}
	args.to = core.Targets
	results := p.bcastGroup(args)
	freeBcArgs(args)

	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			break
		}
	}
	freeBcastRes(results)
}

func (p *proxy) _checkMaint(xargs *xact.ArgsMsg) error {
	smap := p.owner.smap.get()
	for _, tsi := range smap.Tmap {
		switch {
		case tsi.Flags == 0:
			// do nothing
		case tsi.Flags.IsAnySet(meta.SnodeMaint) && !tsi.Flags.IsAnySet(meta.SnodeMaintPostReb):
			warn := "cluster is currently rebalancing while " + tsi.StringEx() + " transitions to maintenance mode"
			if !xargs.Force {
				return fmt.Errorf("cannot abort %s: %s", xargs.String(), warn)
			}
			nlog.Errorln("Warning:", warn, "- proceeding anyway")
		case tsi.Flags.IsAnySet(meta.SnodeDecomm):
			warn := "cluster is currently rebalancing while " + tsi.StringEx() + " is being decommissioned"
			if !xargs.Force {
				return fmt.Errorf("cannot abort %s: %s", xargs.String(), warn)
			}
			nlog.Errorln("Warning:", warn, "- proceeding anyway")
		}
	}
	return nil
}

func (p *proxy) reloadCreds(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: cos.MustMarshal(msg)}
	args.to = core.Targets
	results := p.bcastGroup(args)
	freeBcArgs(args)

	tag := "backend creds"
	if msg.Name != "" {
		tag = msg.Name + " " + tag
	}
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := res.errorf("node %s failed to reload %s (%q)", res.si, tag, msg)
		p.writeErr(w, r, err)
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)
	nlog.Infoln("reloaded", tag)
}

func (p *proxy) rebalanceCluster(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
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
		nlog.Warningf("%s: not enough active targets (%d) - proceeding to rebalance anyway", p, na)
	}
	rmdCtx := &rmdModifier{
		pre:     rmdInc,
		final:   rmdSync, // metasync new rmd instance
		p:       p,
		smapCtx: &smapModifier{smap: smap, msg: msg},
	}
	_, err := p.owner.rmd.modify(rmdCtx)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	writeXid(w, rmdCtx.rebID)
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
		err := cos.NewErrNotFound(p, "node "+opts.DaemonID)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	var inMaint bool
	if smap.InMaintOrDecomm(si.ID()) {
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
				nlog.Errorln("Warning: " + sname + " is currently being decommissioned")
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

	nlog.Infof("%s: %s(%s) opts=%v", p, msg.Action, si.StringEx(), opts)

	switch {
	case si.IsProxy():
		if _, err := p.mcastMaint(msg, si, false /*reb*/, false /*maintPostReb*/); err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err))
			return
		}
		ecode, err := p.rmNodeFinal(msg, si, nil)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err), ecode)
		}
	case msg.Action == apc.ActRmNodeUnsafe: // target unsafe
		if !opts.SkipRebalance {
			err := errors.New("unsafe must be unsafe")
			debug.AssertNoErr(err)
			p.writeErr(w, r, err)
			return
		}
		ecode, err := p.rmNodeFinal(msg, si, nil)
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, msg.Action, si, err), ecode)
		}
	default: // target
		reb := !opts.SkipRebalance && cmn.GCO.Get().Rebalance.Enabled && !inMaint
		nlog.Infof("%s: %s reb=%t", p, msg.Action, reb)
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
			writeXid(w, rebID)
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
			actMsgExt := p.newAmsgActVal(apc.ActStopGFN, nil)
			actMsgExt.UUID = si.ID()
			revs := revsPair{&smapX{Smap: meta.Smap{Version: ctx.nver}}, actMsgExt}
			_ = p.metasyncer.notify(false /*wait*/, revs) // async, failed-cnt always zero
		}
	}
	return
}

func (p *proxy) mcastMaint(msg *apc.ActMsg, si *meta.Snode, reb, maintPostReb bool) (*smapModifier, error) {
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
		err := fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActDecommissionNode, apc.ActStartMaintenance, apc.ActShutdownNode})
		return nil, err
	}

	var (
		dummy = meta.Snode{Flags: flags}
	)
	nlog.Infof("%s mcast-maint: %s, %s reb=(%t, %t), nflags=%s", p, msg, si.StringEx(), reb, maintPostReb, dummy.Fl2S())
	ctx := &smapModifier{
		pre:     p._markMaint,
		post:    p._rebPostRm, // (rmdCtx.rmNode => p.rmNodeFinal when all done)
		final:   p._syncFinal,
		sid:     si.ID(),
		flags:   flags,
		msg:     msg,
		skipReb: !reb,
	}
	if err := p._earlyGFN(ctx, si, msg.Action, false /*joining*/); err != nil {
		return nil, err
	}
	err := p.owner.smap.modify(ctx)
	debug.AssertNoErr(err)

	return ctx, err
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
	rmdCtx.listen(rmdCtx.postRm)
	ctx.rmdCtx = rmdCtx
}

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
	si := smap.GetNode(opts.DaemonID)
	if si == nil {
		err := cos.NewErrNotFound(p, "node "+opts.DaemonID)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}

	sname := si.StringEx()
	pname := p.String()
	nlog.Infoln(tag, pname, "[", msg.Action, sname, opts, "]")

	if !smap.InMaint(si) {
		p.writeErrf(w, r, "node %s is not in maintenance mode - nothing to do", si.StringEx())
		return
	}
	tout := cmn.Rom.CplaneOperation()
	if _, status, err := p.reqHealth(si, tout, nil, smap, false /*retry pub-addr*/); err != nil {
		// TODO: use cmn.KeepaliveRetryDuration()
		sleep, retries := tout/2, 4

		time.Sleep(sleep)
		for i := range retries { // retry
			time.Sleep(sleep)
			_, status, err = p.reqHealth(si, tout, nil, smap, true /*retry pub-addr*/)
			if err == nil {
				if i == 1 {
					nlog.Infoln(tag, pname, "=>", sname, "OK after 1 attempt [", msg.Action, opts, "]")
				} else {
					nlog.Infoln(tag, pname, "=>", sname, "OK after", i, "attempts [", msg.Action, opts, "]")
				}
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

	rebID, err := p.mcastStopMaint(msg, &opts)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if rebID != "" {
		writeXid(w, rebID)
	}
}

func (p *proxy) cluputItems(w http.ResponseWriter, r *http.Request, items []string) {
	action := items[0]
	if p.forwardCP(w, r, &apc.ActMsg{Action: action}, "") {
		return
	}
	switch action {
	case apc.Proxy:
		if err := p.pready(nil, true); err != nil {
			p.writeErr(w, r, err, http.StatusServiceUnavailable)
			return
		}
		// cluster-wide: designate a new primary proxy administratively
		p.cluSetPrimary(w, r)
	case apc.ActSetConfig: // set-config via query parameters and "?n1=v1&n2=v2..."
		if err := p.pready(nil, true); err != nil {
			p.writeErr(w, r, err, http.StatusServiceUnavailable)
			return
		}
		var (
			query    = r.URL.Query()
			toUpdate = &cmn.ConfigToSet{}
			msg      = &apc.ActMsg{Action: action}
		)
		if err := toUpdate.FillFromQuery(query); err != nil {
			p.writeErr(w, r, err)
			return
		}
		if transient := cos.IsParseBool(query.Get(apc.ActTransient)); transient {
			p.setCluCfgTransient(w, r, toUpdate, msg)
		} else {
			p.setCluCfgPersistent(w, r, toUpdate, msg)
		}
	case apc.ActAttachRemAis, apc.ActDetachRemAis:
		p.actRemAis(w, r, action, r.URL.Query())
	case apc.ActEnableBackend:
		p.actBackend(w, r, "enable", apc.URLPathDaeBendEnable, items)
	case apc.ActDisableBackend:
		p.actBackend(w, r, "disable", apc.URLPathDaeBendDisable, items)
	case apc.LoadX509:
		if len(items) < 2 {
			p.cluLoadX509(w, r)
		} else if sid := items[1]; sid == p.SID() {
			p.daeLoadX509(w, r)
		} else {
			smap := p.owner.smap.get()
			node := smap.GetNode(sid)
			if node == nil {
				err := &errNodeNotFound{p.si, smap, "X.509 load failure:", sid}
				p.writeErr(w, r, err, http.StatusNotFound)
				return
			}
			p.callLoadX509(w, r, node, smap)
		}
	}
}

func (p *proxy) actRemAis(w http.ResponseWriter, r *http.Request, action string, query url.Values) {
	what := query.Get(apc.QparamWhat)
	if what != apc.WhatRemoteAIS {
		p.writeErr(w, r, fmt.Errorf(fmtUnknownQue, what))
		return
	}
	if !p.ClusterStarted() {
		const fmerr = "(config-backends modifying) remote cluster: (%t, %s)"
		var timeout time.Duration
		for {
			time.Sleep(cmn.Rom.MaxKeepalive())
			timeout += cmn.Rom.MaxKeepalive()
			config := cmn.GCO.Get()
			if p.ClusterStarted() {
				break
			}
			if timeout > config.Timeout.Startup.D()/2 {
				p.writeErr(w, r, fmt.Errorf("%s: failed to attach "+fmerr, p, p.ClusterStarted(), config))
				return
			}
			nlog.Errorf("%s: waiting to attach "+fmerr, p, p.ClusterStarted(), config)
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

func (p *proxy) actBackend(w http.ResponseWriter, r *http.Request, tag string, upath apc.URLPath, items []string) {
	if len(items) < 2 {
		p.writeErrf(w, r, "invalid URL '%s': missing cloud backend", r.URL.Path)
		return
	}
	var (
		provider = items[1]
		np       = apc.NormalizeProvider(provider)
	)
	if !apc.IsCloudProvider(np) {
		p.writeErrf(w, r, "can only %s cloud backend (have %q)", tag, provider)
		return
	}
	// (two-phase commit)
	for _, phase := range []string{apc.ActBegin, apc.ActCommit} {
		var (
			path string
			args = allocBcArgs()
		)
		// bcast
		path = cos.JoinWords(upath.S, np, phase)
		args.req = cmn.HreqArgs{Method: http.MethodPut, Path: path}
		args.to = core.Targets
		results := p.bcastGroup(args)
		freeBcArgs(args)

		nlog.Infoln(phase+":", tag, provider)
		for _, res := range results {
			if res.err == nil {
				continue
			}
			err := res.errorf("node %s failed to %s %q backend (phase %s)", res.si, tag, provider, phase)
			p.writeErr(w, r, err)
			freeBcastRes(results)
			return
		}
		freeBcastRes(results)
	}

	nlog.Infoln("done:", tag, provider)
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
			nlog.Warningln(errmsg + " - proceeding anyway")
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
		nlog.Infof("%s: %s %s", p, action, detail)
		aisConf[alias] = []string{u}
	}
	config.Backend.Set(apc.AIS, aisConf)

	return true, nil
}

func (p *proxy) mcastStopMaint(msg *apc.ActMsg, opts *apc.ActValRmNode) (rebID string, err error) {
	nlog.Infof("%s mcast-stopm: %s, %s, skip-reb=%t", p, msg, opts.DaemonID, opts.SkipRebalance)
	ctx := &smapModifier{
		pre:     p._stopMaintPre,
		post:    p._stopMaintRMD,
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
	const efmt = "cannot take %s out of maintenance:"
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf(efmt, ctx.sid))
	}
	node := clone.GetNode(ctx.sid)
	if node == nil {
		ctx.status = http.StatusNotFound
		return &errNodeNotFound{p.si, clone, fmt.Sprintf(efmt, ctx.sid), ctx.sid}
	}
	clone.clearNodeFlags(ctx.sid, ctx.flags)
	if node.IsProxy() {
		clone.staffIC()
	}
	return nil
}

func (p *proxy) _stopMaintRMD(ctx *smapModifier, clone *smapX) {
	if ctx.skipReb {
		nlog.Infoln("ctx.skip-reb", ctx.skipReb)
		return
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		return
	}
	if nlog.Stopping() {
		return
	}
	if clone.CountActiveTs() < 2 {
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

//////////////////////////////////////////
// DELETE /v1/cluster - self-unregister //
//////////////////////////////////////////

func (p *proxy) httpcludel(w http.ResponseWriter, r *http.Request) {
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

	// primary (and cluster) to start and finalize rebalancing status _prior_ to removing invidual nodes
	if err := p.pready(smap, true); err != nil {
		p.writeErr(w, r, err, http.StatusServiceUnavailable)
		return
	}

	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	if err := p.checkIntraCall(r.Header, false /*from primary*/); err != nil {
		err = fmt.Errorf("%v (action %q)", err, apc.ActSelfRemove)
		p.writeErr(w, r, err)
		return
	}

	cid := r.Header.Get(apc.HdrCallerID)
	if cid != sid {
		err = fmt.Errorf("expecting %s by %s, got a wrong node ID (%s != %s)", apc.ActSelfRemove, node.StringEx(), cid, sid)
		p.writeErr(w, r, err)
		return
	}

	if ecode, err := p.mcastUnreg(&apc.ActMsg{Action: apc.ActSelfRemove}, node); err != nil {
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

// post-rebalance or post no-rebalance - last step removing a node
// (with msg.Action defining semantics)
func (p *proxy) rmNodeFinal(msg *apc.ActMsg, si *meta.Snode, ctx *smapModifier) (int, error) {
	var (
		smap    = p.owner.smap.get()
		node    = smap.GetNode(si.ID())
		timeout = cmn.Rom.CplaneOperation()
	)
	if node == nil {
		txt := "cannot \"" + msg.Action + "\""
		return http.StatusNotFound, &errNodeNotFound{p.si, smap, txt, si.ID()}
	}

	var (
		err   error
		ecode int
		cargs = allocCargs()
		body  = cos.MustMarshal(msg)
		sname = node.StringEx()
	)
	cargs.si, cargs.timeout = node, timeout
	switch msg.Action {
	case apc.ActShutdownNode, apc.ActRmNodeUnsafe, apc.ActStartMaintenance, apc.ActDecommissionNode:
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDae.S, Body: body}
	default:
		return 0, fmt.Errorf(fmtErrInvaldAction, msg.Action,
			[]string{apc.ActShutdownNode, apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActRmNodeUnsafe})
	}

	nlog.InfoDepth(1, p.String(), msg.Action, sname)
	res := p.call(cargs, smap)
	err = res.unwrap()
	freeCargs(cargs)
	freeCR(res)

	if err != nil {
		emsg := fmt.Sprintf("%s: (%s %s) final: %v - proceeding anyway...", p, msg, sname, err)
		switch msg.Action {
		case apc.ActShutdownNode, apc.ActDecommissionNode: // expecting EOF
			if !cos.IsEOF(err) {
				nlog.Errorln(emsg)
			}
		case apc.ActRmNodeUnsafe:
			if cmn.Rom.FastV(4, cos.SmoduleAIS) {
				nlog.Errorln(emsg)
			}
		default:
			nlog.Errorln(emsg)
		}
		err = nil // NOTE: proceeding anyway
	}

	switch msg.Action {
	case apc.ActDecommissionNode, apc.ActRmNodeUnsafe:
		ecode, err = p.mcastUnreg(msg, node)
	case apc.ActStartMaintenance, apc.ActShutdownNode:
		if ctx != nil && ctx.rmdCtx != nil && ctx.rmdCtx.rebID != "" {
			// final step executing shutdown and start-maintenance transaction:
			// setting si.Flags |= cluster.SnodeMaintPostReb
			// (compare w/ rmTarget --> p.mcastMaint above)
			_, err = p.mcastMaint(msg, node, false /*reb*/, true /*maintPostReb*/)
		}
	}
	if err != nil {
		nlog.Errorf("%s: (%s %s) FATAL: failed to update %s: %v", p, msg, sname, p.owner.smap.get(), err)
	}
	return ecode, err
}

func (p *proxy) mcastUnreg(msg *apc.ActMsg, si *meta.Snode) (ecode int, err error) {
	nlog.Infof("%s mcast-unreg: %s, %s", p, msg, si.StringEx())
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
		return &errNodeNotFound{p.si, clone, "failed to " + verb, sid}
	}
	if node.IsProxy() {
		clone.delProxy(sid)
		nlog.Infof("%s %s (num proxies %d)", verb, node.StringEx(), clone.CountProxies())
		clone.staffIC()
	} else {
		clone.delTarget(sid)
		nlog.Infof("%s %s (num targets %d)", verb, node.StringEx(), clone.CountTargets())
	}
	p.rproxy.nodes.Delete(ctx.sid)
	return nil
}

// rebalance's `can`: factors not including cluster map
func (p *proxy) canRebalance() (err error) {
	if nlog.Stopping() {
		return p.errStopping()
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
	if err = p.pready(smap, true); err != nil {
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
	if nlog.Stopping() {
		return false
	}
	prev := ctx.smap
	if prev.CountActiveTs() == 0 {
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
