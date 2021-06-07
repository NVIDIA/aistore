// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

////////////////////////////
// http /cluster handlers //
////////////////////////////

func (p *proxyrunner) clusterHandler(w http.ResponseWriter, r *http.Request) {
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

//////////////////////////////////////////////////////
// GET /v1/cluster - query cluster states and stats //
//////////////////////////////////////////////////////

func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
		what  = query.Get(cmn.URLParamWhat)
	)
	switch what {
	case cmn.GetWhatStats:
		p.queryClusterStats(w, r, what)
	case cmn.GetWhatSysInfo:
		p.queryClusterSysinfo(w, r, what)
	case cmn.GetWhatQueryXactStats:
		p.queryXaction(w, r, what)
	case cmn.GetWhatStatus:
		p.ic.writeStatus(w, r)
	case cmn.GetWhatMountpaths:
		p.queryClusterMountpaths(w, r, what)
	case cmn.GetWhatRemoteAIS:
		remoteAIS, err := p.getRemoteAISInfo()
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, remoteAIS, what)
	case cmn.GetWhatTargetIPs:
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
			buf.WriteString(si.PublicNet.NodeHostname)
			buf.WriteByte(',')
			buf.WriteString(si.IntraControlNet.NodeHostname)
			buf.WriteByte(',')
			buf.WriteString(si.IntraDataNet.NodeHostname)
		}
		w.Write(buf.Bytes())

	case cmn.GetWhatClusterConfig:
		config, err := p.owner.config.get()
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, config, what)
	case cmn.GetWhatBMD, cmn.GetWhatSmapVote, cmn.GetWhatSnode, cmn.GetWhatSmap:
		p.httprunner.httpdaeget(w, r)
	default:
		p.writeErrf(w, r, fmtUnknownQue, what)
	}
}

func (p *proxyrunner) queryXaction(w http.ResponseWriter, r *http.Request, what string) {
	var (
		body  []byte
		query = r.URL.Query()
	)
	switch what {
	case cmn.GetWhatQueryXactStats:
		var xactMsg xaction.XactReqMsg
		if err := cmn.ReadJSON(w, r, &xactMsg); err != nil {
			return
		}
		body = cos.MustMarshal(xactMsg)
	default:
		p.writeErrStatusf(w, r, http.StatusBadRequest, "invalid `what`: %v", what)
		return
	}
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodGet, Path: cmn.URLPathXactions.S, Body: body, Query: query}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	targetResults := p._queryResults(w, r, results)
	if targetResults != nil {
		p.writeJSON(w, r, targetResults, what)
	}
}

func (p *proxyrunner) queryClusterSysinfo(w http.ResponseWriter, r *http.Request, what string) {
	config := cmn.GCO.Get()
	timeout := config.Client.Timeout.D()
	proxyResults, err := p.cluSysinfo(r, timeout, cluster.Proxies)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	out := &cmn.ClusterSysInfoRaw{}
	out.Proxy = proxyResults
	targetResults, err := p.cluSysinfo(r, timeout, cluster.Targets)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	out.Target = targetResults
	_ = p.writeJSON(w, r, out, what)
}

func (p *proxyrunner) getRemoteAISInfo() (*cmn.BackendInfoAIS, error) {
	config := cmn.GCO.Get()
	smap := p.owner.smap.get()
	si, err := smap.GetRandTarget()
	if err != nil {
		return nil, err
	}
	remoteInfo := &cmn.BackendInfoAIS{}
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Path:   cmn.URLPathDaemon.S,
			Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatRemoteAIS}},
		},
		timeout: config.Timeout.CplaneOperation.D(),
		v:       remoteInfo,
	}
	res := p.call(args)
	err = res.error()
	_freeCallRes(res)
	if err != nil {
		return nil, err
	}
	return remoteInfo, nil
}

func (p *proxyrunner) cluSysinfo(r *http.Request, timeout time.Duration, to int) (cos.JSONRawMsgs, error) {
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: r.Method, Path: cmn.URLPathDaemon.S, Query: r.URL.Query()}
	args.timeout = timeout
	args.to = to
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	sysInfoMap := make(cos.JSONRawMsgs, len(results))
	for _, res := range results {
		if res.err != nil {
			err := res.error()
			freeCallResults(results)
			return nil, err
		}
		sysInfoMap[res.si.ID()] = res.bytes
	}
	freeCallResults(results)
	return sysInfoMap, nil
}

func (p *proxyrunner) queryClusterStats(w http.ResponseWriter, r *http.Request, what string) {
	targetStats := p._queryTargets(w, r)
	if targetStats == nil {
		return
	}
	out := &stats.ClusterStatsRaw{}
	out.Target = targetStats
	out.Proxy = p.statsT.CoreStats()
	_ = p.writeJSON(w, r, out, what)
}

func (p *proxyrunner) queryClusterMountpaths(w http.ResponseWriter, r *http.Request, what string) {
	targetMountpaths := p._queryTargets(w, r)
	if targetMountpaths == nil {
		return
	}
	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	_ = p.writeJSON(w, r, out, what)
}

// helper methods for querying targets

func (p *proxyrunner) _queryTargets(w http.ResponseWriter, r *http.Request) cos.JSONRawMsgs {
	var (
		err  error
		body []byte
	)
	if r.Body != nil {
		body, err = cmn.ReadBytes(r)
		if err != nil {
			p.writeErr(w, r, err)
			return nil
		}
	}
	config := cmn.GCO.Get()
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: r.Method, Path: cmn.URLPathDaemon.S, Query: r.URL.Query(), Body: body}
	args.timeout = config.Timeout.MaxKeepalive.D()
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	return p._queryResults(w, r, results)
}

func (p *proxyrunner) _queryResults(w http.ResponseWriter, r *http.Request, results sliceResults) cos.JSONRawMsgs {
	targetResults := make(cos.JSONRawMsgs, len(results))
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			p.writeErr(w, r, res.error())
			freeCallResults(results)
			return nil
		}
		targetResults[res.si.ID()] = res.bytes
	}
	freeCallResults(results)
	if len(targetResults) == 0 {
		p.writeErrMsg(w, r, "xaction not found", http.StatusNotFound)
		return nil
	}
	return targetResults
}

/////////////////////////////////////////////
// POST /v1/cluster - joins and keepalives //
/////////////////////////////////////////////

func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		regReq                                cluMeta
		tag                                   string
		keepalive, userRegister, selfRegister bool
		nonElectable                          bool
	)
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathCluster.L)
	if err != nil {
		return
	}
	if p.inPrimaryTransition.Load() {
		if apiItems[0] == cmn.Keepalive {
			// Ignore keepalive beat if the primary is in transition.
			return
		}
	}
	if p.forwardCP(w, r, nil, "httpclupost") {
		return
	}
	switch apiItems[0] {
	case cmn.UserRegister: // manual by user (API)
		if cmn.ReadJSON(w, r, &regReq.SI) != nil {
			return
		}
		tag, userRegister = "user-register", true
	case cmn.Keepalive:
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		tag, keepalive = "keepalive", true
	case cmn.AutoRegister: // node self-register
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		tag, selfRegister = "join", true
	default:
		p.writeErrURL(w, r)
		return
	}
	if selfRegister && !p.ClusterStarted() {
		p.reg.mtx.Lock()
		p.reg.pool = append(p.reg.pool, regReq)
		p.reg.mtx.Unlock()
	}
	// NOTE: The node ID is obtained from the node itself, API calls are not trusted.
	//  Any aisnode will either detect or generate an ID for itself during startup.
	//  For more, see `initDaemonID` in `ais/daemon.go`.
	nsi := regReq.SI
	if userRegister {
		si, err := p.getDaemonInfo(nsi)
		if err != nil {
			p.writeErrf(w, r, "failed to obtain daemon info from %q, err: %v",
				nsi.URL(cmn.NetworkPublic), err)
			return
		}
		nsi.DaemonID = si.DaemonID
	}
	if err := nsi.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if p.NodeStarted() {
		bmd := p.owner.bmd.get()
		if err := bmd.validateUUID(regReq.BMD, p.si, nsi, ""); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	var (
		isProxy = nsi.IsProxy()
		msg     = &cmn.ActionMsg{Action: cmn.ActRegTarget}
	)
	if isProxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}

		s := r.URL.Query().Get(cmn.URLParamNonElectable)
		if nonElectable, err = cos.ParseBool(s); err != nil {
			glog.Errorf("%s: failed to parse %s for non-electability: %v", p.si, s, err)
		}
	}
	if err := validateHostname(nsi.PublicNet.NodeHostname); err != nil {
		p.writeErrf(w, r, "%s: failed to %s %s - (err: %v)", p.si, tag, nsi, err)
		return
	}

	smap := p.owner.smap.get()
	if osi := smap.GetNode(nsi.ID()); osi != nil {
		nsi.Flags = osi.Flags
	}
	if nonElectable {
		nsi.Flags = nsi.Flags.Set(cluster.SnodeNonElectable)
	}

	if userRegister {
		if errCode, err := p.userRegisterNode(nsi, tag); err != nil {
			p.writeErr(w, r, err, errCode)
			return
		}
	}
	if selfRegister {
		if osi := smap.GetNode(nsi.ID()); osi != nil && !osi.Equals(nsi) {
			if duplicate, err := p.detectDaemonDuplicate(osi, nsi); err != nil {
				p.writeErrf(w, r, "failed to obtain daemon info, err: %v", err)
				return
			} else if duplicate {
				p.writeErrf(w, r, "duplicate node ID %q (%s, %s)", nsi.ID(), osi, nsi)
				return
			}
			glog.Warningf("%s: self-registering %s with duplicate node ID %q", p.si, nsi, nsi.ID())
		}
	}

	p.owner.smap.Lock()
	update, err := p.handleJoinKalive(nsi, regReq.Smap, tag, keepalive, nsi.Flags)
	if !isProxy && p.NodeStarted() {
		if p.owner.rmd.rebalance.CAS(false, regReq.RebInterrupted) && regReq.RebInterrupted {
			glog.Errorf("%s: target %s reports interrupted rebalance", p.si, nsi)
		}
	}
	p.owner.smap.Unlock()

	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	if !update {
		return
	}

	// send clu-meta to the self-registering node
	if selfRegister {
		glog.Infof("%s: %s %s (%s)...", p.si, tag, nsi, regReq.Smap)
		meta, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, meta, path.Join(msg.Action, nsi.ID()) /* tag */)
	} else if userRegister {
		// Return the node ID and rebalance ID when the node is joined by user.
		rebID, err := p.updateAndDistribute(nsi, msg, nsi.Flags)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.writeJSON(w, r, api.JoinNodeResult{DaemonID: nsi.Name(), RebalanceID: rebID}, "")
		return
	}
	go p.updateAndDistribute(nsi, msg, nsi.Flags)
}

// join(node) => cluster via API
func (p *proxyrunner) userRegisterNode(nsi *cluster.Snode, tag string) (errCode int, err error) {
	meta, err := p.cluMeta(cmetaFillOpt{skipSmap: true})
	if err != nil {
		return http.StatusInternalServerError, err
	}
	body := cos.MustMarshal(meta)
	if glog.FastV(3, glog.SmoduleAIS) {
		glog.Infof("%s: %s %s => (%s)", p.si, tag, nsi, p.owner.smap.get().StringEx())
	}
	config := cmn.GCO.Get()
	args := callArgs{
		si:      nsi,
		req:     cmn.ReqArgs{Method: http.MethodPost, Path: cmn.URLPathDaemonUserReg.S, Body: body},
		timeout: config.Timeout.CplaneOperation.D(),
	}
	res := p.call(args)
	defer _freeCallRes(res)
	if res.err == nil {
		return
	}
	if cos.IsErrConnectionRefused(res.err) {
		err = fmt.Errorf("failed to reach %s at %s:%s",
			nsi, nsi.PublicNet.NodeHostname, nsi.PublicNet.DaemonPort)
	} else {
		err = res.errorf("%s: failed to %s %s", p.si, tag, nsi)
	}
	errCode = res.status
	return
}

// NOTE: under lock
func (p *proxyrunner) handleJoinKalive(nsi *cluster.Snode, regSmap *smapX, tag string,
	keepalive bool, flags cluster.SnodeFlags) (update bool, err error) {
	smap := p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		err = newErrNotPrimary(p.si, smap, "cannot "+tag+" "+nsi.String())
		return
	}
	if nsi.IsProxy() {
		osi := smap.GetProxy(nsi.ID())
		if !p.addOrUpdateNode(nsi, osi, keepalive) {
			return
		}
	} else {
		osi := smap.GetTarget(nsi.ID())
		if keepalive && !p.addOrUpdateNode(nsi, osi, keepalive) {
			return
		}
	}
	// check for cluster integrity errors (cie)
	if err = smap.validateUUID(p.si, regSmap, nsi.String(), 80 /* ciError */); err != nil {
		return
	}
	// no further checks join when cluster's starting up
	if !p.ClusterStarted() {
		clone := smap.clone()
		clone.putNode(nsi, flags)
		p.owner.smap.put(clone)
		return
	}
	if _, err = smap.IsDuplicate(nsi); err != nil {
		err = errors.New(p.si.String() + ": " + err.Error())
	}
	update = err == nil
	return
}

func (p *proxyrunner) updateAndDistribute(nsi *cluster.Snode, msg *cmn.ActionMsg,
	flags cluster.SnodeFlags) (xactID string, err error) {
	ctx := &smapModifier{
		pre:   p._updPre,
		post:  p._updPost,
		final: p._updFinal,
		nsi:   nsi,
		msg:   msg,
		flags: flags,
	}
	err = p.owner.smap.modify(ctx)
	if err != nil {
		glog.Errorf("FATAL: %v", err)
		return
	}
	if ctx.rmd != nil {
		xactID = xaction.RebID(ctx.rmd.Version).String()
	}
	return
}

func (p *proxyrunner) _updPre(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot add %s", ctx.nsi))
	}
	ctx.exists = clone.putNode(ctx.nsi, ctx.flags)
	if ctx.nsi.IsTarget() {
		// Notify targets that they need to set up GFN
		aisMsg := p.newAmsgActVal(cmn.ActStartGFN, nil)
		notifyPairs := revsPair{clone, aisMsg}
		_ = p.metasyncer.notify(true, notifyPairs)
	}
	clone.staffIC()
	return nil
}

func (p *proxyrunner) _updPost(ctx *smapModifier, clone *smapX) {
	if !ctx.nsi.IsTarget() {
		// RMD is sent upon proxy joining to provide for its (RMD's) replication -
		// done under Smap lock to serialize with respect to new joins.
		ctx.rmd = p.owner.rmd.get()
		return
	}
	if err := p.canStartRebalance(); err != nil {
		glog.Warning(err)
		return
	}
	// NOTE: trigger rebalance when target with the same ID already exists
	//       (and see p.requiresRebalance for other conditions)
	if ctx.exists || p.requiresRebalance(ctx.smap, clone) {
		rmdCtx := &rmdModifier{
			pre: func(_ *rmdModifier, clone *rebMD) {
				clone.TargetIDs = []string{ctx.nsi.ID()}
				clone.inc()
			},
		}
		rmdClone, err := p.owner.rmd.modify(rmdCtx)
		if err != nil {
			glog.Error(err)
			debug.AssertNoErr(err)
		} else {
			ctx.rmd = rmdClone
		}
	}
}

func (p *proxyrunner) _updFinal(ctx *smapModifier, clone *smapX) {
	var (
		tokens = p.authn.revokedTokenList()
		bmd    = p.owner.bmd.get()
		aisMsg = p.newAmsg(ctx.msg, bmd)
		pairs  = make([]revsPair, 0, 5)
	)
	if config, err := p.owner.config.get(); err != nil {
		glog.Error(err)
	} else if config != nil {
		pairs = append(pairs, revsPair{config, aisMsg})
	}
	pairs = append(pairs, revsPair{clone, aisMsg}, revsPair{bmd, aisMsg})
	if ctx.rmd != nil && ctx.nsi.IsTarget() {
		pairs = append(pairs, revsPair{ctx.rmd, aisMsg})
		nl := xaction.NewXactNL(xaction.RebID(ctx.rmd.version()).String(),
			cmn.ActRebalance, &clone.Smap, nil)
		nl.SetOwner(equalIC)
		// Rely on metasync to register rebalanace/resilver `nl` on all IC members.  See `p.receiveRMD`.
		err := p.notifs.add(nl)
		debug.AssertNoErr(err)
	} else if ctx.nsi.IsProxy() {
		// Send RMD to proxies to make sure that they have
		// the latest one - newly joined can become primary in a second.
		cos.Assert(ctx.rmd != nil)
		pairs = append(pairs, revsPair{ctx.rmd, aisMsg})
	}
	if len(tokens.Tokens) > 0 {
		pairs = append(pairs, revsPair{tokens, aisMsg})
	}
	debug.Assert(clone._sgl != nil)
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}

func (p *proxyrunner) addOrUpdateNode(nsi, osi *cluster.Snode, keepalive bool) bool {
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s: adding back to the cluster map", nsi)
			return true
		}
		if !osi.Equals(nsi) {
			if duplicate, err := p.detectDaemonDuplicate(osi, nsi); err != nil {
				glog.Errorf("%s: %s(%s) failed to obtain daemon info, err: %v",
					p.si, nsi, nsi.PublicNet.DirectURL, err)
				return false
			} else if duplicate {
				glog.Errorf("%s: %s(%s) is trying to register/keepalive with duplicate ID",
					p.si, nsi, nsi.PublicNet.DirectURL)
				return false
			}
			glog.Warningf("%s: renewing registration %s (info changed!)", p.si, nsi)
			return true
		}
		p.keepalive.heardFrom(nsi.ID(), false /*reset*/)
		return false
	}
	if osi != nil {
		if !p.NodeStarted() {
			return true
		}
		if osi.Equals(nsi) {
			glog.Infof("%s: %s is already registered", p.si, nsi)
			return false
		}
		glog.Warningf("%s: renewing %s registration %+v => %+v", p.si, nsi, osi, nsi)
	}
	return true
}

func (p *proxyrunner) _perfRebPost(ctx *smapModifier, clone *smapX) {
	if ctx.skipReb {
		return
	}
	if err := p.canStartRebalance(); err != nil {
		glog.Warning(err)
		return
	}
	if p.requiresRebalance(ctx.smap, clone) {
		rmdCtx := &rmdModifier{
			pre: func(_ *rmdModifier, clone *rebMD) {
				clone.inc()
			},
		}
		rmdClone, err := p.owner.rmd.modify(rmdCtx)
		if err != nil {
			glog.Error(err)
			debug.AssertNoErr(err)
		} else {
			ctx.rmd = rmdClone
		}
	}
}

// NOTE: when the change involves target node always wait for metasync to distribute updated Smap
func (p *proxyrunner) _syncFinal(ctx *smapModifier, clone *smapX) {
	var (
		aisMsg = p.newAmsg(ctx.msg, nil)
		pairs  = make([]revsPair, 0, 2)
	)
	pairs = append(pairs, revsPair{clone, aisMsg})
	if ctx.rmd != nil {
		nl := xaction.NewXactNL(xaction.RebID(ctx.rmd.version()).String(),
			cmn.ActRebalance, &clone.Smap, nil)
		nl.SetOwner(equalIC)
		// Rely on metasync to register rebalanace/resilver `nl` on all IC members.  See `p.receiveRMD`.
		err := p.notifs.add(nl)
		debug.AssertNoErr(err)
		pairs = append(pairs, revsPair{ctx.rmd, aisMsg})
	}
	debug.Assert(clone._sgl != nil)
	wg := p.metasyncer.sync(pairs...)
	if ctx.isTarget {
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
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.URLPathCluster.L)
	if err != nil {
		return
	}
	if err := p.checkACL(r.Header, nil, cmn.AccessAdmin); err != nil {
		p.writeErr(w, r, err, http.StatusUnauthorized)
		return
	}
	if len(apiItems) == 0 {
		p.cluputJSON(w, r)
	} else {
		p.cluputQuery(w, r, apiItems[0])
	}
}

func (p *proxyrunner) cluputJSON(w http.ResponseWriter, r *http.Request) {
	msg := &cmn.ActionMsg{}
	if cmn.ReadJSON(w, r, msg) != nil {
		return
	}
	if msg.Action != cmn.ActSendOwnershipTbl {
		if p.forwardCP(w, r, msg, "") {
			return
		}
	}
	// handle the action
	switch msg.Action {
	case cmn.ActSetConfig:
		toUpdate := &cmn.ConfigToUpdate{}
		if err := cos.MorphMarshal(msg.Value, toUpdate); err != nil {
			p.writeErrf(w, r, "%s: failed to parse value, err: %v", cmn.ActSetConfig, err)
			return
		}
		p.setClusterConfig(w, r, toUpdate, msg)
	case cmn.ActResetConfig:
		p.resetClusterConfig(w, r, msg)
	case cmn.ActShutdown, cmn.ActDecommission:
		glog.Infoln("Proxy-controlled cluster decommission/shutdown...")
		args := allocBcastArgs()
		args.req = cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathDaemon.S, Body: cos.MustMarshal(msg)}
		args.to = cluster.AllNodes
		_ = p.bcastGroup(args)
		freeBcastArgs(args)
		p.unreg(msg.Action)
	case cmn.ActXactStart, cmn.ActXactStop:
		p.xactStarStop(w, r, msg)
	case cmn.ActSendOwnershipTbl:
		p.sendOwnTbl(w, r, msg)
	case cmn.ActStartMaintenance, cmn.ActDecommissionNode, cmn.ActShutdownNode:
		p.rmNode(w, r, msg)
	case cmn.ActStopMaintenance:
		p.stopMaintenance(w, r, msg)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxyrunner) setClusterConfig(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToUpdate, msg *cmn.ActionMsg) {
	transient := cos.IsParseBool(r.URL.Query().Get(cmn.ActTransient))
	if transient {
		p.setTransientClusterConfig(w, r, toUpdate, msg)
		return
	}
	ctx := &configModifier{
		pre:      p._setConfPre,
		final:    p._syncConfFinal,
		msg:      msg,
		toUpdate: toUpdate,
		wait:     true,
	}
	if _, err := p.owner.config.modify(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxyrunner) resetClusterConfig(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	if err := p.owner.config.resetDaemonConfig(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	body := cos.MustMarshal(msg)
	req := cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathDaemon.S, Body: body}
	p.bcastReqGroup(w, r, req, cluster.AllNodes)
}

func (p *proxyrunner) setTransientClusterConfig(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToUpdate, msg *cmn.ActionMsg) {
	if err := p.owner.config.setDaemonConfig(toUpdate, true /* transient */); err != nil {
		p.writeErr(w, r, err)
		return
	}
	q := url.Values{}
	q.Add(cmn.ActTransient, "true")

	msg.Value = toUpdate
	body := cos.MustMarshal(msg)
	req := cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathDaemon.S, Body: body, Query: q}
	p.bcastReqGroup(w, r, req, cluster.AllNodes)
}

func (p *proxyrunner) _setConfPre(ctx *configModifier, clone *globalConfig) (updated bool, err error) {
	if err = clone.Apply(*ctx.toUpdate, cmn.Cluster); err != nil {
		return
	}
	updated = true
	return
}

func (p *proxyrunner) _syncConfFinal(ctx *configModifier, clone *globalConfig) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAmsg(ctx.msg, nil)})
	if ctx.wait {
		wg.Wait()
	}
}

func (p *proxyrunner) xactStarStop(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	xactMsg := xaction.XactReqMsg{}
	if err := cos.MorphMarshal(msg.Value, &xactMsg); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if msg.Action == cmn.ActXactStart {
		if xactMsg.Kind == cmn.ActRebalance {
			p.rebalanceCluster(w, r)
			return
		}

		xactMsg.ID = cos.GenUUID() // all other xact starts need an id
		if xactMsg.Kind == cmn.ActResilver && xactMsg.Node != "" {
			p.resilverOne(w, r, msg, xactMsg)
			return
		}
	}

	body := cos.MustMarshal(cmn.ActionMsg{Action: msg.Action, Value: xactMsg})
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathXactions.S, Body: body}
	args.to = cluster.Targets
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.error())
		freeCallResults(results)
		return
	}
	freeCallResults(results)
	if msg.Action == cmn.ActXactStart {
		smap := p.owner.smap.get()
		nl := xaction.NewXactNL(xactMsg.ID, xactMsg.Kind, &smap.Smap, nil)
		p.ic.registerEqual(regIC{smap: smap, nl: nl})
		w.Write([]byte(xactMsg.ID))
	}
}

func (p *proxyrunner) rebalanceCluster(w http.ResponseWriter, r *http.Request) {
	if err := p.canStartRebalance(); err != nil && err != errRebalanceDisabled {
		p.writeErr(w, r, err)
		return
	}
	rmdCtx := &rmdModifier{
		pre:   func(_ *rmdModifier, clone *rebMD) { clone.inc() },
		final: p._syncRMDFinal,
		msg:   &cmn.ActionMsg{Action: cmn.ActRebalance},
		smap:  p.owner.smap.get(),
	}
	rmdClone, err := p.owner.rmd.modify(rmdCtx)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	w.Write([]byte(xaction.RebID(rmdClone.version()).String()))
}

func (p *proxyrunner) resilverOne(w http.ResponseWriter, r *http.Request,
	msg *cmn.ActionMsg, xactMsg xaction.XactReqMsg) {
	smap := p.owner.smap.get()
	si := smap.GetTarget(xactMsg.Node)
	if si == nil {
		p.writeErrf(w, r, "cannot resilver %v: node must exist and be a target", si)
		return
	}

	body := cos.MustMarshal(cmn.ActionMsg{Action: msg.Action, Value: xactMsg})
	res := p.call(
		callArgs{
			si: si,
			req: cmn.ReqArgs{
				Method: http.MethodPut,
				Path:   cmn.URLPathXactions.S,
				Body:   body,
			},
		},
	)
	if res.err != nil {
		p.writeErr(w, r, res.error())
		return
	}

	nl := xaction.NewXactNL(xactMsg.ID, xactMsg.Kind, &smap.Smap, nil)
	p.ic.registerEqual(regIC{smap: smap, nl: nl})
	w.Write([]byte(xactMsg.ID))
}

func (p *proxyrunner) sendOwnTbl(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	var (
		smap  = p.owner.smap.get()
		dstID string
	)
	if err := cos.MorphMarshal(msg.Value, &dstID); err != nil {
		p.writeErr(w, r, err)
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
	args := callArgs{
		req:     cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathCluster.S, Body: cos.MustMarshal(msg)},
		timeout: cmn.DefaultTimeout,
	}
	for pid, psi := range smap.Pmap {
		if !smap.IsIC(psi) || pid == dstID {
			continue
		}
		args.si = psi
		res := p.call(args)
		if res.err != nil {
			p.writeErr(w, r, res.error())
		}
		_freeCallRes(res)
		break
	}
}

// gracefully remove node via cmn.ActStartMaintenance, cmn.ActDecommission, cmn.ActShutdownNode
// TODO: support forceful (--force) removal
func (p *proxyrunner) rmNode(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	var (
		opts cmn.ActValRmNode
		smap = p.owner.smap.get()
	)
	if err := p.checkACL(r.Header, nil, cmn.AccessAdmin); err != nil {
		p.writeErr(w, r, err, http.StatusUnauthorized)
		return
	}
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErr(w, r, err)
		return
	}
	si := smap.GetNode(opts.DaemonID)
	if si == nil {
		err := cmn.NewNotFoundError("node %q", opts.DaemonID)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if smap.PresentInMaint(si) {
		p.writeErrf(w, r, "node %q is already in maintenance", opts.DaemonID)
		return
	}
	if p.si.Equals(si) {
		p.writeErrf(w, r, "node %q is primary, cannot perform %q", opts.DaemonID, msg.Action)
		return
	}
	// proxy
	if si.IsProxy() {
		if err := p.markMaintenance(msg, si); err != nil {
			p.writeErrf(w, r, cmn.FmtErrFailed, p.si, msg.Action, si, err)
			return
		}
		if msg.Action == cmn.ActDecommissionNode || msg.Action == cmn.ActShutdownNode {
			errCode, err := p.callRmSelf(msg, si, true /*skipReb*/)
			if err != nil {
				p.writeErrStatusf(w, r, errCode, cmn.FmtErrFailed, p.si, msg.Action, si, err)
			}
		}
		return
	}
	// target
	rebID, err := p.startMaintenance(si, msg, &opts)
	if err != nil {
		p.writeErrf(w, r, cmn.FmtErrFailed, p.si, msg.Action, si, err)
		return
	}
	if rebID != 0 {
		w.Write([]byte(rebID.String()))
	}
}

func (p *proxyrunner) stopMaintenance(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	var (
		opts cmn.ActValRmNode
		smap = p.owner.smap.get()
	)
	if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
		p.writeErr(w, r, err)
		return
	}
	si := smap.GetNode(opts.DaemonID)
	if si == nil {
		err := cmn.NewNotFoundError("node %q", opts.DaemonID)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if !smap.PresentInMaint(si) {
		p.writeErrf(w, r, "node %q is not under maintenance", opts.DaemonID)
		return
	}
	rebID, err := p.cancelMaintenance(msg, si, &opts)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if rebID != 0 {
		w.Write([]byte(rebID.String()))
	}
}

func (p *proxyrunner) cluputQuery(w http.ResponseWriter, r *http.Request, action string) {
	query := r.URL.Query()
	if p.forwardCP(w, r, &cmn.ActionMsg{Action: action}, "") {
		return
	}
	switch action {
	case cmn.Proxy:
		// cluster-wide: designate a new primary proxy administratively
		p.cluSetPrimary(w, r)
	case cmn.ActSetConfig: // setconfig via query parameters and "?n1=v1&n2=v2..."
		toUpdate := &cmn.ConfigToUpdate{}
		if err := toUpdate.FillFromQuery(query); err != nil {
			p.writeErrf(w, r, err.Error())
			return
		}
		p.setClusterConfig(w, r, toUpdate, &cmn.ActionMsg{Action: action})
	case cmn.ActAttach, cmn.ActDetach:
		if err := p.attachDetach(w, r, action); err != nil {
			return
		}
	}
}

func (p *proxyrunner) attachDetach(w http.ResponseWriter, r *http.Request, action string) (err error) {
	// TODO: Implement `ActAttach` as transactions begin -- update locally -- metasync -- commit
	var (
		query = r.URL.Query()
		what  = query.Get(cmn.URLParamWhat)
	)
	if what != cmn.GetWhatRemoteAIS {
		err = fmt.Errorf(fmtUnknownQue, what)
		p.writeErr(w, r, err)
		return
	}

	ctx := &configModifier{
		pre:   p.attachDetachRemoteAIS,
		final: p._syncConfFinal,
		msg:   &cmn.ActionMsg{Action: action},
		query: query,
		wait:  true,
	}

	if _, err = p.owner.config.modify(ctx); err != nil {
		p.writeErr(w, r, err)
		return
	}
	return
}

func (p *proxyrunner) attachDetachRemoteAIS(ctx *configModifier, config *globalConfig) (changed bool, err error) {
	var (
		aisConf cmn.BackendConfAIS
		errMsg  string
		action  = ctx.msg.Action
		query   = ctx.query
		v, ok   = config.Backend.ProviderConf(cmn.ProviderAIS)
	)
	if !ok || v == nil {
		if action == cmn.ActDetach {
			err = fmt.Errorf("%s: remote cluster config is empty", p.si)
			return
		}
		aisConf = make(cmn.BackendConfAIS)
	} else {
		aisConf = cmn.BackendConfAIS{}
		cos.MustMorphMarshal(v, &aisConf)
	}
	// detach
	if action == cmn.ActDetach {
		for alias := range query {
			if alias == cmn.URLParamWhat {
				continue
			}
			if _, ok := aisConf[alias]; ok {
				changed = true
				delete(aisConf, alias)
			}
		}
		if !changed {
			errMsg = "remote cluster does not exist"
		}
		goto rret
	}
	// attach
	for alias, urls := range query {
		if alias == cmn.URLParamWhat {
			continue
		}
		for _, u := range urls {
			if _, err := url.ParseRequestURI(u); err != nil {
				return false, fmt.Errorf("%s: cannot attach remote cluster: %v", p.si, err)
			}
			changed = true
		}
		if changed {
			if confURLs, ok := aisConf[alias]; ok {
				aisConf[alias] = append(confURLs, urls...)
			} else {
				aisConf[alias] = urls
			}
		}
	}
	if !changed {
		errMsg = "empty URL list"
	}
rret:
	if errMsg != "" {
		return false, fmt.Errorf("%s: %s remote cluster: %s", p.si, action, errMsg)
	}
	config.Backend.ProviderConf(cmn.ProviderAIS, aisConf)
	return
}

// Callback: remove the node from the cluster if rebalance finished successfully
func (p *proxyrunner) removeAfterRebalance(nl nl.NotifListener, msg *cmn.ActionMsg, si *cluster.Snode) {
	if err := nl.Err(false); err != nil || nl.Aborted() {
		glog.Errorf("Rebalance(%s) didn't finish successfully, err: %v, aborted: %v",
			nl.UUID(), err, nl.Aborted())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Rebalance(%s) finished. Removing node %s", nl.UUID(), si)
	}
	if _, err := p.callRmSelf(msg, si, true /*skipReb*/); err != nil {
		glog.Errorf("Failed to remove node (%s) after rebalance, err: %v", si, err)
	}
}

// Run rebalance if needed; remove self from the cluster when rebalance finishes
// the method handles msg.Action == cmn.ActStartMaintenance | cmn.ActDecommission | cmn.ActShutdownNode
func (p *proxyrunner) rebalanceAndRmSelf(msg *cmn.ActionMsg, si *cluster.Snode) (rebID xaction.RebID, err error) {
	smap := p.owner.smap.get()
	if smap.CountActiveTargets() < 2 {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%q: removing the last target %s - no rebalance", msg.Action, si)
		}
		_, err = p.callRmSelf(msg, si, true /*skipReb*/)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%q %s and start rebalance", msg.Action, si)
	}
	var cb nl.NotifCallback
	if msg.Action == cmn.ActDecommissionNode || msg.Action == cmn.ActShutdownNode {
		cb = func(nl nl.NotifListener) { p.removeAfterRebalance(nl, msg, si) }
	}
	rmdCtx := &rmdModifier{
		pre: func(_ *rmdModifier, clone *rebMD) {
			clone.inc()
		},
		final: p._syncRMDFinal,
		smap:  p.owner.smap.get(),
		msg:   msg,
		rebCB: cb,
		wait:  true,
	}
	rmdClone, err := p.owner.rmd.modify(rmdCtx)
	if err != nil {
		glog.Error(err)
		debug.AssertNoErr(err)
	} else {
		rebID = xaction.RebID(rmdClone.version())
	}
	return
}

// Stops rebalance if needed, do cleanup, and get the node back to the cluster.
func (p *proxyrunner) cancelMaintenance(msg *cmn.ActionMsg, si *cluster.Snode, opts *cmn.ActValRmNode) (rebID xaction.RebID,
	err error) {
	ctx := &smapModifier{
		pre:      p._cancelMaintPre,
		post:     p._perfRebPost,
		final:    p._syncFinal,
		sid:      opts.DaemonID,
		skipReb:  opts.SkipRebalance,
		msg:      msg,
		flags:    cluster.SnodeMaintenanceMask,
		isTarget: si.IsTarget(),
	}
	err = p.owner.smap.modify(ctx)
	if ctx.rmd != nil {
		rebID = xaction.RebID(ctx.rmd.Version)
	}
	return
}

func (p *proxyrunner) _cancelMaintPre(ctx *smapModifier, clone *smapX) error {
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone,
			fmt.Sprintf("cannot cancel maintenance for %s", ctx.sid))
	}
	clone.clearNodeFlags(ctx.sid, ctx.flags)
	clone.staffIC()
	return nil
}

func (p *proxyrunner) _syncRMDFinal(ctx *rmdModifier, clone *rebMD) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAmsg(ctx.msg, nil)})
	nl := xaction.NewXactNL(xaction.RebID(clone.Version).String(),
		cmn.ActRebalance, &ctx.smap.Smap, nil)
	nl.SetOwner(equalIC)
	if ctx.rebCB != nil {
		nl.F = ctx.rebCB
	}
	// Rely on metasync to register rebalance/resilver `nl` on all IC members. See `p.receiveRMD`.
	err := p.notifs.add(nl)
	cos.AssertNoErr(err)
	if ctx.wait {
		wg.Wait()
	}
}

func (p *proxyrunner) _syncBMDFinal(ctx *bmdModifier, clone *bucketMD) {
	debug.Assert(clone._sgl != nil)
	msg := p.newAmsg(ctx.msg, clone, ctx.txnID)
	wg := p.metasyncer.sync(revsPair{clone, msg})
	if ctx.wait {
		wg.Wait()
	}
}

func (p *proxyrunner) cluSetPrimary(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathClusterProxy.L)
	if err != nil {
		return
	}
	proxyid := apiItems[0]
	s := "designate new primary proxy '" + proxyid + "'"
	if p.forwardCP(w, r, nil, s) {
		return
	}
	smap := p.owner.smap.get()
	psi := smap.GetProxy(proxyid)
	if psi == nil {
		p.writeErrf(w, r, "new primary proxy %s is not present in the %s", proxyid, smap.StringEx())
		return
	}
	if proxyid == p.si.ID() {
		cos.Assert(p.si.ID() == smap.Primary.ID()) // must be forwardCP-ed
		glog.Warningf("Request to set primary to %s(self) - nothing to do", proxyid)
		return
	}
	if smap.PresentInMaint(psi) {
		p.writeErrf(w, r, "cannot set new primary: %s is under maintenance", psi)
		return
	}
	if a, b := p.ClusterStarted(), p.owner.rmd.startup.Load(); !a || b {
		p.writeErrf(w, r,
			"%s primary is not ready yet to start rebalance (started=%t, starting-up=%t)", p.si, a, b)
		return
	}

	// (I.1) Prepare phase - inform other nodes.
	urlPath := cmn.URLPathDaemonProxy.Join(proxyid)
	q := url.Values{}
	q.Set(cmn.URLParamPrepare, "true")
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPut, Path: urlPath, Query: q}
	if cluMeta, err := p.cluMeta(cmetaFillOpt{skipSmap: true}); err == nil {
		args.req.Body = cos.MustMarshal(cluMeta)
	} else {
		p.writeErr(w, r, err)
		return
	}
	args.to = cluster.AllNodes
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := res.errorf("node %s failed to set primary %s in the prepare phase", res.si, proxyid)
		p.writeErr(w, r, err)
		freeCallResults(results)
		return
	}
	freeCallResults(results)

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
	q.Set(cmn.URLParamPrepare, "false")
	args = allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPut, Path: urlPath, Query: q}
	args.to = cluster.AllNodes
	results = p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		if res.si.ID() == proxyid {
			cos.ExitLogf("Commit phase failure: new primary %q returned err: %v", proxyid, res.err)
		} else {
			glog.Errorf("Commit phase failure: %s returned err %v when setting primary = %s",
				res.si.ID(), res.err, proxyid)
		}
	}
	freeCallResults(results)
}

/////////////////////////////////////////
// DELET /v1/cluster - self-unregister //
/////////////////////////////////////////

const testInitiatedRm = "test-initiated-removal" // see TODO below

func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathClusterDaemon.L)
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
	if p.forwardCP(w, r, nil, sid) {
		return
	}
	var errCode int
	if p.isIntraCall(r.Header) {
		if cid := r.Header.Get(cmn.HdrCallerID); cid == sid {
			errCode, err = p.unregNode(&cmn.ActionMsg{Action: "self-initiated-removal"}, node, false /*skipReb*/)
		} else {
			err = fmt.Errorf("expecting self-initiated removal (%s != %s)", cid, sid)
		}
	} else {
		// TODO: phase-out tutils.RemoveNodeFromSmap (currently the only user)
		//       and disallow external calls altogether
		errCode, err = p.callRmSelf(&cmn.ActionMsg{Action: testInitiatedRm}, node, false /*skipReb*/)
	}
	if err != nil {
		p.writeErr(w, r, err, errCode)
	}
}

// Ask the node (`si`) to permanently or temporarily remove itself from the cluster, in
// accordance with the specific `msg.Action` (that we also enumerate and assert below).
func (p *proxyrunner) callRmSelf(msg *cmn.ActionMsg, si *cluster.Snode, skipReb bool) (errCode int, err error) {
	var (
		config  = cmn.GCO.Get()
		smap    = p.owner.smap.get()
		node    = smap.GetNode(si.ID())
		timeout = config.Timeout.CplaneOperation.D()
		args    = callArgs{si: node, timeout: timeout}
	)
	if node == nil {
		err = &errNodeNotFound{fmt.Sprintf("cannot %q", msg.Action), si.ID(), p.si, smap}
		return http.StatusNotFound, err
	}
	switch msg.Action {
	case cmn.ActShutdownNode:
		body := cos.MustMarshal(cmn.ActionMsg{Action: cmn.ActShutdown})
		args.req = cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathDaemon.S, Body: body}
	case cmn.ActStartMaintenance, cmn.ActDecommissionNode, testInitiatedRm:
		act := &cmn.ActionMsg{Action: msg.Action}
		if msg.Action == cmn.ActDecommissionNode {
			act.Value = msg.Value
		}
		body := cos.MustMarshal(act)
		args.req = cmn.ReqArgs{Method: http.MethodDelete, Path: cmn.URLPathDaemonUnreg.S, Body: body}
	default:
		debug.AssertMsg(false, "invalid action: "+msg.Action)
	}
	glog.Infof("%s: removing node %s via %q", p.si, node, msg.Action)
	res := p.call(args)
	er, d := res.err, res.details
	_freeCallRes(res)
	if er != nil {
		glog.Warningf("%s: %s that is being removed via %q fails to respond: %v[%s]",
			p.si, node, msg.Action, er, d)
	}
	if msg.Action == cmn.ActDecommissionNode || msg.Action == testInitiatedRm {
		// NOTE: proceeding anyway even if all retries fail
		errCode, err = p.unregNode(msg, si, skipReb)
	}
	return
}

func (p *proxyrunner) unregNode(msg *cmn.ActionMsg, si *cluster.Snode, skipReb bool) (errCode int, err error) {
	ctx := &smapModifier{
		pre:      p._unregNodePre,
		post:     p._perfRebPost,
		final:    p._syncFinal,
		msg:      msg,
		sid:      si.ID(),
		skipReb:  skipReb,
		isTarget: si.IsTarget(),
	}
	err = p.owner.smap.modify(ctx)
	return ctx.status, err
}

func (p *proxyrunner) _unregNodePre(ctx *smapModifier, clone *smapX) error {
	const verb = "remove"
	sid := ctx.sid
	node := clone.GetNode(sid)
	if !clone.isPrimary(p.si) {
		return newErrNotPrimary(p.si, clone, fmt.Sprintf("cannot cancel %s %s", verb, sid))
	}
	if node == nil {
		ctx.status = http.StatusNotFound
		return &errNodeNotFound{"failed to " + verb, sid, p.si, clone}
	}
	if !p.NodeStarted() {
		ctx.status = http.StatusServiceUnavailable
		return nil
	}
	if node.IsProxy() {
		clone.delProxy(sid)
		glog.Infof("%s %s (num proxies %d)", verb, node, clone.CountProxies())
	} else {
		clone.delTarget(sid)
		glog.Infof("%s %s (num targets %d)", verb, node, clone.CountTargets())
	}
	clone.staffIC()
	p.rproxy.nodes.Delete(ctx.sid)
	return nil
}

////////////////////////
// helpers: rebalance //
////////////////////////

func (p *proxyrunner) canStartRebalance() error {
	smap := p.owner.smap.get()
	if err := smap.validate(); err != nil {
		return err
	}
	if !smap.IsPrimary(p.si) {
		err := newErrNotPrimary(p.si, smap)
		debug.AssertNoErr(err)
		return err
	}
	// NOTE:
	//      When cluster is starting up rebalance is handled elsewhere (see p.resumeReb).
	//      In effect, all rebalance-triggering events including direct and indirect
	//      user requests (such as shutdown, decommission, cnm.ActRebalance, etc.)
	//      are not permitted and will fail.
	if a, b := p.ClusterStarted(), p.owner.rmd.startup.Load(); !a || b {
		return fmt.Errorf("%s primary is not ready yet to start rebalance (started=%t, starting-up=%t)",
			p.si, a, b)
	}
	if smap.CountActiveTargets() < 2 {
		return &errNotEnoughTargets{p.si, smap, 2}
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		return errRebalanceDisabled
	}
	return nil
}

func (p *proxyrunner) requiresRebalance(prev, cur *smapX) bool {
	if cur.CountActiveTargets() < 2 {
		return false
	}
	if cur.CountActiveTargets() > prev.CountActiveTargets() {
		return true
	}
	if cur.CountTargets() > prev.CountTargets() {
		return true
	}
	for _, si := range cur.Tmap {
		if !prev.isPresent(si) {
			return true
		}
	}
	bmd := p.owner.bmd.get()
	if bmd.IsECUsed() {
		// If there is any target missing we must start rebalance.
		for _, si := range prev.Tmap {
			if !cur.isPresent(si) {
				return true
			}
		}
	}
	return false
}
