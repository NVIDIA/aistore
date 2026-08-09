// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

func (p *proxy) cluHandler(w http.ResponseWriter, r *http.Request) {
	p._clu(w, r, false /*isPub*/)
}

func (p *proxy) cluPubHandler(w http.ResponseWriter, r *http.Request) {
	p._clu(w, r, true /*isPub*/)
}

// access control is per-method - delegated to each httpclu*
func (p *proxy) _clu(w http.ResponseWriter, r *http.Request, isPub bool) {
	switch r.Method {
	case http.MethodGet:
		p.httpcluget(w, r, isPub)
	case http.MethodPost:
		p.httpclupost(w, r, isPub)
	case http.MethodPut:
		if !p.ClusterStarted() {
			// TODO: may require preventive return from assorted paths (see e.g. xstatusOne)
			nlog.Warningln("cluster not started:", r.Method, r.URL.RawQuery)
		}
		p.httpcluput(w, r, isPub)
	case http.MethodDelete:
		if !p.ClusterStarted() {
			// TODO: ditto
			nlog.Warningln("cluster not started:", r.Method, r.URL.RawQuery)
		}
		p.httpcludel(w, r, isPub)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

// +gen:endpoint GET /v1/cluster[apc.QparamWhat=string]
// Query cluster states, statistics, and information.
// Supports various query types: node stats, system info, backends, remote AIS, mountpaths, etc.
func (p *proxy) httpcluget(w http.ResponseWriter, r *http.Request, isPub bool) {
	var (
		query = r.URL.Query()
		what  = query.Get(apc.QparamWhat)
	)
	debug.Assert(reqIsPub(r) == isPub)
	if isPub {
		if err := p.checkAccess(w, r, nil, apc.AceShowCluster); err != nil {
			return
		}
	}
	// execute via IC; public callers still need the same cluster visibility permission
	if what == apc.WhatOneXactStatus {
		p.ic.xstatusOne(w, r)
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
		c.Auth = config.Auth.PublicClone()
		p.writeJSON(w, r, &c, what)
	case apc.WhatBMD, apc.WhatSmapVote, apc.WhatSnode, apc.WhatSmap:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)
	default:
		p.writeErrf(w, r, fmtUnknownQue, what)
	}
}

// apc.WhatQueryXactStats (NOTE: may poll for quiescence)
func (p *proxy) xquery(w http.ResponseWriter, r *http.Request, what string, query url.Values) {
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
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
	resRaw, erred := p._rawResults(w, r, results)
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
	type clusterMountpathsRaw struct {
		Targets cos.JSONRawMsgs `json:"targets"`
	}

	targetMountpaths, erred := p._queryTs(w, r, query)
	if targetMountpaths == nil || erred {
		return
	}
	out := &clusterMountpathsRaw{}
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
	rawResults, terr, timedOut := _rawResWithTimeout(results)
	if terr == nil {
		freeBcArgs(args)
		return rawResults, false
	}

	// on client timeout: retry just once with >= 2x timeout
	if timedOut {
		config := cmn.GCO.Get()
		args.timeout = max(args.timeout*2, config.Client.Timeout.D())
		nlog.Warningln(p.String(), "retrying control-plane timeout (query=", query.Encode(), "):", terr)

		results = p.bcastGroup(args)
		rawResults, terr, _ = _rawResWithTimeout(results)
		if terr == nil {
			freeBcArgs(args)
			return rawResults, false
		}
	}

	freeBcArgs(args)
	p.writeErr(w, r, terr)
	return nil, true
}

//
// TODO: refactor and consolidate: _rawResults() vs _rawResWithTimeout()
//

func (p *proxy) _rawResults(w http.ResponseWriter, r *http.Request, results sliceResults) (cos.JSONRawMsgs, bool) {
	rawResults := make(cos.JSONRawMsgs, len(results))
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			if cos.IsErrClientTimeout(res.err) {
				nlog.Warningln(p.String(), "control-plane timeout calling", res.si.StringEx())
			}
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			return nil, true
		}
		rawResults[res.si.ID()] = res.bytes
	}
	freeBcastRes(results)
	return rawResults, false
}

func _rawResWithTimeout(results sliceResults) (cos.JSONRawMsgs, error, bool /*timed out*/) {
	rawResults := make(cos.JSONRawMsgs, len(results))
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			freeBcastRes(results)
			return nil, res.toErr(), cos.IsErrClientTimeout(res.err)
		}
		rawResults[res.si.ID()] = res.bytes
	}
	freeBcastRes(results)
	return rawResults, nil, false
}

/////////////////////
// PUT /v1/cluster //
/////////////////////

// - cluster membership, including maintenance and decommission
// - rebalance
// - set-primary
// +gen:endpoint PUT /v1/cluster[apc.QparamTransient=bool] action=[apc.ActSetConfig=cmn.ConfigToSet|apc.ActResetConfig=apc.ActMsg|apc.ActRotateLogs=apc.ActMsg|apc.ActShutdownCluster=apc.ActMsg|apc.ActDecommissionCluster=apc.ActValRmNode|apc.ActStartMaintenance=apc.ActValRmNode|apc.ActDecommissionNode=apc.ActValRmNode|apc.ActShutdownNode=apc.ActValRmNode|apc.ActRmNodeUnsafe=apc.ActValRmNode|apc.ActStopMaintenance=apc.ActValRmNode|apc.ActResetStats=apc.ActMsg|apc.ActClearLcache=apc.ActMsg|apc.ActXactStart=apc.ActMsg|apc.ActXactStop=apc.ActMsg|apc.ActReloadBackendCreds=apc.ActMsg|apc.ActBumpMetasync=apc.ActMsg]
// +gen:payload apc.ActDecommissionCluster={"action": "decommission", "value": {"sid": "target_id", "skip_rebalance": false, "rm_user_data": true}}
// +gen:payload apc.ActResetStats={"action": "reset-stats", "value": false}
// Administrative cluster operations: configuration changes, node management, log rotation, shutdown/decommission operations.
func (p *proxy) httpcluput(w http.ResponseWriter, r *http.Request, isPub bool) {
	apiItems, err := p.parseURL(w, r, apc.URLPathClu.L, 0, true)
	if err != nil {
		return
	}

	// admin access via pub net - all actions
	debug.Assert(reqIsPub(r) == isPub)
	if isPub {
		if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
			return
		}
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
		if transient := cos.IsParseBool(query.Get(apc.QparamTransient)); transient {
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

// +gen:payload apc.ActSetConfig={"action": "set-config", "value": {"timeout": {"send_file_time": "10m"}}}
func (p *proxy) setCluCfgPersistent(w http.ResponseWriter, r *http.Request, toUpdate *cmn.ConfigToSet, msg *apc.ActMsg) {
	ctx := &configModifier{
		pre:      _setConfPre,
		final:    p._syncConfFinal,
		msg:      msg,
		toUpdate: toUpdate,
		wait:     true,
	}
	config := cmn.GCO.Get()

	//
	// assorted validations: 1 through 4
	//

	// 1. critical cluster-wide config updates require cluster restart
	if toUpdate.Net != nil && toUpdate.Net.HTTP != nil {
		from, _ := jsoniter.Marshal(config.Net.HTTP)
		to, _ := jsoniter.Marshal(toUpdate.Net.HTTP)
		_warnUpd("net.http", string(from), string(to))

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
	// 2. AuthN
	if toUpdate.Auth != nil && toUpdate.Auth.Enabled != nil {
		authEnabled := *toUpdate.Auth.Enabled

		if !config.Auth.Enabled && authEnabled {
			// enabling auth - always validate
			clone := new(cmn.AuthConf)
			cos.CopyStruct(clone, &config.Auth)
			config.Auth.CopyTo(clone)

			if ecode, err := p.validateEnableAuth(r, clone, toUpdate.Auth); err != nil {
				p.writeErr(w, r, err, ecode)
				return
			}
		}
		if config.Auth.Enabled != authEnabled {
			_warnUpd("config.auth JWT/OIDC", strconv.FormatBool(config.Auth.Enabled), strconv.FormatBool(authEnabled))
		}

		if ic := toUpdate.Auth.IntraCluster; ic != nil && ic.Enabled != nil {
			cur := config.Auth.IntraClusterConfigured() // raw config bit (compare with SignVerifyEnabled() runtime)
			upd := *ic.Enabled
			if !cur && upd && cmn.IsV50Bridge() {
				p.writeErr(w, r, errors.New("intra-cluster auth (Ed25519 sign/verify) cannot be enabled on a v5.0 bridge release"),
					http.StatusPreconditionFailed)
				return
			}
			if cur != upd {
				_warnUpd("config.auth.intra_cluster", strconv.FormatBool(cur), strconv.FormatBool(upd))
			}
		}
	}
	// 3. Tracing
	if toUpdate.Tracing != nil {
		from, _ := jsoniter.Marshal(config.Tracing)
		to, _ := jsoniter.Marshal(toUpdate.Tracing)
		_warnUpd("config.tracing", string(from), string(to))
	}
	// 4. config.Timeout section
	if toUpdate.Timeout != nil {
		if toUpdate.Timeout.CplaneOperation != nil &&
			*toUpdate.Timeout.CplaneOperation != config.Timeout.CplaneOperation {
			_warnUpd("timeout.cplane_operation", config.Timeout.CplaneOperation.String(), toUpdate.Timeout.CplaneOperation.String())
		}
		if toUpdate.Timeout.MaxKeepalive != nil &&
			*toUpdate.Timeout.MaxKeepalive != config.Timeout.MaxKeepalive {
			_warnUpd("timeout.max_keepalive", config.Timeout.MaxKeepalive.String(), toUpdate.Timeout.MaxKeepalive.String())
		}
	}

	// 5. cross-section: keepalivetracker.*.interval vs timeout.max_keepalive
	if toUpdate.Keepalive != nil || (toUpdate.Timeout != nil && toUpdate.Timeout.MaxKeepalive != nil) {
		if err := _checkKalive(config, toUpdate); err != nil {
			p.writeErr(w, r, err, http.StatusBadRequest)
			return
		}
	}

	// do
	if _, err := p.owner.config.modify(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

// apply the (keepalivetracker.*.interval >= timeout.max_keepalive)
// rule to the effective post-merge values
func _checkKalive(config *cmn.Config, toUpdate *cmn.ConfigToSet) error {
	kalive := *config.Keepalive
	maxKeepalive := config.Timeout.MaxKeepalive

	if toUpdate.Timeout != nil && toUpdate.Timeout.MaxKeepalive != nil {
		maxKeepalive = *toUpdate.Timeout.MaxKeepalive
	}
	if ka := toUpdate.Keepalive; ka != nil {
		if ka.Proxy != nil && ka.Proxy.Interval != nil {
			kalive.Proxy.Interval = *ka.Proxy.Interval
		}
		if ka.Target != nil && ka.Target.Interval != nil {
			kalive.Target.Interval = *ka.Target.Interval
		}
		if err := kalive.Validate(); err != nil {
			return err
		}
	}
	if kalive.Proxy.Interval < maxKeepalive {
		return fmt.Errorf("keepalivetracker.proxy.interval=%s should be >= timeout.max_keepalive=%s",
			kalive.Proxy.Interval, maxKeepalive)
	}
	if kalive.Target.Interval < maxKeepalive {
		return fmt.Errorf("keepalivetracker.target.interval=%s should be >= timeout.max_keepalive=%s",
			kalive.Target.Interval, maxKeepalive)
	}
	return nil
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

func _warnUpd(what, from, to string) {
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
	if err := _checkTransient(toUpdate); err != nil {
		p.writeErr(w, r, err) // cmn.ErrUnsupp => 501
		return
	}

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
		Query:  url.Values{apc.QparamTransient: []string{"true"}},
	}
	args.to = core.AllNodes
	p.bcastAndRespond(w, r, args)
	freeBcArgs(args)
}

// Transient (in-memory, not persisted) updates do not go through setCluCfgPersistent
// and therefore bypass its pre-flight validations. Hence, the limitations that entail:
// - everything in cmn.ConfigRestartRequired (transient updates to restart-required knobs are meaningless)
// - `auth` (gated enable-time validation)
// - `keepalivetracker` (cross-section rule vs timeout.max_keepalive)
func _checkTransient(toUpdate *cmn.ConfigToSet) error {
	const action = "transiently update"
	switch {
	case toUpdate.Auth != nil:
		return cmn.NewErrUnsupp(action, "config.auth")
	case toUpdate.Net != nil:
		return cmn.NewErrUnsupp(action, "config.net")
	case toUpdate.Tracing != nil:
		return cmn.NewErrUnsupp(action, "config.tracing")
	case toUpdate.Memsys != nil:
		return cmn.NewErrUnsupp(action, "config.memsys")
	case toUpdate.Keepalive != nil:
		return cmn.NewErrUnsupp(action, "config.keepalivetracker")
	case toUpdate.Timeout != nil && toUpdate.Timeout.MaxKeepalive != nil:
		return cmn.NewErrUnsupp(action, "timeout.max_keepalive")
	case toUpdate.Timeout != nil && toUpdate.Timeout.CplaneOperation != nil:
		return cmn.NewErrUnsupp(action, "timeout.cplane_operation")
	}
	return nil
}

func _setConfPre(ctx *configModifier, clone *globalConfig) (updated bool, err error) {
	if err = cmn.CopyProps(ctx.toUpdate, clone, apc.Cluster); err != nil {
		return
	}
	updated = true
	return
}

func (p *proxy) _syncConfFinal(ctx *configModifier, clone *globalConfig) {
	msg := p.newAmsg(ctx.msg, nil)
	wg := p.metasyncer.sync(revsPair{clone, msg})
	if ctx.wait {
		wg.Wait()
	}
}

// xstart: rebalance, resilver, other "startables" (see xaction/api.go)
// +gen:payload apc.ActXactStart={"action": "start-xaction", "name": "rebalance"}
func (p *proxy) xstart(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	var (
		xargs        xact.ArgsMsg
		singleTarget *meta.Snode
	)
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
				p.writeErr(w, r, cmn.NewErrBckNotFound(&xargs.Bck))
				return
			}
		} else if msg.Name != "" {
			p.writeErrf(w, r, "invalid limited-scope %q: (n/a bucket, %q prefix)", apc.ActRebalance, msg.Name)
			return
		}

		var cleanup bool
		if xargs.Flags&xact.FlagRemoveMisplaced != 0 {
			if running, xid := p.notifs.isRebRunning(); running {
				p.writeErrf(w, r, "cannot start rebalance in cleanup mode: rebalance[%s] is currently running", xid)
				return
			}
			// special cleanup mode:
			// piggy-back on the rebalance lifecycle (xreg, abort, status, rebID) to walk
			// mountpaths and remove local copies of objects upon checking their respective
			// expected locations
			cleanup = true
		}
		p.rebalanceCluster(w, r, msg, cleanup)
		return
	}

	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathXactions.S}

	switch xargs.Kind {
	case apc.ActBlobDl:
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
	case apc.ActResilver:
		if xargs.DaemonID == "" {
			freeBcArgs(args)
			err := cmn.NewErrUnsupp("run resilver", "on all targets in the cluster")
			p.writeErr(w, r, err, http.StatusNotImplemented)
			return
		}
		args.smap = p.owner.smap.get()
		tsi := args.smap.GetTarget(xargs.DaemonID)
		if tsi == nil {
			freeBcArgs(args)
			err := &errNodeNotFound{si: p.si, smap: args.smap, msg: "cannot resilver", id: xargs.DaemonID}
			p.writeErr(w, r, err)
			return
		}
		singleTarget = tsi
		xargs.ID = cos.GenUUID() // assign UUID
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
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			return
		}
		debug.Func(func() {
			if xargs.Kind != apc.ActResilver {
				return
			}
			xid := string(res.bytes)
			debug.Assertf(xargs.ID == xid, "expecting proxy-assigned UUID %q, got %q", xargs.ID, xid)
		})
	}
	freeBcastRes(results)

	// IC to listen on
	if xargs.ID != "" {
		smap := p.owner.smap.get()
		var srcs meta.NodeMap
		if singleTarget != nil {
			srcs = meta.NodeMap{singleTarget.ID(): singleTarget}
		}
		nl := xact.NewXactNL(xargs.ID, xargs.Kind, &smap.Smap, srcs)
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

// +gen:payload apc.ActXactStop={"action": "stop-xaction", "name": "rebalance"}
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

// +gen:payload apc.ActReloadBackendCreds={"action": "reload-backend-creds", "name": "aws"}
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

// admin call
func (p *proxy) rebalanceCluster(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, cleanup bool) {
	smap := p.owner.smap.get()
	if err := p.canRebalance(smap, cleanup); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if smap.CountTargets() < 2 {
		p.writeErr(w, r, &errNotEnoughTargets{p.si, smap, 2})
		return
	}
	if nat := smap.CountActiveTs(); nat < 2 {
		if cleanup {
			p.writeErrf(w, r, "not enough active targets (%d, %s) - cannot run rebalance in cleanup mode", nat, smap.StringEx())
			return
		}
		nlog.Warningf("%s: not enough active targets (%d) - proceeding to rebalance cluster anyway", p, nat)
	}
	rmdCtx := &rmdModifier{
		pre:     rmdInc,
		final:   rmdSync,
		p:       p,
		smapCtx: &smapModifier{smap: smap, msg: msg},
	}
	if _, err := p.owner.rmd.modify(rmdCtx); err != nil {
		p.writeErr(w, r, err)
		return
	}
	writeXid(w, rmdCtx.rebID)
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
		if transient := cos.IsParseBool(query.Get(apc.QparamTransient)); transient {
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
		config := cmn.GCO.Get()
		if !config.Net.HTTP.UseHTTPS {
			p.writeErrMsg(w, r, "invalid request to reload X509 certs (running plain HTTP)")
			return
		}
		if len(items) < 2 {
			p.cluLoadX509(w, r)
		} else if sid := items[1]; sid == p.SID() {
			p.daeLoadX509(w, r)
		} else {
			smap := p.owner.smap.get()
			node := smap.GetNode(sid)
			if node == nil {
				err := &errNodeNotFound{si: p.si, smap: smap, msg: "X.509 load failure:", id: sid}
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
	for _, phase := range []string{apc.Begin2PC, apc.Commit2PC} {
		var (
			path string
			args = allocBcArgs()
		)
		// bcast
		path = cos.JoinWP(upath.S, np, phase)
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
			if !slices.Contains(urls, u) {
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
