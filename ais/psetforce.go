// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

// set new primary, scenarios including joining entire (split-brain) cluster

//
// cluster
//

func (p *proxy) cluSetPrimary(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathCluProxy.L, 1, false)
	if err != nil {
		return
	}

	smap := p.owner.smap.get()
	if err := smap.validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	debug.Assert(smap.IsPrimary(p.si))
	npid := apiItems[0]
	npsi := smap.GetProxy(npid)

	if npsi == nil {
		// a) go with force
		p._withForce(w, r, npid, smap)
		return
	}

	if npid == p.SID() {
		debug.Assert(p.SID() == smap.Primary.ID()) // must be forwardCP-ed
		nlog.Warningln(p.String(), "(self) is already primary, nothing to do")
		return
	}

	if err := _checkFlags(npsi); err != nil {
		p.writeErr(w, r, err)
		return
	}

	// b) regular set-primary
	if p.settingNewPrimary.CAS(false, true) {
		p._setPrimary(w, r, npsi)
		p.settingNewPrimary.Store(false)
	} else {
		nlog.Warningln("setting new primary in progress now...")
	}
}

func (p *proxy) _withForce(w http.ResponseWriter, r *http.Request, npid string, smap *smapX) {
	query := r.URL.Query()
	force := cos.IsParseBool(query.Get(apc.QparamForce))
	if !force {
		err := &errNodeNotFound{msg: "set-primary failure:", id: npid, si: p.si, smap: smap}
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if p.settingNewPrimary.CAS(false, true) {
		p.forceJoin(w, r, npid, query)
		p.settingNewPrimary.Store(false)
	} else {
		err := errors.New("setting new primary is in progress, cannot use force")
		p.writeErr(w, r, err)
	}
}

func _checkFlags(npsi *meta.Snode) error {
	if npsi.InMaintOrDecomm() {
		s := "under maintenance"
		if !npsi.InMaint() {
			s = "being decommissioned"
		}
		return fmt.Errorf("%s cannot become a new primary as it is currently %s", npsi, s)
	}
	if npsi.Flags.IsSet(meta.SnodeNonElectable) {
		return fmt.Errorf("%s is non-electable and cannot become a new primary", npsi)
	}
	return nil
}

func (p *proxy) _setPrimary(w http.ResponseWriter, r *http.Request, npsi *meta.Snode) {
	//
	// (I.1) Prepare phase - inform other nodes.
	//
	urlPath := apc.URLPathDaeProxy.Join(npsi.ID())
	q := make(url.Values, 1)
	q.Set(apc.QparamPrepare, "true")
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: urlPath, Query: q}

	cluMeta, errM := p.cluMeta(cmetaFillOpt{skipSmap: true, skipPrimeTime: true})
	if errM != nil {
		p.writeErr(w, r, errM)
		return
	}
	args.req.Body = cos.MustMarshal(cluMeta)

	npname := npsi.StringEx()

	args.to = core.AllNodes
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err := res.errorf("node %s failed to set primary %s in the prepare phase (err: %v)", res.si, npname, res.err)
		p.writeErr(w, r, err)
		freeBcastRes(results)
		return
	}
	freeBcastRes(results)

	//
	// (I.2) Prepare phase - local changes.
	//
	err := p.owner.smap.modify(&smapModifier{pre: func(_ *smapModifier, clone *smapX) error {
		clone.Primary = npsi
		p.metasyncer.becomeNonPrimary()
		return nil
	}})
	debug.AssertNoErr(err)

	//
	// (II) Commit phase.
	//
	q.Set(apc.QparamPrepare, "false")
	args = allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: urlPath, Query: q}
	args.to = core.AllNodes
	results = p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		if res.si.ID() == npsi.ID() {
			// FATAL
			cos.ExitLogf("commit phase failure: new primary %s returned %v", npname, res.err)
		} else {
			nlog.Errorf("node %s failed to set new primary %s in the commit phase (err: %v)", res.si, npname, res.err)
		}
	}
	freeBcastRes(results)
}

// Force primary change (*****)
// 10-steps sequence that now supports merging two different clusters
// Background:
// - when for whatever reason some of the nodes that include at least one proxy stop seeing the _current_ primary
// they may, after keep-aliving for a while and talking to each other, go ahead and elect a new primary -
// from themselves and for themselves;
// - when the network is back up again we then discover split-brain in progress, and we may not like it.
//
// Beware!.. well, just beware.
func (p *proxy) forceJoin(w http.ResponseWriter, r *http.Request, npid string, q url.Values) {
	const (
		tag = "designated primary"
		act = "force-join"
	)
	// 1. validate args
	if p.SID() == npid {
		nlog.Warningln(p.String(), "(self) is the", tag, "- nothing to do")
		return
	}
	var (
		smap          = p.owner.smap.get()
		psi           = smap.GetProxy(npid)
		newPrimaryURL = q.Get(apc.QparamPrimaryCandidate)
		nurl          = newPrimaryURL
	)
	if psi == nil && newPrimaryURL == "" {
		msg := act + " failure (w/ empty destination URL):"
		err := &errNodeNotFound{msg: msg, id: npid, si: p.si, smap: smap}
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if nurl == "" {
		nurl = cos.Left(psi.ControlNet.URL, psi.PubNet.URL)
	}
	if nurl == "" {
		err := fmt.Errorf("cannot %s %s (and cluster %s) to %s[%s]: missing destination URL", act, p, smap, tag, npid)
		p.writeErr(w, r, err)
		return
	}

	// 2. get destination Smap (henceforth, newSmap)
	newSmap, err := p.smapFromURL(nurl)
	if err != nil && psi != nil && psi.PubNet.URL != psi.ControlNet.URL {
		newSmap, err = p.smapFromURL(psi.PubNet.URL)
	}
	if err != nil {
		err := fmt.Errorf("%s %s to %s[%q, %q]: %s", act, p, tag, npid, newPrimaryURL, err)
		p.writeErr(w, r, err)
		return
	}
	npsi := newSmap.Primary
	if nurl != npsi.PubNet.URL && nurl != npsi.ControlNet.URL {
		// must be reachable via its own (new)Smap
		err := fmt.Errorf("%s: %s URLs %q vs (pub %q, ctrl %q)", p, tag, nurl, npsi.PubNet.URL, npsi.ControlNet.URL)
		nlog.Warningln(err)
		if _, e := p.smapFromURL(npsi.ControlNet.URL); e != nil {
			if _, e = p.smapFromURL(npsi.PubNet.URL); e != nil {
				p.writeErr(w, r, err)
				return
			}
		}
	}
	if npid != npsi.ID() {
		err := fmt.Errorf("%s: according to the destination %s %s[%s] is not _the_ primary", p, newSmap.StringEx(), tag, npid)
		p.writeErr(w, r, err)
		return
	}
	if err := _checkFlags(npsi); err != nil {
		p.writeErr(w, r, err)
		return
	}
	npname := npsi.StringEx()

	//
	// 3. begin
	//
	what := "(split-brain):"
	if smap.UUID != newSmap.UUID {
		what = "a different cluster:"
	}
	nlog.Warningln(act, "entire cluster [", p.String(), smap.StringEx(), "] to:\n", "\t", what, "[", npname, newSmap.StringEx(), "]")

	// 4. get destination cluMeta from npsi
	cargs := allocCargs()
	{
		cargs.si = npsi
		cargs.timeout = cmn.Rom.MaxKeepalive()
		cargs.req = cmn.HreqArgs{Path: apc.URLPathDae.S, Query: url.Values{apc.QparamWhat: []string{apc.WhatSmapVote}}}
		cargs.cresv = cresCM{} // -> cluMeta
	}
	res := p.call(cargs, newSmap /* -> header */)
	err = res.unwrap()
	freeCargs(cargs)
	if err != nil {
		freeCR(res)
		p.writeErr(w, r, err)
		return
	}
	ncm, ok := res.v.(*cluMeta)
	debug.Assert(ok)
	freeCR(res)

	if err := ncm.validate(); err != nil {
		p.writeErrf(w, r, "cannot %s %s to %s: %v", act, p, npname, err)
		return
	}

	// 5. backup (see rollback below)
	cm, err := p.cluMeta(cmetaFillOpt{skipPrimeTime: true})
	if err != nil {
		p.writeErrf(w, r, "cannot %s %s to %s: %v", act, p, npname, err) // (unlikely)
		return
	}

	nlog.Infoln(act, "(6) prepare")

	// 6. prepare phase whereby all members health-ping => npsi (see `prepForceJoin`)
	bargs := allocBcArgs()
	{
		aimsg := p.newAmsgActVal(apc.ActPrimaryForce, newSmap)
		q := make(url.Values, 1)
		q.Set(apc.QparamPrepare, "true")
		bargs.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathDaeForceJoin.S, Query: q, Body: cos.MustMarshal(aimsg)}
		bargs.to = core.AllNodes
	}
	results := p.bcastGroup(bargs)
	freeBcArgs(bargs)
	for _, res := range results {
		if res.err != nil {
			p.writeErrf(w, r, "node %s failed to contact new primary %s in the prepare phase (err: %v)",
				res.si, npname, res.err)
			freeBcastRes(results)
			return
		}
	}
	freeBcastRes(results)

	aimsg := p.newAmsgActVal(apc.ActPrimaryForce, nil)
	nlog.Infoln(act, "(7) update clu-meta in mem")

	// 7. update cluMeta in memory (= destination)
	if err = cmn.GCO.Update(&ncm.Config.ClusterConfig); err != nil {
		// rollback #1
		nlog.Errorln(act, "rollback #1", err)
		cm.metasync(p, aimsg, true)
		p.writeErr(w, r, err)
		return
	}
	p.owner.bmd.put(ncm.BMD)
	p.owner.rmd.put(ncm.RMD)

	nlog.Infoln(act, "(8) join self ->", npname)

	// 8. join self (up to 3 attempts)
	joinURL, secondURL := npsi.ControlNet.URL, npsi.PubNet.URL
	if nurl == npsi.PubNet.URL {
		joinURL, secondURL = npsi.PubNet.URL, npsi.ControlNet.URL
	}
	p.owner.smap.put(newSmap)
	res = p.regTo(joinURL, npsi, apc.DefaultTimeout, nil, false /*keepalive*/)
	e, eh := res.err, res.toErr()
	freeCR(res)
	if e != nil {
		if joinURL != secondURL {
			nlog.Errorln(res.toErr(), "- 2nd attempt via", secondURL)
			res = p.regTo(secondURL, npsi, apc.DefaultTimeout, nil, false)
			e, eh = res.err, res.toErr()
			freeCR(res)
			if e != nil {
				time.Sleep(time.Second)
				nlog.Warningln(res.toErr(), "- 3d (final) attempt via", joinURL)
				res = p.regTo(joinURL, npsi, apc.DefaultTimeout, nil, false)
				eh = res.toErr()
				freeCR(res)
			}
		}
	}
	if eh != nil {
		// rollback #2
		nlog.Errorln(act, "rollback #2", eh)
		if nested := cmn.GCO.Update(&cm.Config.ClusterConfig); nested != nil {
			nlog.Errorf("FATAL: nested config-update error when rolling back [%s %s to %s]: %v",
				act, p, npname, nested) // (unlikely)
		}
		p.owner.smap.put(cm.Smap)
		p.owner.bmd.put(cm.BMD)
		p.owner.rmd.put(cm.RMD)

		p.writeErr(w, r, eh)
		return
	}

	nlog.Infoln(act, "(9) commit")

	// 9. commit phase (see `prepForceJoin`)
	if len(smap.Tmap) == 0 && len(smap.Pmap) == 1 { // TODO -- FIXME: count
		goto st10
	}
	bargs = allocBcArgs()
	{
		aimsg = p.newAmsgActVal(apc.ActPrimaryForce, ncm)
		q := make(url.Values, 1)
		q.Set(apc.QparamPrepare, "false") // TODO -- FIXME: begin/commit/abort
		bargs.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathDaeForceJoin.S, Query: q, Body: cos.MustMarshal(aimsg)}
		bargs.to = core.SelectedNodes
		bargs.nodes = make([]meta.NodeMap, 0, 2)
		if len(smap.Tmap) > 0 {
			bargs.nodes = append(bargs.nodes, smap.Tmap)
		}
		if len(smap.Pmap) > 1 {
			bargs.nodes = append(bargs.nodes, smap.Pmap)
		}
	}
	if len(bargs.nodes) == 0 {
		goto st10
	}
	results = p.bcastGroup(bargs)
	freeBcArgs(bargs)
	for _, res := range results {
		if res.err != nil {
			p.writeErrf(w, r, "node %s failed commit phase (err: %v)", res.si, res.err)
			freeBcastRes(results)

			// TODO -- FIXME: rollback #3
			return
		}
	}
	freeBcastRes(results)
st10:
	p.metasyncer.becomeNonPrimary() // point of no return

	time.Sleep(time.Second)

	nlog.Infoln(act, "(10) ask npsi to bump metasync")

	// 10. finally, ask npsi to bump versions and metasync all (see `msyncForceAll`)
	cargs = allocCargs()
	msg := &apc.ActMsg{Action: apc.ActBumpMetasync}
	{
		cargs.si = npsi
		cargs.timeout = cmn.Rom.MaxKeepalive()
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathClu.S, Body: cos.MustMarshal(msg)}
	}
	res = p.call(cargs, newSmap)
	err = res.unwrap()
	freeCR(res)
	if err != nil {
		freeCargs(cargs)
		// retry once
		cargs = allocCargs()
		cargs.si = npsi
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Base: npsi.PubNet.URL, Path: apc.URLPathClu.S, Body: cos.MustMarshal(msg)}
		cargs.timeout = cmn.Rom.MaxKeepalive() + time.Second

		nlog.Errorln(err, "- failed to bump metasync, retrying...")
		res := p.call(cargs, newSmap)
		err = res.unwrap()
		freeCR(res)
	}
	if err != nil {
		p.writeErrf(w, r, "%s: failed to bump metasync => %s: %v", p, npname, err)
	}
	freeCargs(cargs)
}

// node (stray proxy, that is)
func (p *proxy) daeSetPrimary(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathDae.L, 2, false)
	if err != nil {
		return
	}
	var (
		proxyID = apiItems[1]
		query   = r.URL.Query()
		force   = cos.IsParseBool(query.Get(apc.QparamForce))
	)
	// force primary change
	if force && apiItems[0] == apc.Proxy {
		if smap := p.owner.smap.get(); !smap.isPrimary(p.si) {
			p.writeErr(w, r, newErrNotPrimary(p.si, smap))
			return
		}
		p.forceJoin(w, r, proxyID, query) // TODO -- FIXME: test
		return
	}
	prepare, err := cos.ParseBool(query.Get(apc.QparamPrepare))
	if err != nil {
		p.writeErrf(w, r, "failed to parse URL query %q: %v", apc.QparamPrepare, err)
		return
	}
	if p.owner.smap.get().isPrimary(p.si) {
		p.writeErrf(w, r, "%s (self) is primary, expecting '/v1/cluster/...' when designating a new one", p)
		return
	}
	if prepare {
		var cluMeta cluMeta
		if err := cmn.ReadJSON(w, r, &cluMeta); err != nil {
			return
		}
		if err := p.recvCluMeta(&cluMeta, "set-primary", cluMeta.SI.String()); err != nil {
			p.writeErrf(w, r, "%s: failed to receive clu-meta: %v", p, err)
			return
		}
	}

	// self
	if p.SID() == proxyID {
		smap := p.owner.smap.get()
		if smap.GetActiveNode(proxyID) == nil {
			p.writeErrf(w, r, "%s: in maintenance or decommissioned", p)
			return
		}
		if !prepare {
			p.becomeNewPrimary("")
		}
		return
	}

	// other
	smap := p.owner.smap.get()
	psi := smap.GetProxy(proxyID)
	if psi == nil {
		err := &errNodeNotFound{"cannot set new primary", proxyID, p.si, smap}
		p.writeErr(w, r, err)
		return
	}
	if prepare {
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Infoln("Preparation step: do nothing")
		}
		return
	}
	ctx := &smapModifier{pre: func(_ *smapModifier, clone *smapX) error { clone.Primary = psi; return nil }}
	err = p.owner.smap.modify(ctx)
	debug.AssertNoErr(err)
}

// destination (designated) primary
func (p *proxy) msyncForceAll(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	cm, err := p.cluMeta(cmetaFillOpt{})
	if err != nil {
		p.writeErr(w, r, err) // (unlikely)
		return
	}

	if true { // DEBUG
		aimsg := p.newAmsgActVal(apc.ActPrimaryForce, nil)
		cm.metasync(p, aimsg, false)
		return
	}
	ctx := &smapModifier{
		pre: func(_ *smapModifier, clone *smapX) error { clone.Version += 100; return nil }, // TODO -- FIXME: max(smap.Version, newSmap.Version) + 100
		final: func(_ *smapModifier, clone *smapX) {
			aimsg := p.newAmsgActVal(apc.ActPrimaryForce, msg)
			cm.Smap = clone
			cm.metasync(p, aimsg, false)
		},
	}
	err = p.owner.smap.modify(ctx)
	debug.AssertNoErr(err) // TODO -- FIXME: handle
}

//
// becoming
//

func (p *proxy) becomeNewPrimary(proxyIDToRemove string) {
	ctx := &smapModifier{
		pre:   p._becomePre,
		final: p._becomeFinal,
		sid:   proxyIDToRemove,
	}
	err := p.owner.smap.modify(ctx)
	cos.AssertNoErr(err)
}

func (p *proxy) _becomePre(ctx *smapModifier, clone *smapX) error {
	if !clone.isPresent(p.si) {
		cos.Assertf(false, "%s must always be present in the %s", p.si, clone.pp())
	}
	if ctx.sid != "" && clone.GetNode(ctx.sid) != nil {
		// decision is made: going ahead to remove
		nlog.Infof("%s: removing failed primary %s", p, ctx.sid)
		clone.delProxy(ctx.sid)

		// Remove reverse proxy entry for the node.
		p.rproxy.nodes.Delete(ctx.sid)
	}

	clone.Primary = clone.GetProxy(p.SID())
	clone.Version += 100
	clone.staffIC()
	return nil
}

func (p *proxy) _becomeFinal(ctx *smapModifier, clone *smapX) {
	var (
		bmd   = p.owner.bmd.get()
		rmd   = p.owner.rmd.get()
		msg   = p.newAmsgStr(apc.ActNewPrimary, bmd)
		pairs = []revsPair{{clone, msg}, {bmd, msg}, {rmd, msg}}
	)
	nlog.Infof("%s: distributing (%s, %s, %s) with newly elected primary (self)", p, clone, bmd, rmd)
	config, err := p.ensureConfigURLs()
	if err != nil {
		nlog.Errorln(err)
	}
	if config != nil {
		pairs = append(pairs, revsPair{config, msg})
		nlog.Infof("%s: plus %s", p, config)
	}
	etl := p.owner.etl.get()
	if etl != nil && etl.version() > 0 {
		pairs = append(pairs, revsPair{etl, msg})
		nlog.Infof("%s: plus %s", p, etl)
	}
	// metasync
	debug.Assert(clone._sgl != nil)
	_ = p.metasyncer.sync(pairs...)

	// synchronize IC tables
	p.syncNewICOwners(ctx.smap, clone)
}

//
// all nodes except primary
//

// (primary forceJoin() calling)
// TODO: refactor as 2PC begin--abort|commit
func (h *htrun) prepForceJoin(w http.ResponseWriter, r *http.Request) {
	const (
		tag = "prep-force-join"
	)
	// caller
	var (
		callerID = r.Header.Get(apc.HdrCallerID)
		smap     = h.owner.smap.get()
		psi      = smap.GetNode(callerID)
	)
	// msg and query params
	q := r.URL.Query()
	prepare, err := cos.ParseBool(q.Get(apc.QparamPrepare))
	if err != nil {
		err := fmt.Errorf("failed to parse %q query: %v", apc.QparamPrepare, err)
		h.writeErr(w, r, err)
		return
	}
	msg, err := h.readAisMsg(w, r)
	if err != nil {
		return
	}

	// TODO -- FIXME: refactor two methods
	if prepare {
		if !smap.IsPrimary(psi) {
			h.writeErrf(w, r, "%s: expecting %s call from primary, got %q", h, tag, callerID)
			return
		}

		newSmap := &smapX{} // destination cluster's Smap
		if err := cos.MorphMarshal(msg.Value, newSmap); err != nil {
			h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h.si, msg.Action, msg.Value, err)
			return
		}

		// health handshake
		var (
			npsi = newSmap.Primary
			tout = cmn.Rom.CplaneOperation()
		)
		if _, code, err := h.reqHealth(npsi, tout, nil, newSmap /* -> header */, true /*retry pub-addr*/); err != nil {
			err = fmt.Errorf("%s: failed to reach %s, err: %v(%d)", tag, npsi.StringEx(), err, code)
			h.writeErr(w, r, err)
		}

		nlog.Infoln(h.String(), "prepare done", msg.String())
		return
	}

	// commit
	ncm := &cluMeta{}
	if err := cos.MorphMarshal(msg.Value, ncm); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h.si, msg.Action, msg.Value, err)
		return
	}

	// 7. update cluMeta in memory (= destination)
	if err = cmn.GCO.Update(&ncm.Config.ClusterConfig); err != nil {
		// rollback #1
		nlog.Errorln(msg.String(), "rollback #1", err)
		h.writeErr(w, r, err)
		return
	}

	h.owner.bmd.put(ncm.BMD)
	h.owner.rmd.put(ncm.RMD)

	npname := ncm.Smap.Primary.StringEx()
	nlog.Infoln(msg.String(), "(8) join self ->", npname)

	// 8. join self (up to 3 attempts)
	npsi := ncm.Smap.Primary
	joinURL, secondURL := npsi.ControlNet.URL, npsi.PubNet.URL
	h.owner.smap.put(ncm.Smap)
	res := h.regTo(joinURL, npsi, apc.DefaultTimeout, nil, false /*keepalive*/)
	e, eh := res.err, res.toErr()
	freeCR(res)
	if e != nil {
		if joinURL != secondURL {
			time.Sleep(time.Second)
			nlog.Errorln(res.toErr(), "- 2nd attempt via", secondURL)
			res = h.regTo(secondURL, npsi, apc.DefaultTimeout, nil, false)
			eh = res.toErr()
			freeCR(res)
		}
	}
	if eh != nil {
		h.writeErrf(w, r, "failed to join %s: %v", npname, err) // FATAL
		return
	}
	nlog.Infoln(h.String(), "joined", npname, msg.String())
}
