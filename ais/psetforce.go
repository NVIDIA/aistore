// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

// Set new primary, including:
// - simple re-designation within the same cluster (b)
// - joining entire (split-brain) cluster (a)
// The latter is advanced usage only!

//
// cluster -------------------------------------------------------------------
//

// validation followed by either a) or b) below
func (p *proxy) cluSetPrimary(w http.ResponseWriter, r *http.Request) {
	const (
		warnInProgress = "setting new primary is already in progress"
	)
	apiItems, err := p.parseURL(w, r, apc.URLPathCluProxy.L, 1, false)
	if err != nil {
		return
	}
	smap := p.owner.smap.get()
	if err := smap.validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if !smap.IsPrimary(p.si) {
		err := fmt.Errorf("%s is (already?) not primary, please review cluster map for changes (%s) and try again", p, smap.StringEx())
		debug.AssertNoErr(err)
		p.writeErr(w, r, err)
		return
	}
	npid := apiItems[0]
	if npid == p.SID() {
		debug.Assert(p.SID() == smap.Primary.ID())
		nlog.Warningln(p.String(), "(self) is already primary, nothing to do")
		return
	}

	npsi := smap.GetProxy(npid)

	// a) another cluster, with force
	if npsi == nil {
		q := r.URL.Query()
		if !cos.IsParseBool(q.Get(apc.QparamForce)) {
			msg := fmt.Sprintf("set-primary failure ('%s' query is false):", apc.QparamForce)
			err := &errNodeNotFound{msg: msg, id: npid, si: p.si, smap: smap}
			p.writeErr(w, r, err, http.StatusNotFound)
			return
		}
		newPrimaryURL := q.Get(apc.QparamPrimaryCandidate)
		if newPrimaryURL == "" {
			msg := fmt.Sprintf("set-primary failure ('%s' query is empty):", apc.QparamPrimaryCandidate)
			err := &errNodeNotFound{msg: msg, id: npid, si: p.si, smap: smap}
			p.writeErr(w, r, err, http.StatusNotFound)
			return
		}
		if !p.settingNewPrimary.CAS(false, true) {
			p.writeErr(w, r, errors.New(warnInProgress+", cannot use force"))
			return
		}
		// join with force
		// main method, more checks inside
		p.forceJoin(w, r, npid, q)
		p.settingNewPrimary.Store(false)
		return
	}

	// b) regular 'set-primary', same cluster
	if err := _checkFlags(npsi); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if p.settingNewPrimary.CAS(false, true) {
		p._setPrimary(w, r, npsi)
		p.settingNewPrimary.Store(false)
	} else {
		nlog.Warningln(warnInProgress)
	}
}

func _checkFlags(npsi *meta.Snode) error {
	switch {
	case npsi.InMaintOrDecomm():
		s := "under maintenance"
		if !npsi.InMaint() {
			s = "being decommissioned"
		}
		return errors.New(npsi.StringEx() + " cannot become new primary as it is currently " + s)
	case npsi.Flags.IsSet(meta.SnodeNonElectable):
		return errors.New(npsi.StringEx() + " is non-electable and cannot become new primary")
	default:
		return nil
	}
}

func (p *proxy) _setPrimary(w http.ResponseWriter, r *http.Request, npsi *meta.Snode) {
	//
	// (I.1) Prepare phase - inform other nodes.
	//
	var (
		urlPath = apc.URLPathDaeProxy.Join(npsi.ID())
		q       = url.Values{apc.QparamPrepare: []string{"true"}}
		args    = allocBcArgs()
	)
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
	var (
		smap          = p.owner.smap.get()
		psi           = smap.GetProxy(npid)
		newPrimaryURL = q.Get(apc.QparamPrimaryCandidate)
		nurl          = newPrimaryURL
	)
	if nurl == "" {
		// (daeForceJoin => forceJoin) use case must be revisited
		nurl = cos.Left(psi.ControlNet.URL, psi.PubNet.URL)
	}
	if nurl == "" {
		err := fmt.Errorf("cannot %s %s (and cluster %s) to %s[%s]: missing destination URL", act, p, smap, tag, npid)
		p.writeErr(w, r, err)
		return
	}

	// 2. the first inter-cluster network call:
	// get and validate destination Smap, henceforth nsmap
	nsmap, ern := p._getSmapCheckReady(nurl)
	if ern != nil && _destinationNotReady(ern) {
		p.writeErr(w, r, ern)
		return
	}
	if ern != nil && psi != nil && psi.PubNet.URL != psi.ControlNet.URL {
		nlog.Errorln(ern)
		nsmap, ern = p._getSmapCheckReady(psi.PubNet.URL)
	}
	if ern != nil {
		err := fmt.Errorf("%s %s to %s[%q, %q]: %s", act, p, tag, npid, newPrimaryURL, ern)
		p.writeErr(w, r, err)
		return
	}
	if err := nsmap.validate(); err != nil {
		err := fmt.Errorf("%s: destination %s is invalid [%v] - cannot %s", p, nsmap.StringEx(), err, act)
		p.writeErr(w, r, err)
		return
	}
	if !cos.IsValidUUID(smap.UUID) || !cos.IsValidUUID(nsmap.UUID) {
		err := fmt.Errorf("%s: local %s and/or remote %s have invalid UUID(s)", p, smap.StringEx(), nsmap.StringEx()) // (unlikely)
		debug.AssertNoErr(err)
		p.writeErr(w, r, err)
		return
	}

	var (
		npsi   = nsmap.Primary
		npname = npsi.StringEx()
	)
	if npid != npsi.ID() {
		err := fmt.Errorf("%s: according to the destination %s %s %s is not _the_ primary", p, nsmap.StringEx(), tag, npname)
		p.writeErr(w, r, err)
		return
	}
	if nurl != npsi.ControlNet.URL {
		// must be reachable via its own `nsmap`, preferably via control net
		if _, e := p._getSmapCheckReady(npsi.ControlNet.URL); e != nil {
			nlog.Warningln(tag, npname, "is not reachable via its ctrl URL", npsi.ControlNet.URL)
			//
			// TODO: require "double-force" or "super-force" flag to proceed
			//
			if _, e = p._getSmapCheckReady(npsi.PubNet.URL); e != nil {
				err := fmt.Errorf("%s: %s %s is no reachable via both (pub %q, ctrl %q) URLs", p, tag, npname, npsi.PubNet.URL, npsi.ControlNet.URL)
				p.writeErr(w, r, err)
				return
			}
		}
	}
	if err := _checkFlags(npsi); err != nil {
		p.writeErr(w, r, err)
		return
	}

	//
	// 3. begin
	//
	what := "(split-brain):"
	if smap.UUID != nsmap.UUID {
		what = "a different cluster:"
	}
	nlog.Warningln(act, "entire cluster [", p.String(), smap.StringEx(), "] to", what, "\n\t[", npname, nsmap.StringEx(), "]")

	// 4. get destination cluMeta from npsi
	cargs := allocCargs()
	{
		cargs.si = npsi
		cargs.timeout = cmn.Rom.MaxKeepalive()
		cargs.req = cmn.HreqArgs{Path: apc.URLPathDae.S, Query: url.Values{apc.QparamWhat: []string{apc.WhatSmapVote}}}
		cargs.cresv = cresjGeneric[cluMeta]{}
	}
	res := p.call(cargs, nsmap /* -> header */)
	eri := res.unwrap()
	freeCargs(cargs)
	if eri != nil {
		freeCR(res)
		p.writeErr(w, r, eri)
		return
	}
	ncm, ok := res.v.(*cluMeta)
	debug.Assert(ok)
	freeCR(res)

	if err := ncm.validate(); err != nil {
		p.writeErrf(w, r, "cannot %s %s(self, primary) -> %s: destination cluMeta [%v]", act, p, npname, err)
		return
	}
	if ncm.Smap.UUID != nsmap.UUID {
		// (unlikely)
		err := fmt.Errorf("cannot %s %s(self, primary) -> %s: unexpected UUID mismatch (%q vs %q)", act, p, npname,
			ncm.Smap.StringEx(), nsmap.StringEx())
		debug.AssertNoErr(err)
		p.writeErr(w, r, err)
		return
	}
	if ncm.Smap.Version != nsmap.Version {
		debug.Assert(ncm.Smap.Version > nsmap.Version, ncm.Smap.String(), " vs ", nsmap.String())
		p.writeErrf(w, r, "cannot %s %s(self, primary) -> %s: detected version change (%s vs %s) at the destination",
			act, p, npname, ncm.Smap.String(), nsmap.String())
	}

	// 5. backup (see rollback below)
	cm, erj := p.cluMeta(cmetaFillOpt{skipPrimeTime: true})
	if erj != nil {
		p.writeErrf(w, r, "cannot %s %s to %s: %v", act, p, npname, erj) // (unlikely)
		return
	}
	if err := cm.validate(); err != nil {
		// must itself be in the right state
		p.writeErrf(w, r, "cannot %s %s(self, primary) -> %s: local cluMeta [%v]", act, p, npname, err)
		return
	}

	// 6. prepare phase whereby all members health-ping => npsi (see `daeForceJoin`)
	nlog.Infoln(act, "(6) prepare")
	p._bcastPrepForceJoin(w, r, nsmap, npname)

	aimsg := p.newAmsgActVal(apc.ActPrimaryForce, nil)

	// 7. update cluMeta in memory (= destination)
	nlog.Infoln(act, "(7) update clu-meta in mem")
	if err := cmn.GCO.Update(&ncm.Config.ClusterConfig); err != nil {
		// rollback #1
		nlog.Errorln(act, "rollback #1", err)
		cm.metasync(p, aimsg, true)
		p.writeErr(w, r, err)
		return
	}
	p.owner.bmd.put(ncm.BMD)
	p.owner.rmd.put(ncm.RMD)
	p.owner.smap.put(nsmap)

	// 8. join self (up to 3 attempts)
	nlog.Infoln(act, "(8) join self ->", npname)
	if err := p._cluJoinSelf(npsi, nurl); err != nil {
		// rollback #2
		nlog.Errorln(act, "rollback #2", err)
		if nested := cmn.GCO.Update(&cm.Config.ClusterConfig); nested != nil {
			nlog.Errorf("FATAL: nested config-update error when rolling back [%s %s to %s]: %v",
				act, p, npname, nested) // (unlikely)
		}
		p.owner.smap.put(cm.Smap)
		p.owner.bmd.put(cm.BMD)
		p.owner.rmd.put(cm.RMD)

		p.writeErr(w, r, err)
		return
	}

	// 9. commit phase
	nlog.Infoln(act, "(9) commit")
	if !p._bcastCommitForceJoin(w, r, smap, ncm) {
		return
	}

	// point of no return
	p.metasyncer.becomeNonPrimary()
	time.Sleep(time.Second)

	// TODO -- FIXME: remove?
	// 10. finally, ask npsi to bump versions and metasync all (see `msyncForceAll`)
	const act2 = "\"bump\""
	nlog.Infoln(act, "(10) npsi to", act2, "metasync")
	cargs = allocCargs()
	msg := &apc.ActMsg{Action: apc.ActBumpMetasync, Value: float64(smap.Version)}
	{
		cargs.si = npsi
		cargs.timeout = cmn.Rom.MaxKeepalive()
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathClu.S, Body: cos.MustMarshal(msg)}
	}
	res = p.call(cargs, nsmap)
	err := res.unwrap()
	freeCR(res)
	if err != nil {
		freeCargs(cargs)
		// retry just once
		cargs = allocCargs()
		cargs.si = npsi
		cargs.req = cmn.HreqArgs{Method: http.MethodPut, Base: npsi.PubNet.URL, Path: apc.URLPathClu.S, Body: cos.MustMarshal(msg)}
		cargs.timeout = cmn.Rom.MaxKeepalive() + time.Second

		nlog.Errorln(err, "- failed to", act2, "metasync, retrying...")
		res := p.call(cargs, nsmap)
		err = res.unwrap()
		freeCR(res)
	}
	if err != nil {
		p.writeErrf(w, r, "%s: failed to %s metasync => %s: %v", p, act2, npname, err)
	}
	freeCargs(cargs)
}

// TODO: introduce error code or http status code
func _destinationNotReady(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "ready") || strings.Contains(msg, "rebalance")
}

func (p *proxy) _getSmapCheckReady(baseURL string) (smap *smapX, err error) {
	cargs := allocCargs()
	{
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Base:   baseURL,
			Path:   apc.URLPathDae.S,
			Header: http.Header{apc.HdrReadyToJoinClu: []string{"true"}},
			Query:  url.Values{apc.QparamWhat: []string{apc.WhatSmap}},
		}
		cargs.timeout = apc.DefaultTimeout
		cargs.cresv = cresjGeneric[smapX]{} // -> smapX
	}
	res := p.call(cargs, p.owner.smap.get())
	if res.err != nil {
		err = res.err
		if !_destinationNotReady(err) {
			err = res.errorf("failed to get Smap from %s", baseURL)
		}
	} else {
		smap = res.v.(*smapX)
		if err = smap.validate(); err != nil {
			debug.AssertNoErr(err)
			err = fmt.Errorf("%s: invalid %s from %s: %v", p, smap, baseURL, err)
			smap = nil
		}
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

// (see _commitForceJoin counterpart)
func (p *proxy) _cluJoinSelf(npsi *meta.Snode, nurl string) error {
	joinURL, secondURL := npsi.ControlNet.URL, npsi.PubNet.URL
	if nurl == npsi.PubNet.URL {
		// (swap to maybe reflect user preference)
		joinURL, secondURL = npsi.PubNet.URL, npsi.ControlNet.URL
	}
	res := p.regTo(joinURL, npsi, apc.DefaultTimeout, nil, false /*keepalive*/)
	e, eh := res.err, res.toErr()
	freeCR(res)
	if e == nil {
		return nil
	}
	nlog.Errorln(res.toErr())
	if joinURL != secondURL {
		nlog.Warningln("2nd attempt via", secondURL)
		runtime.Gosched()
		res = p.regTo(secondURL, npsi, apc.DefaultTimeout, nil, false)
		e, eh = res.err, res.toErr()
		freeCR(res)
	}
	if e == nil {
		return nil
	}
	nlog.Warningln(eh, "- the 3d (and final) attempt via", joinURL)
	time.Sleep(time.Second)
	res = p.regTo(joinURL, npsi, apc.DefaultTimeout, nil, false)
	eh = res.toErr()
	freeCR(res)
	return eh
}

// (see _prepForceJoin counterpart)
func (p *proxy) _bcastPrepForceJoin(w http.ResponseWriter, r *http.Request, nsmap *smapX, npname string) bool /*ok*/ {
	bargs := allocBcArgs()
	{
		aimsg := p.newAmsgActVal(apc.ActPrimaryForce, nsmap)
		bargs.req = cmn.HreqArgs{
			Method: http.MethodPost,
			Path:   apc.URLPathDaeForceJoin.S,
			Query:  url.Values{apc.QparamPrepare: []string{"true"}},
			Body:   cos.MustMarshal(aimsg),
		}
		bargs.to = core.AllNodes
	}
	results := p.bcastGroup(bargs)
	freeBcArgs(bargs)
	for _, res := range results {
		if res.err != nil {
			p.writeErrf(w, r, "node %s failed to contact new primary %s in the prepare phase (err: %v)",
				res.si, npname, res.err)
			freeBcastRes(results)
			return false
		}
	}
	freeBcastRes(results)
	return true
}

// TODO: rewrite as begin/(commit|abort)
// see _commitForceJoin counterpart
func (p *proxy) _bcastCommitForceJoin(w http.ResponseWriter, r *http.Request, smap *smapX, ncm *cluMeta) bool /*ok*/ {
	if smap.CountActiveTs() == 0 && smap.CountActivePs() == 1 {
		return true // nothing to do
	}
	bargs := allocBcArgs()
	{
		aimsg := p.newAmsgActVal(apc.ActPrimaryForce, ncm)
		bargs.req = cmn.HreqArgs{
			Method: http.MethodPost,
			Path:   apc.URLPathDaeForceJoin.S,
			Query:  url.Values{apc.QparamPrepare: []string{"false"}},
			Body:   cos.MustMarshal(aimsg),
		}
		bargs.to = core.SelectedNodes
		bargs.nodes = make([]meta.NodeMap, 0, 2)
		if len(smap.Tmap) > 0 {
			bargs.nodes = append(bargs.nodes, smap.Tmap)
		}
		if len(smap.Pmap) > 1 {
			bargs.nodes = append(bargs.nodes, smap.Pmap)
		}
	}
	debug.Assert(len(bargs.nodes) > 0)

	results := p.bcastGroup(bargs)
	freeBcArgs(bargs)
	for _, res := range results {
		if res.err != nil {
			p.writeErrf(w, r, "%s failed commit: [%v]", res.si, res.err)
			freeBcastRes(results)

			// TODO -- FIXME: rollback #3
			return false
		}
	}
	freeBcastRes(results)
	return true
}

//
// destination (designated) primary
//

func (p *proxy) msyncForceAll(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	cm, err := p.cluMeta(cmetaFillOpt{})
	if err != nil {
		debug.AssertNoErr(err)
		p.writeErr(w, r, err) // (unlikely)
		return
	}
	var (
		victimVer int64
		v, ok     = msg.Value.(float64)
	)
	if !ok {
		nlog.Errorf("unexpected \"victim-version\" type (%v, %T)", msg.Value, msg.Value)
	} else {
		victimVer = int64(v)
	}

	// 1. Smap (special care)
	nver := max(cm.Smap.Version, victimVer) + 100
	nlog.Warningln("metasync \"bumped\" Smap", cm.Smap.Version, "=>", nver)
	sctx := &smapModifier{
		pre: func(_ *smapModifier, clone *smapX) error { clone.Version = nver; return nil },
		final: func(_ *smapModifier, clone *smapX) {
			aimsg := p.newAmsgActVal(apc.ActPrimaryForce, msg)
			wg := p.metasyncer.sync(revsPair{clone, aimsg})
			wg.Wait()
		},
	}
	err = p.owner.smap.modify(sctx)
	if err != nil {
		debug.AssertNoErr(err)
		p.writeErr(w, r, err) // FATAL
		return
	}

	// 2. the rest, with msg=force
	cm, err = p.cluMeta(cmetaFillOpt{})
	if err != nil {
		debug.AssertNoErr(err)
		p.writeErr(w, r, err) // (unlikely)
		return
	}
	if cm.Smap.Version < nver {
		debug.Assert(false, cm.Smap.Version, " vs ", nver)
		nlog.Errorf("invalid %s version (expected >= %d post-modifier)", cm.Smap.StringEx(), nver) // (unlikely)
	}
	aimsg := p.newAmsgActVal(apc.ActPrimaryForce, nil)
	cm.metasync(p, aimsg, false)
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
	nlog.Infof("%s: distributing (%s, %s, %s) with newly elected or selected primary (self)", p, clone, bmd, rmd)
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
// all nodes except primary -----------------------------------------------------------------------
//

// (primary forceJoin() calling)
// TODO: refactor as 2PC begin--abort|commit
func (h *htrun) daeForceJoin(w http.ResponseWriter, r *http.Request) {
	// msg and query params
	q := r.URL.Query()
	prepare, err := cos.ParseBool(q.Get(apc.QparamPrepare))
	if err != nil {
		err := fmt.Errorf("failed to parse %q query: %w", apc.QparamPrepare, err)
		h.writeErr(w, r, err)
		return
	}
	msg, err := h.readAisMsg(w, r)
	if err != nil {
		return
	}

	// TODO -- FIXME: refactor two methods
	if prepare {
		h._prepForceJoin(w, r, msg)
	} else {
		h._commitForceJoin(w, r, msg)
	}
}

func (h *htrun) _prepForceJoin(w http.ResponseWriter, r *http.Request, msg *actMsgExt) {
	const tag = "prep-force-join:"
	var (
		callerID = r.Header.Get(apc.HdrCallerID)
		smap     = h.owner.smap.get()
		psi      = smap.GetNode(callerID)
	)
	if !smap.IsPrimary(psi) {
		h.writeErrf(w, r, "%s expecting %s call from primary, got %q", h, tag, callerID)
		return
	}

	nsmap := &smapX{} // destination cluster's Smap
	if err := cos.MorphMarshal(msg.Value, nsmap); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h.si, msg.Action, msg.Value, err)
		return
	}

	// handshake designated npsi
	var (
		npsi   = nsmap.Primary
		npname = npsi.StringEx()
		tout   = cmn.Rom.CplaneOperation()
	)
	if _, code, err := h.reqHealth(npsi, tout, nil, nsmap /* -> header */, true /*retry pub-addr*/); err != nil {
		err = fmt.Errorf("%s failed to req-health %s, err: %w(%d)", tag, npname, err, code)
		h.writeErr(w, r, err)
		return
	}
	nlog.Infoln(tag, h.String(), smap.StringEx(), "-> [", npname, nsmap.StringEx(), "]")
}

func (h *htrun) _commitForceJoin(w http.ResponseWriter, r *http.Request, msg *actMsgExt) {
	const tag = "commit-force-join:"

	ncm := &cluMeta{}
	if err := cos.MorphMarshal(msg.Value, ncm); err != nil {
		h.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, h.si, msg.Action, msg.Value, err)
		return
	}
	nsmap := ncm.Smap
	if err := nsmap.validate(); err != nil {
		debug.AssertNoErr(err)
		h.writeErr(w, r, err)
		return
	}

	// update cluMeta in mem (= destination, brute force)
	nconfig := &ncm.Config.ClusterConfig
	if err := cmn.GCO.Update(nconfig); err != nil {
		err = fmt.Errorf("%s failed to update config %s: %w", tag, nconfig.String(), err)
		debug.AssertNoErr(err)
		h.writeErr(w, r, err)
		return
	}
	h.owner.bmd.put(ncm.BMD)
	h.owner.rmd.put(ncm.RMD)
	h.owner.smap.put(nsmap)

	// join self
	var (
		npsi               = ncm.Smap.Primary
		npname             = npsi.StringEx()
		joinURL, secondURL = npsi.ControlNet.URL, npsi.PubNet.URL
	)
	nlog.Infoln(tag, h.String(), "(self) ->", npname, "[", msg.String(), nsmap.StringEx(), "]")

	res := h.regTo(joinURL, npsi, apc.DefaultTimeout, nil, false /*keepalive*/)
	eh := res.toErr()
	freeCR(res)
	if eh == nil {
		goto ok
	}
	// retry once
	if joinURL != secondURL {
		time.Sleep(time.Second)
		nlog.Errorln(tag, res.toErr(), "- 2nd attempt via", secondURL)
		res = h.regTo(secondURL, npsi, apc.DefaultTimeout, nil, false)
		eh = res.toErr()
		freeCR(res)
	}
	if eh != nil {
		h.writeErrf(w, r, "%s -> %s failed: %v", tag, npname, eh) // FATAL
		return
	}
ok:
	nlog.Infoln(tag, h.String(), "->", npname, "done")
}

//
// stray proxy (former primary, that is)
//

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

	// regular flow
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
		err := &errNodeNotFound{p.si, smap, "cannot set new primary", proxyID}
		p.writeErr(w, r, err)
		return
	}
	if prepare {
		if cmn.Rom.V(4, cos.ModAIS) {
			nlog.Infoln("Preparation step: do nothing")
		}
		return
	}
	ctx := &smapModifier{pre: func(_ *smapModifier, clone *smapX) error { clone.Primary = psi; return nil }}
	err = p.owner.smap.modify(ctx)
	debug.AssertNoErr(err)
}

/////////////
// cluMeta //
/////////////

// check essentials
func (cm *cluMeta) validate() error {
	if cm.Smap == nil {
		return errors.New("nil Smap")
	}
	if err := cm.Smap.validate(); err != nil {
		return err
	}
	if at, nt := cm.Smap.CountActiveTs(), cm.Smap.CountTargets(); at != nt {
		nlog.Errorf("Warning: inactive targets in %s: %d vs %d", cm.Smap.StringEx(), at, nt)
	}
	if ap, np := cm.Smap.CountActivePs(), cm.Smap.CountProxies(); ap != np {
		nlog.Errorf("Warning: inactive proxies in %s: %d vs %d", cm.Smap.StringEx(), ap, np)
	}
	if cm.BMD == nil || cm.BMD.version() == 0 {
		return errors.New("nil BMD")
	}
	if !cos.IsValidUUID(cm.BMD.UUID) {
		return fmt.Errorf("invalid %s UUID %q", cm.BMD.String(), cm.BMD.UUID)
	}
	if cm.BMD.UUID != cm.Smap.UUID {
		nlog.Errorf("Warning: %s and %s have different UUIDs: %q and %q, respectively", cm.Smap, cm.BMD, cm.Smap.UUID, cm.BMD.UUID)
	}
	if cm.Config == nil || cm.Config.version() == 0 {
		return errors.New("nil Config")
	}
	if !cos.IsValidUUID(cm.Config.UUID) {
		return errors.New("invalid Config UUID:" + cm.Config.String())
	}
	return nil
}

func (cm *cluMeta) metasync(p *proxy, msg *actMsgExt, wait bool) {
	var (
		detail string
		revs   = make([]revsPair, 0, 5)
	)
	if cm.Smap != nil && cm.Smap.isValid() {
		detail = cm.Smap.StringEx()
		revs = append(revs, revsPair{cm.Smap, msg})
	}
	if cm.BMD != nil && cm.BMD.version() > 0 && cos.IsValidUUID(cm.BMD.UUID) {
		revs = append(revs, revsPair{cm.BMD, msg})
	}
	if cm.Config != nil && cm.Config.version() > 0 && cos.IsValidUUID(cm.Config.UUID) {
		revs = append(revs, revsPair{cm.Config, msg})
	}
	if cm.RMD != nil {
		revs = append(revs, revsPair{cm.RMD, msg})
	}
	if cm.EtlMD != nil {
		revs = append(revs, revsPair{cm.EtlMD, msg})
	}
	nlog.InfoDepth(1, p.String(), "metasync-all", msg.Action, detail, len(revs))
	wg := p.metasyncer.sync(revs...)
	if wait {
		wg.Wait()
	}
}
