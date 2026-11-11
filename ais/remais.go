// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

// remote AIS clusters (remais):
// - target: applies attach/detach and other remais-related config changes via ais/backend,
//   and records the applied config version
// - proxy: keeps local cache, and refreshes it from a random target
//
// TODO:
// * URL change under an existing alias (persistent set-config) skips refresh on every node:
//   - EqualRemAIS compares aliases and ignores their URL lists
//   - the same check gates t.remaisConfig and the proxy-side refresh
// * removing (via set-config) the ais backend key leaves proxy caches stale:
//   - t.remaisConfig installs an empty backend with appliedCfgVer == 0
//   - proxies wait for the requested version, exhaust retries, and keep the removed clusters
// * refresh gives up after remaisRetries attempts, with nothing to re-trigger it:
//   - consider validating cached URL against cluster config in reverseRemAis (the UUID path)
// * remaisRefreshOnce returns empty-handed when a background refresh is in flight
// * applied config version does not capture content:
//   - target skips a remote that is offline at apply time, and still records the version
// * qcluRemais: one failing node fails the entire query; consider partial results
// * return the current version and/or error
// * make remais.Ver atomic

//
// proxy ------------------------------------------------------------------------------
//

type remaisCache struct {
	old []*meta.RemAis // to facilitate a2u resolution (and, therefore, offline access)
	meta.RemAisVec
	mu   sync.RWMutex
	in   atomic.Bool
	want int64 // newest requested backend config version, protected by mu
}

//
// attach, detach
//

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
	if _, err := p.owner.config.modify(ctx); err != nil {
		p.writeErr(w, r, err)
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

//
// resolve
//

func (p *proxy) a2u(aliasOrUUID string) string {
	p.remais.mu.RLock()
	for _, remais := range p.remais.A {
		if aliasOrUUID == remais.Alias || aliasOrUUID == remais.UUID {
			p.remais.mu.RUnlock()
			return remais.UUID
		}
	}
	l := len(p.remais.old)
	for i := l - 1; i >= 0; i-- {
		remais := p.remais.old[i]
		if aliasOrUUID == remais.Alias || aliasOrUUID == remais.UUID {
			p.remais.mu.RUnlock()
			return remais.UUID
		}
	}
	p.remais.mu.RUnlock()
	return aliasOrUUID
}

// Fixup local p.remais cache in the unlikely case it hasn't been populated yet via regular flows:
// - when a proxy starts up (or restarts), p.remais cache is populated asynchronously via remaisRefresh() goroutine
// - if a request for a remote-ais bucket arrives before the latter completes, p.a2u() (below)
//   fails to resolve alias to UUID

func (p *proxy) remaisVersionFixup() bool {
	// NOTE: benign race (monotonic); TODO: consider atomic.Int64
	if p.remais.Ver != 0 {
		return true
	}
	var ver int64
	p.remais.mu.RLock()
	ver = p.remais.Ver
	p.remais.mu.RUnlock()
	if ver != 0 {
		return true
	}

	config := cmn.GCO.Get()
	uptime := p.keepalive.cluUptime(mono.NanoTime())
	if uptime == 0 || uptime > cos.ClampDuration(config.Timeout.JoinTimeout(), time.Minute, 10*time.Minute) {
		return false
	}
	conf := config.Backend.Get(apc.AIS)
	if conf == nil {
		return false
	}
	aisConf, ok := conf.(cmn.BackendConfAIS)
	debug.Assertf(ok, "aisConf %T", conf)
	if !ok || len(aisConf) == 0 {
		return false
	}

	nlog.Warningln(p.String(), "remais fixup (starting up w/ remaisRefresh() delayed?)")
	p.remaisRefreshOnce(&config.ClusterConfig)
	return true
}

//
// query
//

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

// broadcast to get remote clusters as currently seen by each node, self included
func (p *proxy) qcluRemais(w http.ResponseWriter, r *http.Request, what string) {
	smap := p.owner.smap.get()
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathDae.S,
		Query:  url.Values{apc.QparamWhat: []string{apc.WhatRemoteAIS}},
	}
	args.smap = smap
	args.timeout = cmn.Rom.CplaneOperation()
	args.to = core.AllNodes
	args.cresv = cresjGeneric[meta.RemAisVec]{}
	results := p.bcastGroup(args)
	freeBcArgs(args)

	out := &meta.RemAisNodes{
		Proxy:  make(map[string]*meta.RemAisVec, smap.CountProxies()),
		Target: make(map[string]*meta.RemAisVec, smap.CountTargets()),
	}
	for _, res := range results {
		if res.err != nil {
			err := res.toErr()
			freeBcastRes(results)
			p.writeErr(w, r, err)
			return
		}
		all := res.v.(*meta.RemAisVec)
		if res.si.IsProxy() {
			out.Proxy[res.si.ID()] = all
		} else {
			out.Target[res.si.ID()] = all
		}
	}
	freeBcastRes(results)
	out.Proxy[p.SID()] = p._remaisCopy()
	p.writeJSON(w, r, out, what)
}

//
// refresh
//

// refresh local p.remais cache via intra-cluster call to a random target:
// - remaisRefresh:     upon config change; asynchronous, with retries
// - remaisRefreshOnce: upon cache miss; synchronous, single attempt
// NOTE:
// - best-effort: provides eventual consistency for remote-AIS metadata

// record the requested config version and retry until the cache catches up
func (p *proxy) remaisRefresh(newConfig *cmn.ClusterConfig) {
	p.remais.mu.Lock()
	p.remais.want = max(p.remais.want, newConfig.Version)
	p.remais.mu.Unlock()
	p._remaisRetry(newConfig)
}

// polling context: exponential backoff, from short up to cplane-operation timeout
// (remaisRetries attempts add up to about 5x the latter)
const remaisRetries = 7

type remaisPoll struct {
	sleep    time.Duration // next sleep
	maxsleep time.Duration
	i        int // current attempt, 1-based
}

func (poll *remaisPoll) init(p *proxy, config *cmn.ClusterConfig) {
	poll.maxsleep = config.Timeout.CplaneOperation.D()
	poll.sleep = min(cos.PollSleepMedium, poll.maxsleep)
	// starting up
	if maxkalive := config.Timeout.MaxKeepalive.D(); p.keepalive.cluUptime(mono.NanoTime()) < maxkalive {
		poll.sleep = 2 * maxkalive
	}
}

// sleep and back off; return false when out of attempts
func (poll *remaisPoll) next() bool {
	if poll.i >= remaisRetries {
		return false
	}
	poll.i++
	time.Sleep(poll.sleep)
	poll.sleep = min(2*poll.sleep, poll.maxsleep)
	return true
}

func (poll *remaisPoll) left() int       { return remaisRetries - poll.i }
func (poll *remaisPoll) backedOff() bool { return poll.sleep == poll.maxsleep }

func (p *proxy) _remaisRetry(config *cmn.ClusterConfig) {
	if !p.remais.in.CAS(false, true) {
		return
	}
	p.remais.mu.RLock()
	over, requested := p.remais.Ver, p.remais.want
	p.remais.mu.RUnlock()

	var (
		warn string
		poll remaisPoll
	)
	poll.init(p, config)
	for poll.next() {
		all, err := p.getRemAisVec(false /*refresh*/)
		if err != nil {
			if poll.i > 1 {
				nlog.Errorf("%s: failed to get remais (%d attempts)", p, poll.left())
			}
			continue
		}
		p.remais.mu.Lock()
		if want := p.remais.want; all.Ver < want {
			p.remais.mu.Unlock()
			// expected early on: targets may still be applying
			if poll.backedOff() {
				nlog.Warningf("%s: remais v%d, waiting for v%d (%d attempts)", p, all.Ver, want, poll.left())
			}
			continue
		}
		warn = p._remaisAdopt(all)
		p.remais.mu.Unlock()
		break
	}

	ver, want := p._remaisDone()
	if warn != "" {
		nlog.Warningln(warn)
	}
	nlog.Infof("%s: remais v%d => v%d", p, over, ver)

	// a newer request that arrived in the meantime
	if ver < want && want > requested {
		go p._remaisRetry(config)
	}
}

// adopt whatever is newer than the cached version, without waiting for the requested one
func (p *proxy) remaisRefreshOnce(config *cmn.ClusterConfig) {
	if !p.remais.in.CAS(false, true) {
		return
	}
	var (
		warn     string
		over     int64
		all, err = p.getRemAisVec(false /*refresh*/)
	)
	if err == nil {
		p.remais.mu.Lock()
		over = p.remais.Ver
		warn = p._remaisAdopt(all)
		p.remais.mu.Unlock()
	}

	ver, want := p._remaisDone()
	if err != nil {
		nlog.Errorf("%s: failed to get remais: %v", p, err)
	} else {
		if warn != "" {
			nlog.Warningln(warn)
		}
		nlog.Infof("%s: remais v%d => v%d", p, over, ver)
	}

	// catch up in the background
	if ver < want {
		go p._remaisRetry(config)
	}
}

// update the cache if `all` is newer (is called under lock);
// keep old/detached clusters to support access to existing ("cached") buckets,
// i.e., the ability to resolve remote alias to Ns.UUID (see p.a2u)
func (p *proxy) _remaisAdopt(all *meta.RemAisVec) (warn string) {
	if p.remais.Ver >= all.Ver {
		return ""
	}
	for _, a := range p.remais.RemAisVec.A {
		var found bool
		for _, b := range p.remais.old {
			if b.UUID == a.UUID {
				*b = *a
				found = true
				break
			}
			if b.Alias == a.Alias {
				warn = fmt.Sprintf("duplicated remais alias: (%q, %q) vs (%q, %q)", a.UUID, a.Alias, b.UUID, b.Alias)
			}
		}
		if !found {
			p.remais.old = append(p.remais.old, a)
		}
	}
	p.remais.RemAisVec = *all
	return warn
}

func (p *proxy) _remaisCopy() *meta.RemAisVec {
	p.remais.mu.RLock()
	all := &meta.RemAisVec{Ver: p.remais.Ver, A: make([]*meta.RemAis, len(p.remais.A))}
	for i, rem := range p.remais.A {
		entry := *rem
		all.A[i] = &entry
	}
	p.remais.mu.RUnlock()
	return all
}

// release, and only then return the cached and the requested versions
// (the order makes sure that a concurrent request is not lost)
func (p *proxy) _remaisDone() (ver, want int64) {
	p.remais.in.Store(false)
	p.remais.mu.RLock()
	ver, want = p.remais.Ver, p.remais.want
	p.remais.mu.RUnlock()
	return ver, want
}

//
// target --------------------------------------------------------------------------------
//

// apply remais-related config changes, if any
func (t *target) remaisConfig(oldConfig *cmn.Config, newConfig *globalConfig, msg *actMsgExt) error {
	if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
		return t.attachDetachRemAis(newConfig, msg)
	}

	if newConfig.Backend.EqualRemAIS(&oldConfig.Backend, t.String()) {
		return nil
	}
	if aisConf := newConfig.Backend.Get(apc.AIS); aisConf != nil {
		return t.attachDetachRemAis(newConfig, msg)
	}
	aisbp := backend.NewAIS(t, t.statsT, false)
	t.bps[apc.AIS] = aisbp
	t.rlbps[apc.AIS] = &rlbackend{Backend: aisbp, t: t}

	return nil
}

// NOTE: apply the entire config: add new and update existing entries (remote clusters)
func (t *target) attachDetachRemAis(newConfig *globalConfig, msg *actMsgExt) (err error) {
	var (
		aisbp   *backend.AISbp
		aisConf = newConfig.Backend.Get(apc.AIS)
	)
	debug.Assert(aisConf != nil)
	aisbp = t.aisbp()
	return aisbp.Apply(aisConf, msg.Action, &newConfig.ClusterConfig)
}

// return in-memory info; when `refresh`, also check connectivity and get remote Smap(s)
func (t *target) remaisInfo(refresh bool) meta.RemAisVec {
	aisbp := t.aisbp()
	if !refresh {
		return aisbp.GetInfoInternal()
	}
	anyConf := cmn.GCO.Get().Backend.Get(apc.AIS)
	if anyConf == nil {
		return meta.RemAisVec{}
	}
	aisConf, ok := anyConf.(cmn.BackendConfAIS)
	debug.Assert(ok)

	return aisbp.GetInfo(aisConf)
}
