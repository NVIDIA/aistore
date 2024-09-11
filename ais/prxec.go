// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

func (p *proxy) ecHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.httpecpost(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

func (p *proxy) httpecpost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathEC.L, 1, false)
	if err != nil {
		return
	}
	action := apiItems[0]
	switch action {
	case apc.ActEcOpen:
		p._setActiveEC(mono.NanoTime())
	case apc.ActEcClose:
		p._setActiveEC(0)
	default:
		p.writeErr(w, r, errActEc(action))
	}
}

//
// (EC is active) and (is EC active?) via fastKalive
//

func isActiveEC(hdr http.Header) (ok bool) {
	_, ok = hdr[apc.HdrActiveEC]
	return ok
}

// (target kalive => primary)
func (p *proxy) _recvActiveEC(hdr http.Header, now int64) {
	if isActiveEC(hdr) {
		p._setActiveEC(now)
		return
	}
	// check if time has come to close (easy checks first)
	if p.ec.rust == 0 || time.Duration(now-p.ec.rust) < cmn.Rom.EcStreams() {
		return
	}
	last := p.ec.last.Load()
	if last == 0 || time.Duration(now-last) < cmn.Rom.EcStreams() {
		return
	}

	// NOTE: go ahead and close EC streams (with one last check inside)
	p.offEC(last)
}

func (p *proxy) _setActiveEC(now int64) {
	p.ec.last.Store(now)
	p.ec.rust = now
}

// (primary kalive response => non-primary)
func (p *proxy) _respActiveEC(hdr http.Header, now int64) {
	tout := cmn.Rom.EcStreams()
	last := p.ec.last.Load()
	if last != 0 && time.Duration(now-last) < tout {
		hdr.Set(apc.HdrActiveEC, "true")
	}
}

//
// primary action: on | off
//

const (
	ecStreamsNack = max(cmn.EcStreamsMini>>1, 3*time.Minute)
)

func (p *proxy) onEC(bck *meta.Bck) error {
	if !bck.Props.EC.Enabled || cmn.Rom.EcStreams() < 0 /* cmn.EcStreamsEver */ {
		return nil
	}
	now := mono.NanoTime()
	debug.Assert(cmn.Rom.EcStreams() >= cmn.EcStreamsMini, cmn.Rom.EcStreams(), " vs ", cmn.EcStreamsMini)
	if p.ec.rust != 0 && time.Duration(now-p.ec.rust) < ecStreamsNack {
		return nil
	}
	return p._onEC(now)
}

func (p *proxy) _onEC(now int64) error {
	last := p.ec.last.Load()
	if last != 0 && time.Duration(now-last) < ecStreamsNack {
		return nil
	}
	err := p._toggleEC(apc.ActEcOpen)
	if err == nil {
		p._setActiveEC(mono.NanoTime())
	}
	return err
}

func (p *proxy) _toggleEC(action string) error {
	// 1. targets
	args := allocBcArgs()
	{
		args.smap = p.owner.smap.get()
		args.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathEC.Join(action)}
		args.network = cmn.NetIntraControl
		args.timeout = cmn.Rom.CplaneOperation()
		args.nodes = []meta.NodeMap{args.smap.Tmap}
		args.nodeCount = len(args.smap.Tmap)
	}
	results := p.bcastNodes(args)

	for _, res := range results {
		if res.err != nil {
			freeBcArgs(args)
			return fmt.Errorf("%s: %s failed to %s: %v", p, res.si.StringEx(), action, res.err)
		}
	}

	// 2. proxies, upon success
	if args.nodeCount = len(args.smap.Pmap) - 1; args.nodeCount == 0 {
		goto ex
	}
	args.nodes = []meta.NodeMap{args.smap.Pmap}
	freeBcastRes(results)
	results = p.bcastNodes(args)
	for _, res := range results {
		if res.err != nil {
			// NOTE: warn, ignore
			nlog.Warningln("action:", action, "proxy:", res.si.StringEx(), "failed to get notified:", res.err)
		}
	}

	nlog.Infoln(p.String(), "toggle:", action)
ex:
	freeBcArgs(args)
	freeBcastRes(results)
	return nil
}

func (p *proxy) offEC(last int64) {
	if !p.ec.last.CAS(last, 0) {
		return
	}
	p.ec.rust = 0

	if err := p._toggleEC(apc.ActEcClose); err == nil {
		return
	}

	// undo
	p._onEC(mono.NanoTime())
}
