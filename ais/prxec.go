// Package ais provides AIStore's proxy and target nodes.
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
		p.ec.setActive(mono.NanoTime())
	case apc.ActEcClose:
		p.ec.setActive(0)
	default:
		p.writeErr(w, r, errActEc(action))
	}
}

// (target kalive => primary)
func (p *proxy) _recvActiveEC(hdr http.Header, now int64) { p.ec.recvKalive(hdr, now) }

// (primary kalive response => non-primary)
func (p *proxy) _respActiveEC(hdr http.Header, now int64) { p.ec.respKalive(hdr, now) }

//
// primary action: on | off
//

func (p *proxy) onEC(bck *meta.Bck) error {
	if !bck.Props.EC.Enabled || cmn.Rom.EcStreams() < 0 /* cmn.SharedStreamsEver */ {
		return nil
	}
	now := mono.NanoTime()
	debug.Assert(cmn.Rom.EcStreams() >= cmn.SharedStreamsMin, cmn.Rom.EcStreams(), " vs ", cmn.SharedStreamsMin)
	return p._onEC(now)
}

func (p *proxy) _onEC(now int64) error {
	last := p.ec.last.Load()
	if last != 0 && time.Duration(now-last) < cmn.SharedStreamsNack {
		return nil
	}
	err := p._toggleEC(apc.ActEcOpen)
	if err == nil {
		p.ec.setActive(mono.NanoTime())
	}
	return err
}

// bcast primary's control
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
			err := fmt.Errorf("%s: %s failed to %s: %v", p, res.si.StringEx(), action, res.err)
			return err
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

	err := p._toggleEC(apc.ActEcClose)
	if err == nil {
		return
	}

	nlog.Warningln(err) // benign (see errCloseStreams)

	// undo
	err = p._onEC(mono.NanoTime())
	if err != nil {
		nlog.Warningln("nested failure:", apc.ActEcClose, "--> undo:", err)
	}
}
