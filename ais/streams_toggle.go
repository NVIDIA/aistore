// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

// track cluster-wide “active” state refreshed by keep-alives and
// auto-deactivates after a timeout;
// negative timeout (see `tout` below) => never deactivate

// non-primary action
const (
	nonpBit  = uint64(1) << 63
	nonpMask = ^nonpBit
)

type (
	streamsToggle struct {
		hdrActive string        // apc.HdrActiveEC, ...
		actOn     string        // apc.ActOpenEC, ...
		actOff    string        // apc.ActCloseEC, ...
		last      atomic.Uint64 // mono-time of last positive refresh
	}

	ecToggle struct {
		streamsToggle
	}
	dmToggle struct {
		streamsToggle
	}
)

func (f *ecToggle) init() {
	f.hdrActive = apc.HdrActiveEC
	f.actOn = apc.ActOpenEC
	f.actOff = apc.ActCloseEC
}

func (*ecToggle) timeout() time.Duration { return cmn.Rom.EcStreams() }

func (f *dmToggle) init() {
	f.hdrActive = apc.HdrActiveDM
	f.actOn = apc.ActOpenSDM
	f.actOff = apc.ActCloseSDM
}

func (*dmToggle) timeout() time.Duration { return cmn.SharedStreamsDflt } // see also: sharedDM.getActive

func (f *streamsToggle) isActive(h http.Header) bool { _, ok := h[f.hdrActive]; return ok }

func (f *streamsToggle) setActive(now int64) {
	f.last.Store(uint64(now))
}

// non-primary: executed user call requiring SDM
func (f *streamsToggle) nonpSetActive(now int64) {
	debug.Assert(now > 0, "invalid timestamp: ", now)
	f.last.Store((uint64(now) & nonpMask) | nonpBit)
}

// non-primary: fast-keepalive => primary
func (f *streamsToggle) nonpResetActive() (uint64, bool) {
	last := f.last.Load()
	if last&nonpBit == 0 {
		return last, false
	}
	last = f.last.Swap(0)
	return last, true
}

// non-primary: fast-keepalive => primary
func (f *streamsToggle) nonpUndo(last uint64) {
	_ = f.last.CAS(0, last)
}

// target => primary keep-alive
func (f *streamsToggle) recvKalive(p *proxy, hdr http.Header, now int64, tout time.Duration) {
	if _, ok := hdr[f.hdrActive]; ok {
		f.setActive(now)
		return
	}
	if tout < 0 {
		return
	}
	last := int64(f.last.Load() & nonpMask)
	if last == 0 || time.Duration(now-last) < tout {
		return
	}
	f.off(p, last)
}

// primary => target keep-alive
func (f *streamsToggle) respKalive(hdr http.Header, now int64, tout time.Duration) {
	if tout > 0 {
		v := f.last.Load()
		last := int64(v & nonpMask)
		debug.Assert(int64(v) >= 0, f.hdrActive, " invalid timestamp: ", v)
		if last != 0 && time.Duration(now-last) < tout {
			hdr.Set(f.hdrActive, "true")
		}
	}
}

//
// primary action
//

func (f *streamsToggle) on(p *proxy, tout time.Duration) error {
	if tout < 0 /* cmn.SharedStreamsEver */ {
		return nil
	}
	var (
		now  = mono.NanoTime()
		last = int64(f.last.Load() & nonpMask)
	)
	if last != 0 && time.Duration(now-last) < cmn.SharedStreamsNack {
		return nil
	}
	err := p._toggleStreams(f.actOn)
	if err == nil {
		f.setActive(mono.NanoTime())
	}
	return err
}

func (f *streamsToggle) off(p *proxy, last int64) {
	debug.Assert(last > 0, f.hdrActive, " invalid timestamp: ", last)
	if !f.last.CAS(uint64(last), 0) {
		return
	}

	err := p._toggleStreams(f.actOff)
	if err == nil {
		return
	}

	nlog.WarningDepth(1, err) // benign (see errCloseStreams)

	// undo
	errN := p._toggleStreams(f.actOn)
	if errN != nil {
		nlog.WarningDepth(1, "nested failure:", f.actOff, "--> undo:", err, errN)
	} else {
		f.setActive(mono.NanoTime())
	}
}

// bcast primary's control: on/off shared streams
func (p *proxy) _toggleStreams(action string) error {
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
			err := fmt.Errorf("%s: %s failed to %s: %w", p, res.si.StringEx(), action, res.err)
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
