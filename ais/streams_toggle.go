// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
)

// Track cluster-wide "active" state refreshed by keep-alives.
// `last` is the mono-time of the most recent positive refresh; 0 => inactive/closed.
// A negative timeout (see `tout`) => never deactivate.

type (
	streamsToggle struct {
		hdrActive string       // apc.HdrActiveEC, ...
		actOn     string       // apc.ActOpenEC, ...
		actOff    string       // apc.ActCloseEC, ...
		last      atomic.Int64 // mono-time of last positive refresh (0 == inactive)

		mu         sync.Mutex  // serialize primary-side open/close
		offPending atomic.Bool // single-flight close worker
	}

	ecToggle struct {
		streamsToggle
	}
)

func (f *ecToggle) init() {
	f.hdrActive = apc.HdrActiveEC
	f.actOn = apc.ActOpenEC
	f.actOff = apc.ActCloseEC
}

func (*ecToggle) timeout() time.Duration { return cmn.Rom.EcStreams() }

func (f *streamsToggle) isActive(h http.Header) bool { _, ok := h[f.hdrActive]; return ok }

func (f *streamsToggle) setActive(now int64) { f.last.Store(now) }

// target => primary keep-alive
func (f *streamsToggle) recvKalive(p *proxy, hdr http.Header, now int64, tout time.Duration) {
	if _, ok := hdr[f.hdrActive]; ok {
		f.setActive(now)
		return
	}
	if tout < 0 {
		return
	}
	last := f.last.Load()
	if last == 0 || time.Duration(now-last) < tout {
		return
	}

	// not running cluster-wide broadcasts from the keep-alive receive path
	f.offAsync(p, last, tout)
}

// primary => target keep-alive
func (f *streamsToggle) respKalive(hdr http.Header, now int64, tout time.Duration) {
	if tout <= 0 {
		return
	}
	last := f.last.Load()
	if last != 0 && time.Duration(now-last) < tout {
		hdr.Set(f.hdrActive, "true")
	}
}

//
// primary action
//

func (f *streamsToggle) on(p *proxy, tout time.Duration) error {
	if tout < 0 /* cmn.SharedStreamsEver */ {
		return nil
	}
	now := mono.NanoTime()
	if last := f.last.Load(); last != 0 && time.Duration(now-last) < cmn.SharedStreamsNack {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// recheck after serializing with an in-flight close/open
	now = mono.NanoTime()
	if last := f.last.Load(); last != 0 && time.Duration(now-last) < cmn.SharedStreamsNack {
		return nil
	}

	err := p._toggleStreams(f.actOn)
	if err == nil {
		f.setActive(mono.NanoTime())
	}
	return err
}

func (f *streamsToggle) offAsync(p *proxy, last int64, tout time.Duration) {
	debug.Assert(last > 0, f.hdrActive, " invalid timestamp: ", last)
	if !f.offPending.CAS(false, true) {
		return
	}
	go func() {
		defer f.offPending.Store(false)
		f.off(p, last, tout)
	}()
}

func (f *streamsToggle) off(p *proxy, expectedLast int64, tout time.Duration) {
	debug.Assert(expectedLast > 0, f.hdrActive, " invalid timestamp: ", expectedLast)

	f.mu.Lock()
	defer f.mu.Unlock()

	// recheck after leaving the keep-alive path and serializing vs. on()
	now := mono.NanoTime()
	last := f.last.Load()
	if last == 0 || last != expectedLast || time.Duration(now-last) < tout {
		return
	}

	// claim this exact idle boundary
	if !f.last.CAS(expectedLast, 0) {
		return
	}

	if err := p._toggleStreams(f.actOff); err != nil {
		nlog.WarningDepth(1, err) // benign (see errCloseStreams)
		// undo partial close
		if errN := p._toggleStreams(f.actOn); errN != nil {
			nlog.WarningDepth(1, "nested failure:", f.actOff, "--> undo:", err, errN)
		} else {
			f.setActive(mono.NanoTime())
		}
		return
	}

	// a positive keep-alive may have arrived during the close: reopen
	if f.last.Load() != 0 {
		if errN := p._toggleStreams(f.actOn); errN != nil {
			nlog.WarningDepth(1, "failed to reopen after concurrent activity:", errN)
		}
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
			freeBcastRes(results)
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
			nlog.Warningln("action:", action, "proxy:", res.si.StringEx(), "failed to get notified:", res.err)
		}
	}

	nlog.Infoln(p.String(), "toggle:", action)
ex:
	freeBcArgs(args)
	freeBcastRes(results)
	return nil
}
