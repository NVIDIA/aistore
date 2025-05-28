// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
)

// track cluster-wide “active” state refreshed by keep-alives and
// auto-deactivates after a timeout;
// negative timeout (see `tout` below) => never deactivate

type (
	featureToggle struct {
		hdrActive  string               // e.g. apc.HdrActiveEC
		timeoutFn  func() time.Duration // timeout (configured or default; negative => don't deactivate)
		deactivate func(last int64)     // broadcast Close()
		last       atomic.Int64         // mono-time of last positive refresh
	}

	ecFT struct {
		featureToggle
	}
)

func (ft *ecFT) init(deactivate func(last int64)) {
	ft.hdrActive = apc.HdrActiveEC
	ft.timeoutFn = cmn.Rom.EcStreams
	ft.deactivate = deactivate
}

func (ft *featureToggle) isActive(h http.Header) bool { _, ok := h[ft.hdrActive]; return ok }
func (ft *featureToggle) setActive(now int64)         { ft.last.Store(now) }

// target => primary keep-alive
func (ft *featureToggle) recvKalive(hdr http.Header, now int64) {
	if _, ok := hdr[ft.hdrActive]; ok {
		ft.setActive(now)
		return
	}
	tout := ft.timeoutFn()
	if tout < 0 {
		return
	}
	last := ft.last.Load()
	if last == 0 || time.Duration(now-last) < tout {
		return
	}
	ft.deactivate(last) // extra sanity check lives inside deactivate()
}

// primary => target keep-alive
func (ft *featureToggle) respKalive(hdr http.Header, now int64) {
	if tout := ft.timeoutFn(); tout > 0 {
		if last := ft.last.Load(); last != 0 && time.Duration(now-last) < tout {
			hdr.Set(ft.hdrActive, "true")
		}
	}
}
