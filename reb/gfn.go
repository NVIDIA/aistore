// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const timedDuration = time.Minute + time.Minute/2

const (
	gfnT   = "timed"
	hkgfnT = "gfn-timed"
	gfnG   = "global"
)

// get-from-neighbors (GFN) state
type gfnCtx struct {
	exp atomic.Int64
	gon atomic.Bool
}

var gfn = &gfnCtx{}

func IsGFN() bool {
	return gfn.gon.Load() || gfn.exp.Load() > mono.NanoTime()
}

func OnTimedGFN() {
	if gfn.gon.Load() {
		return
	}
	act := " updated"
	exp := mono.NanoTime() + timedDuration.Nanoseconds()
	if gfn.exp.Swap(exp) == 0 {
		act = " on"
	}
	nlog.Infoln(gfnT, act)
}

func OffTimedGFN(detail string) {
	gfn.exp.Store(0)
	nlog.Infoln(gfnT, " off ", detail)
}

func onGFN() (prev bool) {
	if prev = gfn.gon.Swap(true); prev {
		return
	}
	if exp := gfn.exp.Swap(0); exp > mono.NanoTime() {
		nlog.Infoln(gfnG, " on ", gfnT, " off")
	} else {
		nlog.Infoln(gfnG, " on")
	}
	return
}

func offGFN() {
	gfn.gon.Store(false)
	nlog.Infoln(gfnG, " off")
}
