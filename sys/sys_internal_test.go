// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tools/tassert"
)

func applyEMA(rawUtil, elapsed int64) {
	util := gcpu.compute(elapsed, rawUtil)
	gcpu.utilEMA.Store(util)
}

func TestCPUSmoothing(t *testing.T) {
	var r ring
	gcpu.utilEMA.Store(0)
	ncpu := int64(NumCPU())
	interval := int64(10 * time.Second)

	inject := func(seq int, targetPct int64) (util, elapsed int64) {
		ts := int64(seq) * interval
		usage := int64(seq) * interval * ncpu * targetPct / 100
		cur := sample{usage: usage, ts: ts}
		util, _, elapsed = r.sample(cur)
		return
	}

	// tick 1: prime the ring (no previous => 0)
	inject(1, 50)

	// tick 2: first real sample — seed the EMA directly
	raw, _ := inject(2, 50)
	gcpu.utilEMA.Store(raw)

	// ticks 3-12: steady 50%
	for i := 3; i <= 12; i++ {
		raw, elapsed := inject(i, 50)
		applyEMA(raw, elapsed)
	}
	ema := gcpu.utilEMA.Load()
	tassert.Errorf(t, ema >= 48 && ema <= 52, "steady 50%%: EMA=%d", ema)

	// tick 13: spike to 90%
	raw, elapsed := inject(13, 90)
	applyEMA(raw, elapsed)
	ema = gcpu.utilEMA.Load()
	t.Logf("spike: EMA=%d", ema)
	tassert.Errorf(t, ema > 50 && ema < 70, "single spike: EMA=%d, want (50,70)", ema)

	// ticks 14-24: back to 50%
	for i := 14; i <= 24; i++ {
		raw, elapsed = inject(i, 50)
		applyEMA(raw, elapsed)
	}
	ema = gcpu.utilEMA.Load()
	t.Logf("back to normal: EMA=%d", ema)
	tassert.Errorf(t, ema >= 48 && ema <= 55, "decay back: EMA=%d", ema)
}
