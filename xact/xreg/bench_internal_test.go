// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
)

// registry:
// - "nominal": history well below the cap, nothing skipped (`e.skipped` empty)
// - "atcap":   history at `keepOldThreshold`, live (quiet, brief) xactions skipped
func benchFill(nAll, nSkipped int, kinds ...string) {
	TestReset()
	dreg.entries.mtx.Lock()
	for i := range nAll {
		dreg.entries._add(newFakeEntry(kinds[i%len(kinds)]))
	}
	for range nSkipped {
		dreg.entries._add(newFakeEntry(apc.ActGetBatch))
	}
	dreg.entries.mtx.Unlock()
}

func BenchmarkMatchingXactsStats(b *testing.B) {
	flt := &Flt{Kind: apc.ActGetBatch}
	all := func(core.Xact) bool { return true }

	b.Run("nominal/bykind", func(b *testing.B) {
		benchFill(300, 0, apc.ActLRU, apc.ActECGet, apc.ActList)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			dreg.matchingXactsStats(flt.Matches)
		}
	})
	b.Run("atcap/bykind", func(b *testing.B) {
		benchFill(keepOldThreshold, 40, apc.ActLRU, apc.ActECGet, apc.ActList)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			dreg.matchingXactsStats(flt.Matches)
		}
	})
	b.Run("atcap/unfiltered", func(b *testing.B) {
		benchFill(keepOldThreshold, 40, apc.ActLRU, apc.ActECGet, apc.ActList)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			dreg.matchingXactsStats(all)
		}
	})
}

// `match` runs once per registry entry, so its cost is multiplied by `len(e.all)`
// on every `GetSnap` - and `GetSnap` is polled (`ais show job`, api.WaitForXaction)
func BenchmarkFltMatches(b *testing.B) {
	TestReset()
	entry := newFakeEntry(apc.ActLRU)
	xctn := entry.Get()
	flt := &Flt{Kind: apc.ActGetBatch}

	b.ReportAllocs()
	for b.Loop() {
		flt.Matches(xctn)
	}
}

// `e.active` scan, for reference: what gating on `e.skipped` avoids paying per query
func BenchmarkActiveScan(b *testing.B) {
	benchFill(300, 0, apc.ActLRU, apc.ActECGet, apc.ActList)
	flt := &Flt{Kind: apc.ActGetBatch}

	b.ReportAllocs()
	for b.Loop() {
		var n int
		dreg.entries.mtx.RLock()
		for _, entry := range dreg.entries.active {
			xctn := entry.Get()
			if xctn != nil && xact.Table[xctn.Kind()].QuietBrief && flt.Matches(xctn) {
				n++
			}
		}
		dreg.entries.mtx.RUnlock()
		_ = n
	}
}
