// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
)

// minimal Renewable implementation wrapping a mock.XactMock which is never actually
// renewed via the normal Start/renewLocked path -- entries are inserted
// directly into the registry to control `e.all` size precisely
type fakeRenewable struct {
	RenewBase
	xctn *mock.XactMock
	kind string
}

func newFakeEntry(kind string) Renewable {
	return &fakeRenewable{xctn: mock.NewXact(kind), kind: kind}
}

func (p *fakeRenewable) New(Args, *meta.Bck) Renewable          { return p }
func (p *fakeRenewable) Kind() string                           { return p.kind }
func (p *fakeRenewable) Get() core.Xact                         { return p.xctn }
func (*fakeRenewable) WhenPrevIsRunning(Renewable) (WPR, error) { return WprKeepAndStartNew, nil }
func (p *fakeRenewable) Bucket() *meta.Bck                      { return p.RenewBase.Bck }
func (p *fakeRenewable) UUID() string                           { return p.xctn.ID() }
func (*fakeRenewable) Start() error                             { return nil }

// TestLiveQuietBriefXactQueryableAcrossHistoryCap is a regression test for a bug
// where a currently-running "quiet, brief" xaction (e.g. get-batch, list-objects
// tasks) became permanently invisible to snapshot queries (list_snapshots(),
// get_details(), etc.) once the registry's shared history buffer (`e.all`)
// (which is capped at `keepOldThreshold` across all xaction kinds combined),
// was already full from unrelated xaction traffic.
//
// The xaction was still correctly tracked in `e.active` (so reuse/dispatch
// decisions worked fine) but `_add` silently skipped appending it to `e.all`,
// and `matchingXactsStats` (which backs `GetSnap`) only ever scanned `e.all`.
func TestLiveQuietBriefXactQueryableAcrossHistoryCap(t *testing.T) {
	TestReset()

	tassert.Fatalf(t, xact.Table[apc.ActGetBatch].QuietBrief,
		"expecting %q to be a QuietBrief kind (test assumption)", apc.ActGetBatch)

	// fill `e.all` to the cap with unrelated (non-QuietBrief) xactions,
	// simulating heavy, mixed xaction traffic on a busy target
	dreg.entries.mtx.Lock()
	for range keepOldThreshold {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	dreg.entries.mtx.Unlock()
	tassert.Fatalf(t, len(dreg.entries.all) == keepOldThreshold,
		"expected e.all to be primed at the cap, got %d", len(dreg.entries.all))

	// register one more, currently-running get-batch xaction; per `_add`,
	// this should land in `e.active` but get silently excluded from `e.all`
	live := newFakeEntry(apc.ActGetBatch)
	dreg.entries.mtx.Lock()
	dreg.entries._add(live)
	dreg.entries.mtx.Unlock()

	tassert.Fatalf(t, len(dreg.entries.all) == keepOldThreshold,
		"expected e.all to stay at the cap (live entry excluded), got %d", len(dreg.entries.all))

	var foundActive bool
	for _, entry := range dreg.entries.active {
		if entry.Get().ID() == live.UUID() {
			foundActive = true
			break
		}
	}
	tassert.Fatalf(t, foundActive, "expected live get-batch xaction to be in e.active")

	// the actual regression check: despite being excluded from `e.all`,
	// the live xaction must still be discoverable via a snapshot query
	snaps, err := GetSnap(&Flt{Kind: apc.ActGetBatch})
	tassert.CheckFatal(t, err)

	var found bool
	for _, snap := range snaps {
		if snap.ID == live.UUID() {
			found = true
			break
		}
	}
	tassert.Fatalf(t, found,
		"live get-batch xaction %q not found in GetSnap() result past the history cap", live.UUID())
}

// simulate `hkDelOld` reaping the (short-lived) x-lso history and shrinking `e.all`
// well below the cap while a skipped xaction is still running
func TestSkippedXactSurvivesHistoryShrink(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range keepOldThreshold >> 1 {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	for range keepOldThreshold - keepOldThreshold>>1 {
		dreg.entries._add(newFakeEntry(apc.ActList)) // QuietBrief history
	}
	live := newFakeEntry(apc.ActGetBatch)
	dreg.entries._add(live) // at the cap => skipped
	dreg.entries.mtx.Unlock()

	tassert.Fatalf(t, len(dreg.entries.skipped) == 1, "expected 1 skipped, got %d", len(dreg.entries.skipped))

	// simulate x-lso entries aging out at hk.OldAgeXshort
	dreg.entries.mtx.Lock()
	dreg.entries.all = dreg.entries.all[:keepOldThreshold>>1]
	dreg.entries.mtx.Unlock()

	snaps, err := GetSnap(&Flt{Kind: apc.ActGetBatch})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(snaps) == 1 && snaps[0].ID == live.UUID(),
		"live get-batch lost once history fell to %d: got %d snaps", len(dreg.entries.all), len(snaps))
}

// `e.skipped` is a subset of `e.active` and is reclaimed on the same terms
func TestSkippedReclaimedByHousekeeper(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range keepOldThreshold {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	live := newFakeEntry(apc.ActGetBatch)
	dreg.entries._add(live)
	dreg.entries.mtx.Unlock()
	tassert.Fatalf(t, len(dreg.entries.skipped) == 1, "expected 1 skipped, got %d", len(dreg.entries.skipped))

	live.Get().(*mock.XactMock).SetStopping()
	dreg.finDelta.Inc()
	dreg.hkPruneActive(mono.NanoTime())

	tassert.Fatalf(t, len(dreg.entries.skipped) == 0,
		"skipped entry not reclaimed: %d", len(dreg.entries.skipped))
}

// two distinct entries can carry the same xaction ID: a stopped-but-unreaped one and a
// live one renewed under the same client-supplied UUID (x-lso, x-moss)
func TestSameIDDistinctEntries(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range keepOldThreshold {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	dreg.entries.mtx.Unlock()

	prev := newFakeEntry(apc.ActGetBatch).(*fakeRenewable)
	uuid := prev.xctn.ID()
	prev.xctn.SetStopping() // done, awaiting hkPruneActive

	next := newFakeEntry(apc.ActGetBatch).(*fakeRenewable)
	next.xctn.InitBase(uuid, apc.ActGetBatch, nil) // same UUID, live

	dreg.entries.mtx.Lock()
	dreg.entries._add(prev)
	dreg.entries._add(next)
	dreg.entries.mtx.Unlock()

	snaps, err := GetSnap(&Flt{Kind: apc.ActGetBatch})
	tassert.CheckFatal(t, err)

	var n int
	for _, snap := range snaps {
		if snap.ID == uuid {
			n++
		}
	}
	tassert.Fatalf(t, n == 2, "expected both entries w/ uuid %q, got %d", uuid, n)
}

func TestDelExactEntryPreservesOrder(t *testing.T) {
	TestReset()

	prev := newFakeEntry(apc.ActLRU).(*fakeRenewable)
	uuid := prev.xctn.ID()
	prev.xctn.SetStopping()

	middle := newFakeEntry(apc.ActLRU)
	next := newFakeEntry(apc.ActLRU).(*fakeRenewable)
	next.xctn.InitBase(uuid, apc.ActLRU, nil) // same UUID, live

	dreg.entries.mtx.Lock()
	dreg.entries._add(prev)
	dreg.entries._add(middle)
	dreg.entries._add(next)
	dreg.entries.del([]Renewable{prev})
	dreg.entries.mtx.Unlock()

	tassert.Fatalf(t, len(dreg.entries.all) == 2 && dreg.entries.all[0] == middle && dreg.entries.all[1] == next,
		"history order not preserved after deleting %q", uuid)
	tassert.Fatalf(t, len(dreg.entries.active) == 2 && dreg.entries.active[0] == middle && dreg.entries.active[1] == next,
		"active order not preserved after deleting %q", uuid)

	xctn, err := GetXact(uuid)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xctn == next.Get(), "expected newest xaction %q, got %v", uuid, xctn)
	tassert.Fatalf(t, GetActiveXact(uuid) == next.Get(), "expected newest active xaction %q", uuid)
}

// test `Flt.Matches`
func TestFltMatches(t *testing.T) {
	TestReset()

	tru, fls := true, false
	bck := meta.NewBck("b1", apc.AIS, cmn.NsGlobal)

	mk := func(kind string, stopped bool) core.Xact {
		e := newFakeEntry(kind).(*fakeRenewable)
		if stopped {
			e.xctn.SetStopping()
		}
		return e.Get()
	}
	var (
		lru     = mk(apc.ActLRU, false)      // ScopeGB
		lruDone = mk(apc.ActLRU, true)       //
		ecg     = mk(apc.ActECGet, false)    // ScopeB
		gbatch  = mk(apc.ActGetBatch, false) // ScopeGB
	)
	tests := []struct {
		name string
		flt  Flt
		xctn core.Xact
		want bool
	}{
		{"empty filter matches all", Flt{}, lru, true},
		{"kind hit", Flt{Kind: apc.ActLRU}, lru, true},
		{"kind miss", Flt{Kind: apc.ActECGet}, lru, false},
		{"bucket ignored for ScopeGB/LRU", Flt{Bck: bck}, lru, true},
		{"bucket ignored for ScopeGB/get-batch", Flt{Bck: bck}, gbatch, true},
		{"bucket applies to ScopeB", Flt{Bck: bck}, ecg, false},
		{"kind+bucket, ScopeB", Flt{Kind: apc.ActECGet, Bck: bck}, ecg, false},
		{"kind+bucket, ScopeGB", Flt{Kind: apc.ActLRU, Bck: bck}, lru, true},
		{"only-running true, running", Flt{OnlyRunning: &tru}, lru, true},
		{"only-running true, stopped", Flt{OnlyRunning: &tru}, lruDone, false},
		{"only-running false, running", Flt{OnlyRunning: &fls}, lru, false},
		{"only-running false, stopped", Flt{OnlyRunning: &fls}, lruDone, true},
		{"by id, hit", Flt{ID: lru.ID(), Kind: apc.ActLRU}, lru, true},
		{"by id without kind", Flt{ID: lru.ID()}, lru, true},
		{"by id, miss", Flt{ID: lru.ID(), Kind: apc.ActLRU}, lruDone, false},
		{"by id wins over bucket", Flt{ID: ecg.ID(), Kind: apc.ActECGet, Bck: bck}, ecg, true},
	}
	for _, tc := range tests {
		if got := tc.flt.Matches(tc.xctn); got != tc.want {
			t.Errorf("%s: got %t, want %t", tc.name, got, tc.want)
		}
	}
}

// a burst must not retain its peak allocation for the lifetime of the process
func TestShrinkAfterBurst(t *testing.T) {
	TestReset()

	const burst = 100_000
	dreg.entries.mtx.Lock()
	for range burst {
		entry := newFakeEntry(apc.ActLRU)
		dreg.entries._add(entry)
		entry.Get().(*mock.XactMock).SetStopping()
	}
	dreg.entries.mtx.Unlock()
	capAll, capActive := cap(dreg.entries.all), cap(dreg.entries.active)
	capROActive := cap(dreg.entries.roActive)

	// hkPruneActive drains `active`
	dreg.finDelta.Inc()
	dreg.hkPruneActive(mono.NanoTime())

	// drop the history by hand - this test is about reclamation, not eviction
	dreg.entries.mtx.Lock()
	dreg.entries.all = dreg.entries.all[:0]
	dreg.entries.mtx.Unlock()

	// ... and hkDelOld hands the capacity back (shrink-only pass: nothing to remove)
	dreg.hkDelOld(mono.NanoTime())
	tassert.Fatalf(t, cap(dreg.entries.active) < capActive/10,
		"active capacity not reclaimed: %d -> %d", capActive, cap(dreg.entries.active))
	tassert.Fatalf(t, cap(dreg.entries.roActive) < capROActive/10,
		"roActive capacity not reclaimed: %d -> %d", capROActive, cap(dreg.entries.roActive))
	tassert.Fatalf(t, cap(dreg.entries.all) < capAll/10,
		"history capacity not reclaimed: %d -> %d", capAll, cap(dreg.entries.all))
}

func TestShrinkSkipped(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range keepOldThreshold {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	for range 5000 { // burst of (quiet, brief) entries excluded from history
		entry := newFakeEntry(apc.ActGetBatch)
		dreg.entries._add(entry)
		entry.Get().(*mock.XactMock).SetStopping()
	}
	dreg.entries.mtx.Unlock()
	tassert.Fatalf(t, len(dreg.entries.skipped) == 5000, "skipped: %d", len(dreg.entries.skipped))

	// NOTE: `skipped` entries never enter `all`, so they never give hkDelOld anything
	// to remove - and here `all` is at the cap with everything still running, i.e. the
	// worst case: no removals at all, and the capacity must still come back
	dreg.finDelta.Inc()
	dreg.hkPruneActive(mono.NanoTime())
	dreg.hkDelOld(mono.NanoTime())
	tassert.Fatalf(t, dreg.entries.skipped == nil,
		"skipped not released: len=%d cap=%d", len(dreg.entries.skipped), cap(dreg.entries.skipped))
}

// a pass with nothing to reclaim must not take the write lock or re-allocate
func TestShrinkNoopPass(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range initialCap {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	dreg.entries.mtx.Unlock()

	dreg.entries.mtx.RLock()
	tassert.Fatalf(t, !dreg.entries.shrinkable(), "nothing to reclaim, yet shrinkable()")
	dreg.entries.mtx.RUnlock()

	capAll, capActive := cap(dreg.entries.all), cap(dreg.entries.active)
	dreg.hkDelOld(mono.NanoTime())
	tassert.Fatalf(t, cap(dreg.entries.all) == capAll && cap(dreg.entries.active) == capActive,
		"re-allocated on a no-op pass: all %d->%d, active %d->%d",
		capAll, cap(dreg.entries.all), capActive, cap(dreg.entries.active))
}

// steady state must not re-allocate on every housekeeping pass
func TestShrinkHysteresis(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range initialCap {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	dreg.entries.mtx.Unlock()

	capAll := cap(dreg.entries.all)
	dreg.entries.mtx.Lock()
	dreg.entries.shrinkAll()
	dreg.entries.mtx.Unlock()
	tassert.Fatalf(t, cap(dreg.entries.all) == capAll,
		"re-allocated a healthy slice: %d -> %d", capAll, cap(dreg.entries.all))
}

// hkDelOld hands `del` everything it collected, in `all` order; removal must be
// order-preserving and must not touch anything else
func TestDelBatch(t *testing.T) {
	TestReset()

	const n = 300
	var (
		toRemove = make([]Renewable, 0, n/3)
		want     = make([]Renewable, 0, n)
	)
	dreg.entries.mtx.Lock()
	for i := range n {
		entry := newFakeEntry(apc.ActLRU)
		dreg.entries._add(entry)
		if i%3 == 0 { // every third, collected older-to-newer as hkDelOld does
			entry.Get().(*mock.XactMock).SetStopping()
			toRemove = append(toRemove, entry)
		} else {
			want = append(want, entry)
		}
	}
	dreg.entries.del(toRemove)
	dreg.entries.mtx.Unlock()

	tassert.Fatalf(t, len(dreg.entries.all) == len(want), "all: %d, want %d", len(dreg.entries.all), len(want))
	tassert.Fatalf(t, len(dreg.entries.active) == len(want), "active: %d, want %d", len(dreg.entries.active), len(want))
	for i := range want {
		tassert.Fatalf(t, dreg.entries.all[i] == want[i], "history order broken at %d", i)
		tassert.Fatalf(t, dreg.entries.active[i] == want[i], "active order broken at %d", i)
	}
	// the vacated tail must not pin the removed entries
	for _, entry := range dreg.entries.all[len(want):cap(dreg.entries.all)] {
		tassert.Fatalf(t, entry == nil, "removed entries still referenced by the tail")
	}
}

// `e.roActive` is written under the write lock only
func TestROActiveNoRace(t *testing.T) {
	TestReset()

	dreg.entries.mtx.Lock()
	for range 400 {
		dreg.entries._add(newFakeEntry(apc.ActLRU))
	}
	dreg.entries.mtx.Unlock()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { // the (single) periodic caller - stats
		defer wg.Done()
		for range 2000 {
			var inout core.AllRunningInOut
			dreg.entries.getAllRunning(&inout, true /*periodic*/)
		}
	}()
	go func() { // the API path
		defer wg.Done()
		for range 2000 {
			var inout core.AllRunningInOut
			dreg.entries.getAllRunning(&inout, false /*periodic*/)
		}
	}()
	go func() { // housekeeping
		defer wg.Done()
		for range 2000 {
			dreg.hkDelOld(mono.NanoTime())
		}
	}()
	wg.Wait()

	// nothing was running-and-done, so the registry must be intact
	var inout core.AllRunningInOut
	dreg.entries.getAllRunning(&inout, true /*periodic*/)
	tassert.Fatalf(t, len(inout.Running) == 400, "expected 400 running, got %d", len(inout.Running))
	tassert.Fatalf(t, cap(dreg.entries.roActive) >= len(dreg.entries.active),
		"roActive undersized: cap=%d, active=%d", cap(dreg.entries.roActive), len(dreg.entries.active))
}
