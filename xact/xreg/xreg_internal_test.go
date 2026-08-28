// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
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
