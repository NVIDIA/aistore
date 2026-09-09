// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	tstDiv     = uint64(time.Minute)
	tstSmapVer = int64(7)
)

var tstTag = []byte("tco|from-bck|to-bck|1")

func genBEID(t *testing.T, now uint64) string {
	t.Helper()
	TestReset() // fresh registry: the previous beid must not count as a collision
	beid, prev, err := GenBEID(tstDiv, tstSmapVer, tstTag, now)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, prev == nil, "unexpected collision: %v", prev)
	tassert.Fatalf(t, beid != "", "empty beid")
	return beid
}

// the beid must not depend on MyTime/PrimeTime, each target updates those on its own
func TestGenBEID_IgnoresLocalClock(t *testing.T) {
	const now = uint64(1_700_000_000_000_000_000)

	myTime, primeTime := MyTime.Load(), PrimeTime.Load()
	t.Cleanup(func() {
		MyTime.Store(myTime)
		PrimeTime.Store(primeTime)
	})

	MyTime.Store(0)
	PrimeTime.Store(0)
	first := genBEID(t, now)

	// as if this node had since re-synced against the primary
	MyTime.Store(int64(now))
	PrimeTime.Store(int64(now) + int64(10*time.Minute))
	second := genBEID(t, now)

	tassert.Errorf(t, first == second, "same inputs, different beid: %q vs %q", first, second)
}

// bucket = now/div: same bucket => same beid, next bucket => different beid
func TestGenBEID_DivBoundary(t *testing.T) {
	boundary := (uint64(time.Now().UnixNano())/tstDiv + 1) * tstDiv

	var (
		before = genBEID(t, boundary-1)
		at     = genBEID(t, boundary)
		after  = genBEID(t, boundary+1)
	)
	tassert.Errorf(t, before != at, "expected different beid across the boundary, got %q for both", at)
	tassert.Errorf(t, at == after, "expected same beid within the bucket: %q vs %q", at, after)
}
