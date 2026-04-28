// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xs"
)

// TestTuneBlobDlWorkers exercises the worker-count formula without a running cluster:
//
//	min(TuneNumWorkers(...), ceil(fullSize/256MiB), blobMaxWorkers)
//
// Cases use small `requested` values (≤ 2) so TuneNumWorkers under normal test-host
// load returns the requested value verbatim — making the size-cap branch the
// deterministic binder. NwpNone is the only result that propagates regardless of size.
func TestTuneBlobDlWorkers(t *testing.T) {
	tools.PrepareMountPaths(t, 4)

	tests := []struct {
		name      string
		requested int
		fullSize  int64
		expected  int
	}{
		// serial fast-path: short-circuits before applying size cap
		{"serial/small", xact.NwpNone, cos.MiB, xact.NwpNone},
		{"serial/huge", xact.NwpNone, 16 * cos.GiB, xact.NwpNone},

		// size cap = ceil(fullSize / 256MiB) is the binder; requested=2 is high enough to bypass it only when sizeCap > 2
		{"sizeCap=1/tiny", 2, 1024, 1},                  // ceil(1KiB/256MiB) = 1
		{"sizeCap=1/quarter", 2, 64 * cos.MiB, 1},       // sizeCap = 1
		{"sizeCap=1/256MiB-exact", 2, 256 * cos.MiB, 1}, // sizeCap = 1
		{"sizeCap=2/257MiB", 2, 257 * cos.MiB, 2},       // sizeCap = 2 (ceil)
		{"sizeCap=2/512MiB", 2, 512 * cos.MiB, 2},       // sizeCap = 2

		// requested binds (sizeCap is larger than requested)
		{"reqBinds/req=1,size=1GiB", 1, cos.GiB, 1},     // sizeCap = 4, req=1 wins
		{"reqBinds/req=2,size=2GiB", 2, 2 * cos.GiB, 2}, // sizeCap = 8, req=2 wins
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := xs.TuneBlobDlWorkers("test", tc.requested, 4, tc.fullSize)
			tassert.CheckFatal(t, err)
			// Test host may transiently report extreme load -> NwpNone; skip rather than fail.
			if got == xact.NwpNone && tc.expected != xact.NwpNone {
				t.Skipf("transient NwpNone (system load); req=%d, size=%d", tc.requested, tc.fullSize)
			}
			tassert.Fatalf(t, got == tc.expected,
				"TuneBlobDlWorkers(req=%d, size=%d): expected %d, got %d",
				tc.requested, tc.fullSize, tc.expected, got)
		})
	}
}
