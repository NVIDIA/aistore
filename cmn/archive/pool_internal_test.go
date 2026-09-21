// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

// every allocOffsets class must survive the freeOffsets round-trip: the byte slice is
// reconstructed from cap(offs), so a mismatch shows up here (or as a memsys assert)
func TestShardIdxOffsetsRoundTrip(t *testing.T) {
	mm := memsys.PageMM()
	for _, tc := range []struct {
		name       string
		n          int
		mm         *memsys.MMSA
		capacity   int
		wantSlab   bool
		wantPooled bool
	}{
		{"zero", 0, mm, 0, false, false},
		{"slab", 1024, mm, 0, true, false},
		{"slab-max", memsys.MaxPageSlabSize / cos.SizeofI32, mm, 0, true, false},
		{"pool-256k", memsys.MaxPageSlabSize/cos.SizeofI32 + 1, mm, 256 * cos.KiB / cos.SizeofI32, false, true},
		{"pool-512k", 256*cos.KiB/cos.SizeofI32 + 1, mm, 512 * cos.KiB / cos.SizeofI32, false, true},
		{"pool-1m", 512*cos.KiB/cos.SizeofI32 + 1, mm, cos.MiB / cos.SizeofI32, false, true},
		{"heap-explicit", 1024, nil, 1024, false, false},
		{"heap-large", cos.MiB/cos.SizeofI32 + 1, mm, cos.MiB/cos.SizeofI32 + 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			offs, slab, pooled := allocOffsets(tc.n, tc.mm)
			if len(offs) != tc.n {
				t.Fatalf("offsets len: got %d, want %d", len(offs), tc.n)
			}
			if tc.capacity > 0 && cap(offs) != tc.capacity {
				t.Fatalf("offsets capacity: got %d, want %d", cap(offs), tc.capacity)
			}
			if (slab != nil) != tc.wantSlab {
				t.Fatalf("slab ownership: got %t, want %t", slab != nil, tc.wantSlab)
			}
			if (pooled != nil) != tc.wantPooled {
				t.Fatalf("pool ownership: got %t, want %t", pooled != nil, tc.wantPooled)
			}
			if tc.n > 0 {
				offs[0], offs[tc.n-1] = 1, 2
			}
			freeOffsets(offs, slab, pooled)
		})
	}
}
