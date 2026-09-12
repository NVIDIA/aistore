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

func TestShardIdxPoolSizing(t *testing.T) {
	mm := memsys.PageMM()
	for _, tc := range []struct {
		name       string
		size       int
		mm         *memsys.MMSA
		capacity   int
		wantSlab   bool
		wantPooled bool
	}{
		{"slab", memsys.MaxPageSlabSize, mm, memsys.MaxPageSlabSize, true, false},
		{"pool-256k", memsys.MaxPageSlabSize + 1, mm, 256 * cos.KiB, false, true},
		{"pool-512k", 256*cos.KiB + 1, mm, 512 * cos.KiB, false, true},
		{"pool-1m", 512*cos.KiB + 1, mm, cos.MiB, false, true},
		{"heap-large", cos.MiB + 1, mm, cos.MiB + 1, false, false},
		{"heap-explicit", 512 * cos.KiB, nil, 512 * cos.KiB, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, slab, pooled := allocBytes(tc.size, tc.mm)
			if len(buf) != tc.size || cap(buf) != tc.capacity {
				t.Fatalf("buffer len/cap: got %d/%d, want %d/%d", len(buf), cap(buf), tc.size, tc.capacity)
			}
			if (slab != nil) != tc.wantSlab {
				t.Fatalf("slab ownership: got %t, want %t", slab != nil, tc.wantSlab)
			}
			if (pooled != nil) != tc.wantPooled {
				t.Fatalf("pool ownership: got %t, want %t", pooled != nil, tc.wantPooled)
			}
			buf[0], buf[len(buf)-1] = 1, 2
			freeBytes(buf, slab, pooled)
		})
	}
}

func TestShardIdxOffsetsPool(t *testing.T) {
	const n = memsys.MaxPageSlabSize/cos.SizeofI32 + 1
	offs, slab, pooled := allocOffsets(n, memsys.PageMM())
	if slab != nil || pooled == nil {
		t.Fatalf("expected pooled offsets: slab=%t, pooled=%t", slab != nil, pooled != nil)
	}
	if cap(offs) != 256*cos.KiB/cos.SizeofI32 {
		t.Fatalf("offsets capacity: got %d, want %d", cap(offs), 256*cos.KiB/cos.SizeofI32)
	}
	offs[0], offs[len(offs)-1] = 1, 2
	freeOffsets(offs, slab, pooled)
}
