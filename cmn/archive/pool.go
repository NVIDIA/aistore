// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

const numShardIdxPools = 3

var (
	// Cover allocations immediately above the largest page slab without retaining
	// multi-megabyte indexes between uses.
	shardIdxPoolCaps = [numShardIdxPools]int{256 * cos.KiB, 512 * cos.KiB, cos.MiB}
	shardIdxPools    [numShardIdxPools]sync.Pool
)

type shardIdxBuf struct {
	b    []byte
	pool *sync.Pool
}

func allocBytes(size int, mm *memsys.MMSA) ([]byte, *memsys.Slab, *shardIdxBuf) {
	if size == 0 {
		return nil, nil, nil
	}
	if mm == nil {
		return make([]byte, size), nil, nil
	}
	if size <= memsys.MaxPageSlabSize {
		buf, slab := mm.AllocSize(int64(size))
		return buf[:size], slab, nil
	}

	for i, capacity := range shardIdxPoolCaps {
		if size > capacity {
			continue
		}
		pool := &shardIdxPools[i]
		if v := pool.Get(); v != nil {
			pooled := v.(*shardIdxBuf)
			debug.Func(func() { debug.Assert(len(pooled.b) == capacity) })
			return pooled.b[:size], nil, pooled
		}
		pooled := &shardIdxBuf{b: make([]byte, capacity), pool: pool}
		return pooled.b[:size], nil, pooled
	}

	return make([]byte, size), nil, nil
}

func allocOffsets(n int, mm *memsys.MMSA) ([]uint32, *memsys.Slab, *shardIdxBuf) {
	if n == 0 {
		return nil, nil, nil
	}
	buf, slab, pooled := allocBytes(n*cos.SizeofI32, mm)
	offs := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(buf))), cap(buf)/cos.SizeofI32)
	return offs[:n], slab, pooled
}

func freeBytes(buf []byte, slab *memsys.Slab, pooled *shardIdxBuf) {
	if slab != nil {
		slab.Free(buf)
		return
	}
	if pooled == nil {
		return
	}
	debug.Func(func() {
		debug.Assert(unsafe.SliceData(buf) == unsafe.SliceData(pooled.b))
		debug.Assert(cap(buf) == len(pooled.b))
	})
	pooled.pool.Put(pooled)
}

func freeOffsets(offs []uint32, slab *memsys.Slab, pooled *shardIdxBuf) {
	if offs == nil {
		return
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(offs))), cap(offs)*cos.SizeofI32)
	debug.Func(func() {
		if slab != nil {
			debug.Assert(int64(cap(buf)) == slab.Size(), "offsets slab round-trip: ", cap(buf), " vs ", slab.Size())
		}
	})
	freeBytes(buf, slab, pooled)
}
