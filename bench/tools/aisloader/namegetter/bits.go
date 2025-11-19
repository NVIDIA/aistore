// Package namegetter is a utility to provide random object and archived file names to aisloader
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package namegetter

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// low-level bit-set and bit-mask-tracker for aisloader

const (
	bitsPerWord = cos.SizeofI64 * 8
	bitsMask    = bitsPerWord - 1
	bitsShift   = 6 // log2(64)
)

type (
	bitmaskTracker struct {
		mask []uint64
		used int
	}

	bitSet struct {
		bits []uint64
		n    int
	}
)

////////////////////
// bitmaskTracker //
////////////////////

func (t *bitmaskTracker) init(size int) {
	words := (size + bitsPerWord - 1) >> bitsShift
	t.mask = make([]uint64, words)
	t.used = 0
}

func (t *bitmaskTracker) grow() {
	t.mask = append(t.mask, 0)
}

// fast path
func (t *bitmaskTracker) tryMark(idx int) bool {
	w := idx >> bitsShift              // idx / 64
	b := uint64(1) << (idx & bitsMask) // 1 << (idx % 64)
	if t.mask[w]&b != 0 {
		return false
	}
	t.mask[w] |= b
	t.used++
	return true
}

func (t *bitmaskTracker) reset() {
	for i := range t.mask {
		t.mask[i] = 0
	}
	t.used = 0
}

func (t *bitmaskTracker) needsReset(totalSize int) bool {
	return t.used >= totalSize
}

////////////
// bitSet //
////////////

func (s *bitSet) reinit(n int) {
	words := (n + bitsPerWord - 1) >> bitsShift
	if cap(s.bits) < words {
		s.bits = make([]uint64, words)
	} else {
		s.bits = s.bits[:words]
		for i := range s.bits {
			s.bits[i] = 0
		}
	}
	s.n = n
}

func (s *bitSet) set(i int) {
	debug.Assert(i >= 0 && i < s.n)
	w := i >> bitsShift
	b := uint64(1) << (i & bitsMask)

	debug.Assert(s.bits[w]&b == 0)
	s.bits[w] |= b
}

func (s *bitSet) isSet(i int) bool {
	debug.Assert(i >= 0 && i < s.n)
	w := i >> bitsShift
	b := uint64(1) << (i & bitsMask)
	return s.bits[w]&b != 0
}

func (s *bitSet) testAndSet(i int) bool {
	if s.isSet(i) {
		return false
	}
	s.set(i)
	return true
}
