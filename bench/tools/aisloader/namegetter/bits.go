// Package namegetter is a utility to provide random object and archived file names to aisloader
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package namegetter

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// low-level bit-set for aisloader

const (
	bitsPerWord = cos.SizeofI64 * 8
	bitsMask    = bitsPerWord - 1
	bitsShift   = 6 // log2(64)
)

type bitSet struct {
	bits []uint64
	n    int
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

	debug.AssertFunc(func() bool { return s.bits[w]&b == 0 })
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
