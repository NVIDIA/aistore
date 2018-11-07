/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
package memsys

import (
	"sync"

	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	countThreshold = 32 // exceeding this scatter-gather count warrants selecting a larger-size Slab
	minSizeUnknown = 32 * cmn.KiB
)

var (
	fixedSizes = []int64{4 * cmn.KiB, 8 * cmn.KiB, 16 * cmn.KiB, 32 * cmn.KiB, 64 * cmn.KiB, 96 * cmn.KiB, 128 * cmn.KiB}
	allSlabs   = []*Slab{nil, nil, nil, nil, nil, nil, nil} // the length of allSlabs must equal the length of fixedSizes.
)

func init() {
	for i, f := range fixedSizes {
		allSlabs[i] = newSlab(f)
	}
}

// as in: "Slab allocation"
type Slab struct {
	sync.Pool
	fixedSize int64
}

func newSlab(fixedSize int64) *Slab {
	s := &Slab{fixedSize: fixedSize}
	s.Pool.New = s.New
	return s
}

func (s *Slab) New() interface{} { return make([]byte, s.fixedSize) }

func SelectSlab(estimatedSize int64) *Slab {
	if estimatedSize == 0 {
		estimatedSize = minSizeUnknown
	}
	size := cmn.DivCeil(estimatedSize, countThreshold)
	for _, slab := range allSlabs {
		if slab.Size() >= size {
			return slab
		}
	}
	return allSlabs[len(allSlabs)-1]
}

func AllocFromSlab(estimSize int64) ([]byte, *Slab) {
	slab := SelectSlab(estimSize)
	return slab.Alloc(), slab
}

func FreeToSlab(buf []byte, s *Slab) { s.Free(buf) }

func (s *Slab) Alloc() []byte { return s.Get().([]byte) }

func (s *Slab) Free(buf []byte) { s.Put(buf) }

func (s *Slab) Size() int64 { return s.fixedSize }
