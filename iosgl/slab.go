/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package iosgl provides Reader and (streaming) Writer on top of a Scatter Gather List (SGL) of reusable buffers.
package iosgl

import (
	"sync"

	"github.com/NVIDIA/dfcpub/common"
)

const (
	countThreshold = 32 // exceeding this scatter-gather count warrants selecting a larger-size Slab
	minSizeUnknown = 32 * common.KiB
)

var (
	fixedSizes = []int64{4 * common.KiB, 8 * common.KiB, 16 * common.KiB, 32 * common.KiB, 64 * common.KiB, 96 * common.KiB, 128 * common.KiB}
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
	for i, slab := range allSlabs {
		n := common.DivCeil(estimatedSize, slab.Size())
		if n <= countThreshold {
			return allSlabs[i]
		}
	}
	return allSlabs[len(allSlabs)-1]
}

func (s *Slab) Alloc() []byte { return s.Get().([]byte) }

func (s *Slab) Free(buf []byte) { s.Put(buf) }

func (s *Slab) Size() int64 { return s.fixedSize }

func AllocFromSlab(desiredSize int64) ([]byte, *Slab) {
	slab := SelectSlab(desiredSize)
	return slab.Alloc(), slab
}

func FreeToSlab(buf []byte, s *Slab) { s.Free(buf) }
