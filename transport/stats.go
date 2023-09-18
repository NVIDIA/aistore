// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"github.com/NVIDIA/aistore/cmn/atomic"
)

// cumulative transport counters (this target)
const (
	OutObjCount = "stream.out.n"
	OutObjSize  = "stream.out.size"
	InObjCount  = "stream.in.n"
	InObjSize   = "stream.in.size"
)

// stream (session) stats
type Stats struct {
	Num            atomic.Int64 // number of transferred objects including zero size (header-only) objects
	Size           atomic.Int64 // transferred object size (does not include transport headers)
	Offset         atomic.Int64 // stream offset, in bytes
	CompressedSize atomic.Int64 // compressed size (converges to the actual compressed size over time)
}

type nopRxStats struct{}

// interface guard
var (
	_ rxStats = (*Stats)(nil)
	_ rxStats = (*nopRxStats)(nil)
)

func (s *Stats) addOff(o int64) { s.Offset.Add(o) }
func (s *Stats) incNum()        { s.Num.Inc() }

func (nopRxStats) addOff(int64) {}
func (nopRxStats) incNum()      {}
