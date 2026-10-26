// Package meta: cluster-level metadata
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Target placement weights (v5.2)
// - all targets are weighted, or none
// - proportional weights define the same placement; so do equal weights and no weights
// - require a same-version cluster (see ais/pversion.go)

// TODO, in order:
// - Python SDK:
//   * get/set weights
//   * weighted HRW for direct-to-target access (****)

// max/min weight ratio
const (
	PlacementWeightWarnRatio = 10  // warn
	PlacementWeightMaxRatio  = 100 // reject
)

// returns zero when not weighted
func (d *Snode) PlacementWeight() int64 {
	if d.Placement == nil {
		return 0
	}
	return d.Placement.Weight
}

// replaces (never modifies) Placement - Smap clones share it
func (d *Snode) SetPlacementWeight(w int64) {
	if w == 0 {
		d.Placement = nil
		return
	}
	d.Placement = &PlacementConf{Weight: w}
}

// computes weight for a target joining a weighted cluster: the (rounded) mean weight
// of active targets, which approximates its uniform-HRW share
// - falls back to all weighted targets when none is active
// - returns zero when not weighted
func (m *Smap) MeanPlacementWeight() int64 {
	if mean := m._mean(true /*active only*/); mean > 0 {
		return mean
	}
	return m._mean(false)
}

func (m *Smap) _mean(activeOnly bool) int64 {
	var (
		hi, lo uint64 // 128-bit sum
		n      uint64
		g      int64
	)
	for _, tsi := range m.Tmap {
		w := tsi.PlacementWeight()
		debug.Assert(w >= 0)
		if w <= 0 || (activeOnly && tsi.InMaintOrDecomm()) {
			continue
		}
		g = cos.GCD(g, w)
		n++
	}
	if n == 0 {
		return 0
	}
	for _, tsi := range m.Tmap {
		w := tsi.PlacementWeight()
		if w <= 0 || (activeOnly && tsi.InMaintOrDecomm()) {
			continue
		}
		var carry uint64
		lo, carry = bits.Add64(lo, uint64(w/g), 0)
		hi += carry
	}
	// round to nearest (hi < n since sum < n * 2^63)
	var carry uint64
	lo, carry = bits.Add64(lo, n/2, 0)
	hi += carry
	q, _ := bits.Div64(hi, lo, n)
	debug.Assert(q > 0 && q <= math.MaxInt64/uint64(g))
	return max(int64(q), 1) * g
}

// validates all target weights at once: either all zero (clear) or all positive,
// with max/min ratio below PlacementWeightMaxRatio; returns the ratio
func CheckPlacementWeights(weights map[string]int64) (ratio int64, err error) {
	if len(weights) == 0 {
		return 0, errors.New("no target placement weights")
	}
	var (
		lo, hi int64
		zeros  int
	)
	for tid, w := range weights {
		switch {
		case w < 0:
			return 0, fmt.Errorf("invalid placement weight %d for %s: negative values are reserved", w, Tname(tid))
		case w == 0:
			zeros++
			continue
		}
		if lo == 0 || w < lo {
			lo = w
		}
		hi = max(hi, w)
	}
	switch zeros {
	case len(weights):
		return 0, nil // clear all
	case 0:
	default:
		return 0, errors.New("invalid placement weights: either all targets are weighted (positive) or none is (all zero)")
	}
	ratio = hi / lo
	if ratio >= PlacementWeightMaxRatio {
		return ratio, fmt.Errorf("invalid placement weights: max/min ratio %d/%d is %dx or greater", hi, lo, PlacementWeightMaxRatio)
	}
	return ratio, nil
}

// returns true if any target is weighted
func (m *Smap) IsWeighted() bool {
	for _, tsi := range m.Tmap {
		if tsi.Placement != nil {
			return true
		}
	}
	return false
}

// returns true if both Smaps place objects the same way across active targets
func (m *Smap) SamePlacementWeights(other *Smap) bool {
	a, b := m.reducedWeights(), other.reducedWeights()
	return maps.Equal(a, b)
}

// divides active targets' weights by their GCD; returns nil when uniform
func (m *Smap) reducedWeights() map[string]int64 {
	var (
		g       int64
		first   int64
		n       int
		uniform = true
	)
	for _, tsi := range m.Tmap {
		if tsi.InMaintOrDecomm() {
			continue
		}
		w := tsi.PlacementWeight()
		debug.Assert(w >= 0)
		if n == 0 {
			first = w
		} else if w != first {
			uniform = false
		}
		n++
		g = cos.GCD(g, w)
	}
	if uniform || g == 0 {
		return nil
	}
	reduced := make(map[string]int64, n)
	for tid, tsi := range m.Tmap {
		if !tsi.InMaintOrDecomm() {
			reduced[tid] = tsi.PlacementWeight() / g
		}
	}
	return reduced
}

// formats weights and normalized shares of active targets, e.g. "t[a]=1(10.0%), t[b]=3(30.0%)"
func (m *Smap) StrPlacementWeights() string {
	if !m.IsWeighted() {
		return "none"
	}
	var sum float64
	for _, tsi := range m.Tmap {
		if !tsi.InMaintOrDecomm() {
			sum += float64(tsi.PlacementWeight())
		}
	}
	var (
		sb   cos.SB
		tids = slices.Sorted(maps.Keys(m.Tmap))
	)
	sb.Init(len(tids) * 32)
	for i, tid := range tids {
		tsi := m.Tmap[tid]
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(tsi.StringEx())
		sb.WriteUint8('=')
		w := tsi.PlacementWeight()
		if w == 0 {
			sb.WriteString("unassigned")
			continue
		}
		sb.WriteString(strconv.FormatInt(w, 10))
		sb.WriteUint8('(')
		if tsi.InMaintOrDecomm() {
			sb.WriteString("maint")
		} else {
			sb.WriteString(strconv.FormatFloat(float64(w)*100/sum, 'f', 1, 64))
			sb.WriteUint8('%')
		}
		sb.WriteUint8(')')
	}
	return sb.String()
}
