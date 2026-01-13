// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/sys"
)

/*
 * The major requirement is supporting variety of deployments ranging from small VMs to
 * multi-terabyte servers. Other requirements include: early warning and quick reaction
 * to running out of memory (OOM).
 *
 * ------------
 * 1. Pressure Levels: Memory pressure is enumerated as follows:
 *    - PressureLow:      plenty of memory
 *    - PressureModerate: enough memory
 *    - PressureHigh:     approaching memory limits
 *    - PressureExtreme:  severely constrained, immediate action required
 *    - OOM:              out-of-memory, with substantial risk to be killed by the kernel
 *
 * 2. Detection Mechanisms:
 *    a) Absolute thresholds:
 *       - MMSA.MinFree: minimum acceptable free memory
           (NOTE: cluster-configurable via "memsys.min_free" and/or "memsys.min_pct_free")
 *       - Low watermark: threshold for transitioning between pressure states
 *
 *    b) Swap trend (the MMSA.swap structure):
 *       - Tracks incremental increases in swap usage over time
 *       - Maintains a criticality counter (ncrit) that increases when swap usage grows
 *         and decreases when swap usage stabilizes or decreases
 *       - Provides early warning by detecting sustained swap pressure trends
 *
 * 3. Housekeeping:
 *    - Periodic memory checks with adaptive intervals based on pressure level
 *    - Memory return to OS only under moderate+ pressure conditions
 *    - Garbage collection triggered when freed memory exceeds threshold
 *      (NOTE: cluster-configurable via "memsys.to_gc")
 *
 * Implementation:
 * ---------------------
 * The updSwap() method:
 * - Increments the swap criticality (MMSA.swap.ncrit) counter when swap usage increases
 * - Decrements when swap usage stabilizes or decreases
 * - Maintains stateful tracking to distinguish between temporary and sustained pressure
 *
 * The Pressure() method:
 * - Determines current pressure level using both absolute free memory and swap trends
 * - Uses tiered logic to handle various combinations of indicators
 * - Calculates pressure between thresholds using proportional scaling
 *
 * TODO:
 * -----
 * 1. Consider incorporating/utilizing Linux PSI: https://docs.kernel.org/accounting/psi.html
 *
 * 2. Dynamic thresholds (MemFree, sizeToGC) based on the rate of memory allocation (or deallocation).
 *    2.1. will also require review of the hysteresis logic, to eliminate oscillation near thresholds.
*/

const (
	PressureLow = iota
	PressureModerate
	PressureHigh
	PressureExtreme
	OOM
)

const highLowThreshold = 40

const (
	FmtErrExtreme = "extreme memory pressure" // (to run or not to run)
	fmtErrHigh    = "high memory pressure"
)

var memPressureText = map[int]string{
	PressureLow:      "low",
	PressureModerate: "moderate",
	PressureHigh:     "high",
	PressureExtreme:  "extreme",
	OOM:              "OOM",
}

// NOTE: used instead of mem.Free as a more realistic estimate where
// mem.BuffCache - kernel buffers and page caches that can be reclaimed -
// is always included _unless_ the resulting number exceeds mem.ActualFree
// (which is unlikely)
func memFree(mem *sys.MemStat) (free uint64) {
	if free = mem.Free + mem.BuffCache; free > mem.ActualFree {
		free = mem.ActualFree
	}
	return
}

// update swapping state
func (r *MMSA) updSwap(mem *sys.MemStat) {
	var ncrit int32
	swapping, crit := mem.SwapUsed > r.swap.size.Load(), r.swap.crit.Load()
	if swapping {
		ncrit = min(swappingMax, crit+1)
	} else {
		ncrit = max(0, crit-1)
	}
	r.swap.crit.Store(ncrit)
	r.swap.size.Store(mem.SwapUsed)
}

// returns an estimate for the current memory pressure expressed as enumerated values
// also, tracks swapping stateful vars
func (r *MMSA) Pressure(mems ...*sys.MemStat) int {
	var mem *sys.MemStat
	if len(mems) > 0 {
		mem = mems[0]
	} else {
		mem = &sys.MemStat{}
		_ = mem.Get()
	}
	free := memFree(mem)
	ncrit := r.swap.crit.Load()
	switch {
	case ncrit > 2:
		nlog.ErrorDepth(1, FmtErrExtreme, "[ ncrit", ncrit, "]")
		return OOM
	case ncrit > 1 || mem.ActualFree <= r.MinFree:
		nlog.ErrorDepth(1, FmtErrExtreme, "[ ncrit", ncrit, "actual", mem.ActualFree, "min", r.MinFree, "]")
		return PressureExtreme
	case ncrit > 0:
		nlog.WarningDepth(1, fmtErrHigh, "[ ncrit", ncrit, "]")
		return PressureHigh
	case free <= r.MinFree:
		return PressureHigh
	case free > r.lowWM+(r.lowWM>>4):
		return PressureLow
	}

	p := PressureModerate
	x := (free - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x < highLowThreshold {
		p = PressureHigh
	}
	return p
}

func (r *MMSA) _p2s(sb *cos.SB, mem *sys.MemStat) {
	sb.WriteString("pressure '")
	p := r.Pressure(mem)
	sb.WriteString(memPressureText[p])
	sb.WriteString("'")
	if crit := r.swap.crit.Load(); crit > 0 {
		sb.WriteString(", swapping(")
		sb.WriteString(strconv.Itoa(int(crit)))
		sb.WriteUint8(')')
	}
}
