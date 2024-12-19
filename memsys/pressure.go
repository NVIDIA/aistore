// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/sys"
)

// memory _pressure_

const (
	PressureLow = iota
	PressureModerate
	PressureHigh
	PressureExtreme
	OOM
)

const highLowThreshold = 40

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
func (r *MMSA) Pressure(mems ...*sys.MemStat) (pressure int) {
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
		return OOM
	case ncrit > 1 || mem.ActualFree <= r.MinFree:
		return PressureExtreme
	case ncrit > 0:
		return PressureHigh
	case free <= r.MinFree:
		return PressureHigh
	case free > r.lowWM+(r.lowWM>>4):
		return PressureLow
	}

	pressure = PressureModerate
	x := (free - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x < highLowThreshold {
		pressure = PressureHigh
	}
	return
}

func (r *MMSA) pressure2S(sb *strings.Builder, mem *sys.MemStat) {
	sb.WriteString("pressure '")
	p := r.Pressure(mem)
	sb.WriteString(memPressureText[p])
	sb.WriteString("'")
	if crit := r.swap.crit.Load(); crit > 0 {
		sb.WriteString(", swapping(")
		sb.WriteString(strconv.Itoa(int(crit)))
		sb.WriteByte(')')
	}
}
