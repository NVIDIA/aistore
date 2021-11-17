// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

// update swapping state
func (r *MMSA) updSwap(mem *sys.MemStat) {
	var ncrit int32
	swapping, crit := mem.SwapUsed > r.swap.size.Load(), r.swap.crit.Load()
	if swapping {
		ncrit = cos.MinI32(swappingMax, crit+1)
	} else {
		ncrit = cos.MaxI32(0, crit-1)
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
		memStat, err := sys.Mem()
		debug.AssertNoErr(err)
		mem = &memStat
	}

	ncrit := r.swap.crit.Load()
	switch {
	case ncrit > 2:
		return OOM
	case ncrit > 1 || mem.ActualFree <= r.MinFree:
		return PressureExtreme
	case ncrit > 0:
		return PressureHigh
	case mem.Free <= r.MinFree:
		return PressureHigh
	case mem.Free >= r.lowWM:
		return PressureLow
	}

	pressure = PressureModerate
	x := (mem.Free - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x < highLowThreshold {
		pressure = PressureHigh
	}
	return
}

func (r *MMSA) pressure2S(p int) (sp string) {
	sp = "pressure '" + memPressureText[p] + "'"
	if crit := r.swap.crit.Load(); crit > 0 {
		sp = fmt.Sprintf("%s, swapping(%d)", sp, crit)
	}
	return
}
