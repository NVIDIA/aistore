// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package load

import (
	"runtime"

	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
)

// bitmask of load dimensions (used in refresh argument and getters)
const (
	FlFdt = 1 << iota // file descriptors
	FlGor             // goroutines
	FlMem             // memory (memsys)
	FlCla             // CPU load averages
	FlDsk             // disk utilization

	FlAll = FlFdt | FlGor | FlMem | FlCla | FlDsk
)

// per-dimension load grades
type Load uint8

const (
	Low Load = iota + 1
	Moderate
	High
	Critical // “extreme” in memsys terms
)

// 8 bits per dimension packed into a uint64
const (
	slotBits = 8
	slotMask = uint64(0xff)

	fdtShift = 0 * slotBits
	gorShift = 1 * slotBits
	memShift = 2 * slotBits
	cpuShift = 3 * slotBits
	dskShift = 4 * slotBits
)

const (
	lshiftNgrHigh = 10                // red alert: 1024 * num-CPUs
	lshiftNgrWarn = lshiftNgrHigh - 1 // yellow
)

var (
	gmp int
	// grades atomic.Uint64 // TODO: cache it when/if required - likely with mono-time timestamp(s)
)

func Init() {
	gmp = runtime.GOMAXPROCS(0)
}

// goroutines
func Gor(ngr int) Load {
	if gmp == 0 {
		gmp = runtime.GOMAXPROCS(0)
	}
	warn := gmp << lshiftNgrWarn
	high := gmp << lshiftNgrHigh
	switch {
	case ngr >= high:
		return Critical // red alert
	case ngr >= warn:
		return High // yellow alert
	default:
		return Low
	}
}

func Mem() Load {
	mm := memsys.PageMM()
	switch mm.Pressure() {
	case memsys.OOM, memsys.PressureExtreme:
		return Critical
	case memsys.PressureHigh:
		return High
	case memsys.PressureModerate:
		return Moderate
	default:
		return Low
	}
}

func CPU() Load {
	load, extreme := sys.MaxLoad2()
	high := sys.HighLoadWM()

	switch {
	case extreme:
		return Critical
	case load >= float64(high):
		return High
	default:
		return Low
	}
}

//
// refresh selected load "dimensions" and return the packed vector{shifted grades}
//

// 8 bits per dimension packed into a uint64
// each slot is 0 (unset) or one of Load(Low..Critical)
func Refresh(refresh uint64) (cur uint64) {
	if refresh&FlGor != 0 {
		ngr := runtime.NumGoroutine()
		cur = setSlot(cur, gorShift, Gor(ngr))
	}
	if refresh&FlMem != 0 {
		cur = setSlot(cur, memShift, Mem())
	}
	if refresh&FlCla != 0 {
		cur = setSlot(cur, cpuShift, CPU())
	}
	// grades.Store(cur)
	return cur
}

// func fdtOf(x uint64) Load { return Load((x >> fdtShift) & slotMask) } // TODO: use it
func gorOf(x uint64) Load { return Load((x >> gorShift) & slotMask) }
func memOf(x uint64) Load { return Load((x >> memShift) & slotMask) }
func cpuOf(x uint64) Load { return Load((x >> cpuShift) & slotMask) }

// func dskOf(x uint64) Load { return Load((x >> dskShift) & slotMask) } // ditto

func setSlot(x uint64, shift int, g Load) uint64 {
	x &^= (slotMask << shift)
	return x | (uint64(g) << shift)
}
