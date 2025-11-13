// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package load

import (
	"runtime"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
)

// load dimensions: bit flags
const (
	FlFdt = 1 << iota // file descriptors
	FlGor             // goroutines
	FlMem             // memory (memsys)
	FlCla             // CPU load averages
	FlDsk             // disk utilization

	FlAll = FlFdt | FlGor | FlMem | FlCla | FlDsk
)

// per-dimension load level
type Load uint8

const (
	Low Load = iota + 1
	Moderate
	High
	Critical // “extreme” in memsys terms
)

// 8 bits per _dimension_ packed into a uint64
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
	lshiftNgrHigh = 10                // high-number-goroutines red alert: 1024 * num-CPUs
	lshiftNgrWarn = lshiftNgrHigh - 1 // yellow
)

var (
	gmp int

	Text = [...]string{"", "low", "moderate", "high", "critical"}
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

// NOTE: may trigger free-to-OS
func Mem() Load {
	mm := memsys.PageMM()
	switch mm.Pressure() {
	case memsys.OOM, memsys.PressureExtreme:
		oom.FreeToOS(true /*force*/)
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

func Dsk(mi *fs.Mountpath, cfg *cmn.DiskConf) Load {
	var util int64
	if mi == nil {
		util = fs.GetMaxUtil()
	} else {
		util = mi.GetUtil()
	}
	switch {
	case util >= cfg.DiskUtilMaxWM:
		return Critical
	case util > cfg.DiskUtilHighWM:
		return High
	default:
		return Low
	}
}

// refresh selected load _dimensions_ and return the packed vector:
// - 8 bits per dimension packed into a uint64
// - each slot is 0 (unset) or one of Load(Low..Critical)
// - calls oom.FreeToOS when critical
func refresh(flags uint64, mi *fs.Mountpath, cfg *cmn.DiskConf) (vec uint64) {
	if flags&FlGor != 0 {
		ngr := runtime.NumGoroutine()
		vec = setSlot(vec, gorShift, Gor(ngr))
	}
	if flags&FlMem != 0 {
		vec = setSlot(vec, memShift, Mem())
	}
	if flags&FlCla != 0 {
		vec = setSlot(vec, cpuShift, CPU())
	}
	if flags&FlDsk != 0 {
		vec = setSlot(vec, dskShift, Dsk(mi, cfg))
	}
	return vec
}

// func fdtOf(x uint64) Load { return Load((x >> fdtShift) & slotMask) } // TODO: use it

func dskOf(x uint64) Load { return Load((x >> dskShift) & slotMask) } // ditto
func gorOf(x uint64) Load { return Load((x >> gorShift) & slotMask) }
func memOf(x uint64) Load { return Load((x >> memShift) & slotMask) }
func cpuOf(x uint64) Load { return Load((x >> cpuShift) & slotMask) }

func setSlot(x uint64, shift int, g Load) uint64 {
	x &^= (slotMask << shift)
	return x | (uint64(g) << shift)
}
