// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package load

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
)

//
// for background and usage, see README.md in this package
//

// load dimensions: bit flags
const (
	FlFdt = 1 << iota // file descriptors
	FlSch             // scheduler latency (see sched.go)
	FlMem             // memory (memsys)
	FlCla             // CPU load averages
	FlDsk             // disk utilization

	FlAll = FlFdt | FlSch | FlMem | FlCla | FlDsk
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
	schShift = 1 * slotBits
	memShift = 2 * slotBits
	cpuShift = 3 * slotBits
	dskShift = 4 * slotBits
)

var Text = [...]string{"", "low", "moderate", "high", "critical"}

// Optionally, grade a caller-provided sample - for callers that must grade and
// size against one and the same /proc/meminfo reading.
// NOTE: may trigger free-to-OS
func Mem(mems ...*sys.MemStat) Load {
	return mem(true /*allowFreeToOS*/, mems...)
}

func mem(allowFreeToOS bool, mems ...*sys.MemStat) Load {
	switch memsys.PageMM().Pressure(mems...) {
	case memsys.OOM, memsys.PressureExtreme:
		if allowFreeToOS {
			oom.FreeToOS(true /*force*/)
		}
		return Critical
	case memsys.PressureHigh:
		return High
	case memsys.PressureModerate:
		return Moderate
	default:
		return Low
	}
}

// return `mm` housekeeping interval for a given memory grade (see memsys.HKIval)
func Ival(mm *memsys.MMSA, l Load) time.Duration {
	p := memsys.PressureLow
	switch l {
	case Moderate:
		p = memsys.PressureModerate
	case High:
		p = memsys.PressureHigh
	case Critical:
		p = memsys.PressureExtreme
	}
	return mm.HKIval(p)
}

func CPU() Load {
	load, extreme := sys.CPU(false /*periodic*/)
	high := sys.HighLoadWM()

	switch {
	case extreme:
		return Critical
	case load >= high:
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
// - call oom.FreeToOS on Critical when allowed
func refresh(flags uint64, mi *fs.Mountpath, cfg *cmn.DiskConf, allowFreeToOS bool) (vec uint64) {
	if flags&FlSch != 0 {
		vec = setSlot(vec, schShift, Sched())
	}
	if flags&FlMem != 0 {
		vec = setSlot(vec, memShift, mem(allowFreeToOS))
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
func schOf(x uint64) Load { return Load((x >> schShift) & slotMask) }
func memOf(x uint64) Load { return Load((x >> memShift) & slotMask) }
func cpuOf(x uint64) Load { return Load((x >> cpuShift) & slotMask) }

func setSlot(x uint64, shift int, g Load) uint64 {
	x &^= (slotMask << shift)
	return x | (uint64(g) << shift)
}
