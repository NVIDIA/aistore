// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package load

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

//
// unified throttling recommendation based on current system load
// for background and usage, see README.md in this package
//

const (
	sleep1ms   = time.Millisecond
	sleep10ms  = 10 * time.Millisecond
	sleep100ms = 100 * time.Millisecond

	maxBatch   = 0x1fff // mask: every 8192 ops
	dfltBatch  = 0x1ff  // 512
	smallBatch = 0x1f   // 32
	minBatch   = 0xf    // 16
)

type (
	Extra struct {
		Mi  *fs.Mountpath // when nil return max util across mountpaths
		Cfg *cmn.DiskConf
		RW  bool // true when reading or writing data
	}
	Advice struct {
		// init
		flags uint64
		extra Extra

		// packed per-dimension grades (Low..Critical)
		loads uint64

		// runtime
		Sleep time.Duration // recommended sleep at check points
		Batch int64         // bitmask of the form 2^k - 1 (e.g., 0x1f => check memory, CPU, etc. load every 32 ops)
		Load  Load          // highest load level across requested _dimensions_
	}
)

// memory always included
func (a *Advice) Init(flags uint64, extra *Extra) {
	debug.Assert(flags&FlAll != 0, "no flags")
	if flags&FlDsk != 0 {
		debug.Assert(extra != nil && extra.Cfg != nil, "disk requires config and, optionally, mountpath")
	}

	a.flags = flags | FlMem
	if extra != nil {
		a.extra = *extra
	}

	a.Refresh()
}

// return true when the caller should perform a throttle check after the N-th op.
func (a *Advice) ShouldCheck(n int64) bool {
	debug.Assert(a.Batch > 0, "must check every so often")
	return n&a.Batch == a.Batch
}

// recompute throttling recommendation; note:
// disk requires config and, optionally, mountpath
func (a *Advice) Refresh() {
	var (
		mi  *fs.Mountpath
		cfg *cmn.DiskConf
	)
	if a.flags&FlDsk != 0 {
		mi, cfg = a.extra.Mi, a.extra.Cfg
	}
	a.loads = refresh(a.flags, mi, cfg)

	// reset optimistically
	a.Sleep = 0
	a.Batch = maxBatch
	a.Load = memOf(a.loads)

	// 1) memory pressure
	switch memOf(a.loads) {
	case Critical:
		a.Sleep = sleep100ms
		a.Batch = minBatch
		return // memory-critical short-circuit, regardless
	case High:
		a.Sleep = sleep10ms
		a.Batch = smallBatch
	case Moderate:
		a.Batch = smallBatch
	}

	// 2) goroutines and CPU
	switch {
	case gorOf(a.loads) == Critical && cpuOf(a.loads) == Critical:
		a.Sleep = cos.Ternary(a.Sleep >= sleep10ms, max(sleep100ms, a.Sleep), sleep10ms)
		a.Batch = min(a.Batch, minBatch)
		a.Load = Critical
	case gorOf(a.loads) == Critical || cpuOf(a.loads) == Critical:
		a.Sleep = cos.Ternary(a.Sleep >= sleep1ms, max(sleep10ms, a.Sleep), sleep1ms)
		a.Batch = min(a.Batch, smallBatch)
		a.Load = Critical
	case gorOf(a.loads) == High || cpuOf(a.loads) == High:
		a.Sleep = max(a.Sleep, sleep1ms)
		a.Batch = min(a.Batch, smallBatch)
		a.Load = max(a.Load, High)
	}

	// 3) disk
	switch dskOf(a.loads) {
	case Critical:
		a.Sleep = cos.Ternary(a.Sleep >= sleep10ms, max(sleep100ms, a.Sleep), sleep10ms)
		a.Batch = min(a.Batch, minBatch)
		a.Load = Critical
	case High:
		a.Sleep = max(a.Sleep, sleep1ms)
		a.Batch = min(a.Batch, smallBatch)
		a.Load = max(a.Load, High)
	}

	// 4) adjust for metadata-only workloads (list-objects, cleanup, storage-summary):
	// keep going under High load
	// only back off when things are really critical
	// super-fine sampling not needed
	if !a.extra.RW {
		if a.Load < Critical {
			a.Sleep = 0
			a.Batch = min(a.Batch<<4, maxBatch)
		}
	}
}

//
// inline helpers
//

func (a *Advice) MemLoad() Load { return memOf(a.loads) }
func (a *Advice) ClaLoad() Load { return cpuOf(a.loads) }
func (a *Advice) DskLoad() Load { return dskOf(a.loads) }

//
// usage: nlog
//

func (a *Advice) String() string {
	var sb strings.Builder
	sb.Grow(92)

	sb.WriteString("load=")
	sb.WriteString(Text[a.Load])
	sb.WriteString(" sleep=")
	sb.WriteString(a.Sleep.String())

	get := func(shift uint64) Load {
		return Load((a.loads >> shift) & slotMask)
	}
	dims := []struct {
		name string
		l    Load
	}{
		{"mem", get(memShift)},
		{"cpu", get(cpuShift)},
		{"dsk", get(dskShift)},
		{"gor", get(gorShift)},
		{"fdt", get(fdtShift)},
	}
	first := true
	for _, d := range dims {
		if d.l == 0 {
			continue
		}
		if first {
			sb.WriteString(" [")
			first = false
		} else {
			sb.WriteByte(' ')
		}
		sb.WriteString(d.name)
		sb.WriteByte('=')
		sb.WriteString(Text[d.l])
	}
	if !first {
		sb.WriteByte(']')
	}
	return sb.String()
}
