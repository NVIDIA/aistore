// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/sys"
)

// memory pressure
const (
	MemPressureLow = iota
	MemPressureModerate
	MemPressureHigh
	MemPressureExtreme
	OOM
)

var (
	gmm              *MMSA     // page-based MMSA for usage by packages *other than* `ais`
	smm              *MMSA     // memory manager for *small-size* allocations
	gmmOnce, smmOnce sync.Once // ensures singleton-ness

	memPressureText = map[int]string{
		MemPressureLow:      "low",
		MemPressureModerate: "moderate",
		MemPressureHigh:     "high",
		MemPressureExtreme:  "extreme",
		OOM:                 "OOM",
	}

	verbose bool
)

func init() {
	gmm = &MMSA{Name: gmmName, defBufSize: DefaultBufSize}
	smm = &MMSA{Name: smmName, defBufSize: DefaultSmallBufSize}
	verbose = bool(glog.FastV(4, glog.SmoduleMemsys))
	verbose = true // DEBUG
}

// default page-based memory-manager-slab-allocator (MMSA) for packages other than `ais`
func PageMM() *MMSA {
	gmmOnce.Do(func() {
		gmm.Init(0 /*unlimited*/, false, true)
		if smm != nil {
			smm.sibling = gmm
			gmm.sibling = smm
		}
	})
	return gmm
}

// default small-size MMSA
func ByteMM() *MMSA {
	smmOnce.Do(func() {
		smm.Init(0 /*unlimited*/, false, true)
		if gmm != nil {
			gmm.sibling = smm
			smm.sibling = gmm
		}
	})
	return smm
}

func TestPageMM(name ...string) (mm *MMSA) {
	mm = &MMSA{defBufSize: DefaultBufSize, MinFree: minMemFreeTests}
	if len(name) == 0 {
		mm.Name = "pmm.test"
	} else {
		mm.Name = name[0]
	}
	mm.Init(maxMemUsedTests /*absolute usage limit*/, true, true)
	return
}

func TestByteMM(pmm *MMSA) (mm *MMSA) {
	mm = &MMSA{Name: "smm.test", defBufSize: DefaultSmallBufSize, MinFree: minMemFreeTests}
	mm.Init(maxMemUsedTests /*ditto*/, true, true)
	mm.sibling = pmm
	if pmm != nil {
		pmm.sibling = mm
	}
	return
}

// defines the type of Slab rings: NumSmallSlabs x 128 vs NumPageSlabs x 4K
func (r *MMSA) isSmall() bool { return r.defBufSize == DefaultSmallBufSize }

func (r *MMSA) RegWithHK() {
	d := r.duration
	mem, _ := sys.Mem()
	if mem.Free < r.lowWM || mem.Free < minMemFree {
		d = r.duration / 4
		if d < 10*time.Second {
			d = 10 * time.Second
		}
	}
	hk.Reg(r.Name+".gc", r.garbageCollect, d)
}

// initialize new MMSA instance
func (r *MMSA) Init(maxUse int64, panicOnEnvErr, panicOnInsufFreeErr bool) (err error) {
	debug.Assert(r.Name != "")
	// 1. environment overrides defaults and MMSA{...} hard-codings
	if err = r.env(); err != nil {
		if panicOnEnvErr {
			panic(err)
		}
		cos.Errorf("%v", err)
	}

	// 2. compute min-free (must remain free at all times) and low watermark
	mem, _ := sys.Mem()
	if r.MinPctTotal > 0 {
		x := mem.Total * uint64(r.MinPctTotal) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = cos.MinU64(r.MinFree, x)
		}
	}
	if r.MinPctFree > 0 {
		x := mem.Free * uint64(r.MinPctFree) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = cos.MinU64(r.MinFree, x)
		}
	}
	if maxUse > 0 {
		r.MinFree = uint64(cos.MaxI64(int64(r.MinFree), int64(mem.Free)-maxUse))
	}
	if r.MinFree == 0 {
		r.MinFree = minMemFree
	} else if r.MinFree < minMemFree { // warn invalid config
		cos.Errorf("Warning: configured min-free memory %s < %s (absolute minimum)",
			cos.B2S(int64(r.MinFree), 2), cos.B2S(minMemFree, 2))
	}
	r.lowWM = cos.MaxU64(r.MinFree*2, mem.Free/2) // Heu #1: hysteresis
	if maxUse > 0 {
		r.lowWM = uint64(cos.MaxI64(int64(r.lowWM), int64(mem.Free)-maxUse))
	}

	// 3. validate
	if mem.Free < r.MinFree+cos.MiB*16 {
		err = fmt.Errorf("insufficient free memory %s (see %s for guidance)", r.Str(&mem), readme)
		if panicOnInsufFreeErr {
			panic(err)
		}
		cos.Errorf("%v", err)
	}
	if r.lowWM < r.MinFree+minMemFree {
		err = fmt.Errorf("Warning: insufficient free memory %s (see %s)", r.Str(&mem), readme)
		cos.Errorf("%v", err)
		r.lowWM = r.MinFree + minMemFreeTests
	}

	// 4. timer
	if r.TimeIval == 0 {
		r.TimeIval = memCheckAbove
	}
	r.duration = r.TimeIval

	// 5. final construction steps
	r.swap.Store(mem.SwapUsed)
	r.minDepth.Store(minDepth)
	r.toGC.Store(0)

	// init rings and slabs
	r.slabIncStep, r.maxSlabSize, r.numSlabs = PageSlabIncStep, MaxPageSlabSize, NumPageSlabs
	if r.isSmall() {
		r.slabIncStep, r.maxSlabSize, r.numSlabs = SmallSlabIncStep, MaxSmallSlabSize, NumSmallSlabs
	}
	r.slabStats = &slabStats{}
	r.statsSnapshot = &Stats{}
	r.rings = make([]*Slab, r.numSlabs)
	r.sorted = make([]*Slab, r.numSlabs)
	for i := 0; i < r.numSlabs; i++ {
		bufSize := r.slabIncStep * int64(i+1)
		slab := &Slab{
			m:       r,
			tag:     r.Name + "." + cos.B2S(bufSize, 0),
			bufSize: bufSize,
			get:     make([][]byte, 0, minDepth),
			put:     make([][]byte, 0, minDepth),
		}
		slab.pMinDepth = &r.minDepth
		r.rings[i] = slab
		r.sorted[i] = slab
	}

	// 6. GC at init time but only non-small
	if !r.isSmall() {
		runtime.GC()
	}
	debug.Func(func() {
		if flag.Parsed() {
			debug.Infof("%s started", r)
		}
	})
	return
}

// terminate this MMSA instance and, possibly, GC as well
func (r *MMSA) Terminate(unregHK bool) {
	var (
		freed int64
		gced  string
	)
	if unregHK {
		hk.Unreg(r.Name + ".gc")
	}
	for _, s := range r.rings {
		freed += s.cleanup()
	}
	r.toGC.Add(freed)
	mem, _ := sys.Mem()
	swapping := mem.SwapUsed > 0
	if r.doGC(mem.Free, sizeToGC, true, swapping) {
		gced = ", GC ran"
	}
	debug.Infof("%s terminated%s", r, gced)
}

// returns an estimate for the current memory pressure expressed as enumerated values
// also, tracks swapping stateful vars
func (r *MMSA) MemPressure(mems ...*sys.MemStat) (pressure int, swapping bool) {
	var (
		mem   *sys.MemStat
		ncrit int32
	)
	// 1. get mem stats
	if len(mems) > 0 {
		mem = mems[0]
	} else {
		memStat, err := sys.Mem()
		debug.AssertNoErr(err)
		mem = &memStat
	}

	// 2. update swapping state
	swapping, crit := mem.SwapUsed > r.swap.Load(), r.swapCriticality.Load()
	if swapping {
		ncrit = cos.MinI32(swappingMax, crit+1)
	} else {
		ncrit = cos.MaxI32(0, crit-1)
	}
	r.swapCriticality.Store(ncrit)
	r.swap.Store(mem.SwapUsed)

	// 3. recompute mem pressure
	switch {
	case ncrit > 2:
		return OOM, swapping
	case ncrit > 1 || mem.ActualFree <= r.MinFree || swapping:
		return MemPressureExtreme, swapping
	case ncrit > 0 || mem.Free <= r.MinFree:
		return MemPressureHigh, swapping
	case mem.Free >= r.lowWM:
		return MemPressureLow, swapping
	}
	pressure = MemPressureModerate
	x := (mem.Free - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x < 50 {
		pressure = MemPressureHigh
	}
	return
}

func (r *MMSA) MemPressure2S(p int) (s string) {
	s = fmt.Sprintf("pressure '%s'", memPressureText[p])
	if a, b := r.swap.Load(), r.swapCriticality.Load(); a != 0 || b != 0 {
		s = fmt.Sprintf("swapping(%s, criticality '%d'), %s", cos.B2S(int64(a), 0), b, s)
	}
	return
}
