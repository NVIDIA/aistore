// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
)

const (
	minMemFree      = cos.GiB + cos.GiB>>1 // default minimum memory (see description above)
	minMemFreeTests = cos.MiB * 128        // minimum free to run tests
	maxMemUsedTests = cos.GiB * 10         // maximum tests allowed to allocate
)

var (
	gmm              *MMSA     // page-based system allocator
	smm              *MMSA     // slab allocator for small sizes in the range 1 - 4K
	gmmOnce, smmOnce sync.Once // ensures singleton-ness
	verbose          bool
)

func Init(gmmName, smmName string, config *cmn.Config) {
	debug.Assert(gmm == nil && smm == nil)

	// page mmsa config (see also "AIS_MINMEM_FREE" and related environment)
	defBufSize := int64(DefaultBufSize)
	if config.Memsys.DefaultBufSize != 0 {
		defBufSize = int64(config.Memsys.DefaultBufSize)
	}
	gmm = &MMSA{Name: gmmName + ".gmm", defBufSize: defBufSize, slabIncStep: PageSlabIncStep}
	gmm.MinFree = uint64(config.Memsys.MinFree)
	gmm.MinPctTotal = config.Memsys.MinPctTotal
	gmm.MinPctFree = config.Memsys.MinPctFree

	// hk config
	if config.Memsys.SizeToGC != 0 {
		sizeToGC = int64(config.Memsys.SizeToGC)
	}
	if config.Memsys.HousekeepTime != 0 {
		memCheckAbove = config.Memsys.HousekeepTime.D()
	}

	gmm.Init(0)

	// byte mmsa:
	smm = &MMSA{Name: smmName + ".smm", defBufSize: DefaultSmallBufSize, slabIncStep: SmallSlabIncStep}
	smm.Init(0)
	smm.sibling = gmm
	gmm.sibling = smm

	// verbosity
	verbose = config.FastV(5, glog.SmoduleMemsys)
}

func NewMMSA(name string) (mem *MMSA, err error) {
	mem = &MMSA{Name: name + ".test.pmm", defBufSize: DefaultBufSize, slabIncStep: PageSlabIncStep, MinFree: minMemFreeTests}
	err = mem.Init(0)
	return
}

// system page-based memory-manager-slab-allocator (MMSA)
func PageMM() *MMSA {
	gmmOnce.Do(func() {
		if gmm == nil {
			// tests only
			gmm = &MMSA{
				Name:        "test.pmm",
				defBufSize:  DefaultBufSize,
				slabIncStep: PageSlabIncStep,
				MinFree:     minMemFreeTests,
			}
			gmm.Init(maxMemUsedTests)
			if smm != nil {
				smm.sibling = gmm
				gmm.sibling = smm
			}
		}
		debug.Assert(gmm.rings != nil)
	})
	return gmm
}

// system small-size allocator (range 1 - 4K)
func ByteMM() *MMSA {
	smmOnce.Do(func() {
		if smm == nil {
			// tests only
			smm = &MMSA{
				Name:        "test.smm",
				defBufSize:  DefaultSmallBufSize,
				slabIncStep: SmallSlabIncStep,
				MinFree:     minMemFreeTests,
			}
			smm.Init(maxMemUsedTests)
			if gmm != nil {
				gmm.sibling = smm
				smm.sibling = gmm
			}
		}
		debug.Assert(smm.rings != nil)
	})
	return smm
}

// byte vs page rings: (NumSmallSlabs x 128) vs (NumPageSlabs x 4K), respectively
func (r *MMSA) isPage() bool { return r.slabIncStep == PageSlabIncStep }

func (r *MMSA) RegWithHK() {
	d := r.TimeIval
	_ = r.mem.Get()
	free := memFree(&r.mem)
	if free < r.lowWM || free < minMemFree {
		d >>= 1
	}
	hk.Reg(r.Name+hk.NameSuffix, r.hkcb, d)
}

// initialize new MMSA instance
func (r *MMSA) Init(maxUse int64) (err error) {
	// 1. environment overrides defaults and MMSA{...} hard-codings
	if err = r.env(); err != nil {
		cos.Errorf("%v", err)
	}

	// 2. compute min-free (must remain free at all times) and low watermark
	err = r.mem.Get()
	if err != nil {
		cos.Errorf("%v", err)
	}
	free := memFree(&r.mem)
	if r.MinPctTotal > 0 {
		x := r.mem.Total * uint64(r.MinPctTotal) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = cos.MinU64(r.MinFree, x)
		}
	}
	if r.MinPctFree > 0 {
		x := free * uint64(r.MinPctFree) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = cos.MinU64(r.MinFree, x)
		}
	}
	if maxUse > 0 {
		r.MinFree = uint64(cos.MaxI64(int64(r.MinFree), int64(free)-maxUse))
	}
	if r.MinFree == 0 {
		r.MinFree = minMemFree
	}
	r.lowWM = (r.MinFree+free)>>1 - (r.MinFree+free)>>4 // a quarter of
	r.lowWM = cos.MaxU64(r.lowWM, r.MinFree+minMemFreeTests)

	// 3. validate min-free & low-wm
	if free < cos.MinU64(r.MinFree*2, r.MinFree+minMemFree) {
		err = fmt.Errorf("insufficient free memory %s (see %s for guidance)", r.Str(&r.mem), readme)
		cos.Errorf("%v", err)
		r.lowWM = cos.MinU64(r.lowWM, r.MinFree+minMemFreeTests)
		r.info = ""
	}

	// 4. timer
	if r.TimeIval == 0 {
		r.TimeIval = memCheckAbove
	}

	// 5. final construction steps
	r.swap.size.Store(r.mem.SwapUsed)
	r.optDepth.Store(optDepth)
	r.toGC.Store(0)

	if r.defBufSize == 0 {
		// assume page
		debug.Assert(r.slabIncStep == 0 || r.slabIncStep == PageSlabIncStep)
		r.defBufSize = DefaultBufSize
		r.slabIncStep = PageSlabIncStep
	}
	r.maxSlabSize, r.numSlabs = MaxPageSlabSize, NumPageSlabs
	if !r.isPage() {
		r.maxSlabSize, r.numSlabs = MaxSmallSlabSize, NumSmallSlabs
	}
	r.slabStats = &slabStats{}
	r.statsSnapshot = &Stats{}
	r.rings = make([]*Slab, r.numSlabs)
	r.sorted = make([]*Slab, r.numSlabs)
	for i := 0; i < r.numSlabs; i++ {
		bufSize := r.slabIncStep * int64(i+1)
		slab := &Slab{
			m:       r,
			tag:     r.Name + "." + cos.ToSizeIEC(bufSize, 0),
			bufSize: bufSize,
			get:     make([][]byte, 0, optDepth),
			put:     make([][]byte, 0, optDepth),
		}
		slab.pMinDepth = &r.optDepth
		r.rings[i] = slab
		r.sorted[i] = slab
	}

	if gmm != nil {
		cos.Infof("%s started", r.Str(&r.mem))
	}
	return
}

// terminate this MMSA instance and, possibly, GC as well
func (r *MMSA) Terminate(unregHK bool) {
	var (
		freed int64
		gced  string
	)
	if unregHK {
		hk.Unreg(r.Name + hk.NameSuffix)
	}
	for _, s := range r.rings {
		freed += s.cleanup()
	}
	r.toGC.Add(freed)
	if r.doGC(sizeToGC, true) {
		gced = " (GC-ed)"
	}
	debug.Infof("%s terminated%s", r, gced)
}
