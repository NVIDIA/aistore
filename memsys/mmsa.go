// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/sys"
)

// ====================== How to run unit tests ===========================
//
// 1. Run all tests with default parameters
// $ go test -v
// 2. ... and debug enabled
// $ go test -v -tags=debug
// 3. ... and deadbeef (build tag) enabled, to "DEADBEEF" every freed buffer
// $ go test -v -tags=debug,deadbeef
// 4. Run a given named test with the specified build tags for 100s; redirect logs to STDERR
// $ go test -v -tags=debug,deadbeef -logtostderr=true -run=Test_Sleep -duration=100s

// ============== Memory Manager Slab Allocator (MMSA) ===========================
//
// MMSA is, simultaneously, a Slab and SGL allocator, and a memory manager
// responsible to optimize memory usage between different (more vs less) utilized
// Slabs.
//
// Multiple MMSA instances may coexist in the system, each having its own
// constraints and managing its own Slabs and SGLs.
//
// There will be use cases, however, when actually running a MMSA instance
// won't be necessary: e.g., when an app utilizes a single (or a few distinct)
// Slab size(s) for the duration of its relatively short lifecycle,
// while at the same time preferring minimal interference with other running apps.
//
// In that sense, a typical initialization sequence includes 2 steps, e.g.:
// 1) construct:
// 	mm := &memsys.MMSA{TimeIval: ..., MinPctFree: ..., Name: ...}
// 2) initialize:
// 	err := mm.Init()
// 	if err != nil {
//		...
// 	}
//
// To free up all memory allocated by a given MMSA instance, use its Terminate() method.
//
// In addition, there are several environment variables that can be used
// (to circumvent the need to change the code, for instance):
// 	"AIS_MINMEM_FREE"
// 	"AIS_MINMEM_PCT_TOTAL"
// 	"AIS_MINMEM_PCT_FREE"
// 	"AIS_DEBUG"
// These names must be self-explanatory.
//
// Once constructed and initialized, memory-manager-and-slab-allocator
// (MMSA) can be exercised via its public API that includes
// GetSlab() and Alloc*() methods.
//
// Once selected, each Slab instance can be used via its own public API that
// includes Alloc() and Free() methods. In addition, each allocated SGL internally
// utilizes one of the existing enumerated slabs to "grow" (that is, allocate more
// buffers from the slab) on demand. For details, look for "grow" in the iosgl.go.

const readme = "https://github.com/NVIDIA/aistore/blob/master/memsys/README.md"

const (
	PageSize            = cos.KiB * 4
	DefaultBufSize      = PageSize * 8
	DefaultSmallBufSize = cos.KiB
)

const (
	gmmName = ".dflt.mm"
	smmName = ".dflt.mm.small"
)

// page slabs: pagesize increments up to MaxPageSlabSize
const (
	MaxPageSlabSize = 128 * cos.KiB
	PageSlabIncStep = PageSize
	NumPageSlabs    = MaxPageSlabSize / PageSlabIncStep // = 32
)

// small slabs: 128 byte increments up to MaxSmallSlabSize
const (
	MaxSmallSlabSize = PageSize
	SmallSlabIncStep = 128
	NumSmallSlabs    = MaxSmallSlabSize / SmallSlabIncStep // = 32
)
const NumStats = NumPageSlabs // NOTE: must be greater or equal NumSmallSlabs, otherwise out-of-bounds at init time

// =================================== MMSA config defaults ==========================================
// The minimum memory (that must remain available) gets computed as follows:
// 1) environment AIS_MINMEM_FREE takes precedence over everything else;
// 2) if AIS_MINMEM_FREE is not defined, environment variables AIS_MINMEM_PCT_TOTAL and/or
//    AIS_MINMEM_PCT_FREE define percentages to compute the minimum based on total
//    or the currently available memory, respectively;
// 3) with no environment, the minimum is computed based on the following MMSA member variables:
//	* MinFree     uint64        // memory that must be available at all times
//	* MinPctTotal int           // same, via percentage of total
//	* MinPctFree  int           // ditto, as % of free at init time
//     (example:
//         mm := &memsys.MMSA{MinPctTotal: 4, MinFree: cos.GiB * 2}
//     )
//  4) finally, if none of the above is specified, the constant `minMemFree` below is used
//  Other important defaults are also commented below.
// =================================== MMSA config defaults ==========================================
const (
	minDepth   = 128                   // ring cap min; default ring growth increment
	loadAvg    = 10                    // "idle" load average to deallocate Slabs when below
	sizeToGC   = cos.GiB * 2           // see heuristics ("Heu")
	minMemFree = cos.GiB + cos.MiB*256 // default minimum memory - see extended comment above
)

const (
	memCheckAbove = 90 * time.Second // default memory checking frequency when above low watermark
	freeIdleMin   = memCheckAbove    // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero  = freeIdleMin * 2  // ... to zero
)

// slab constants
const (
	countThreshold = 16 // exceeding this scatter-gather count warrants selecting a larger-size Slab
)

const swappingMax = 4 // make sure that `swapping` condition, once noted, lingers for a while

// memory pressure
const (
	MemPressureLow = iota
	MemPressureModerate
	MemPressureHigh
	MemPressureExtreme
	OOM
)

type (
	Slab struct {
		m            *MMSA // parent
		bufSize      int64
		tag          string
		get, put     [][]byte
		muget, muput sync.Mutex
		pMinDepth    *atomic.Int64
		pos          int
	}
	Stats struct {
		Hits [NumStats]uint64
		Idle [NumStats]time.Duration
	}
	MMSA struct {
		// public
		Name        string
		MinFree     uint64        // memory that must be available at all times
		TimeIval    time.Duration // interval of time to watch for low memory and make steps
		MinPctTotal int           // same, via percentage of total
		MinPctFree  int           // ditto, as % of free at init time
		Sibling     *MMSA         // sibling mem manager to delegate allocations of need be
		// private
		duration      time.Duration
		lowWM         uint64
		rings         []*Slab
		sorted        []*Slab
		slabStats     *slabStats // private counters and idle timestamp
		statsSnapshot *Stats     // pre-allocated limited "snapshot" of slabStats
		slabIncStep   int64
		maxSlabSize   int64
		defBufSize    int64
		numSlabs      int
		// atomic state
		toGC            atomic.Int64  // accumulates over time and triggers GC upon reaching spec-ed limit
		minDepth        atomic.Int64  // minimum ring depth aka length
		swap            atomic.Uint64 // actual swap size
		swapCriticality atomic.Int32  // tracks increasing swap size up to swappingMax const
	}
	FreeSpec struct {
		IdleDuration time.Duration // reduce only the slabs that are idling for at least as much time
		MinSize      int64         // minimum freed size that'd warrant calling GC (default = sizetoGC)
		Totally      bool          // true: free all slabs regardless of their idle-ness and size
		ToOS         bool          // GC and then return the memory to the operating system
	}
	//
	// private
	//
	slabStats struct {
		sync.RWMutex
		hits   [NumStats]atomic.Uint64
		prev   [NumStats]uint64
		hinc   [NumStats]uint64
		idleTs [NumStats]int64
	}
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
}

// default page-based memory-manager-slab-allocator (MMSA) for packages other than `ais`
func DefaultPageMM() *MMSA {
	gmmOnce.Do(func() {
		gmm.Init(true)
		if smm != nil {
			smm.Sibling = gmm
			gmm.Sibling = smm
		}
	})
	return gmm
}

// default small-size MMSA
func DefaultSmallMM() *MMSA {
	smmOnce.Do(func() {
		smm.Init(true)
		if gmm != nil {
			gmm.Sibling = smm
			smm.Sibling = gmm
		}
	})
	return smm
}

//////////
// MMSA //
//////////

func (r *MMSA) String() string {
	minfree := cos.B2S(int64(r.MinFree), 0)
	lowwm := cos.B2S(int64(r.lowWM), 0)
	s := fmt.Sprintf("%s[min-free %s, low-wm %s", r.Name, minfree, lowwm)
	if r.MinPctTotal == 0 && r.MinPctFree == 0 {
		return s + "]"
	}
	return fmt.Sprintf("%s, %%-total %d, %%-free %d]", s, r.MinPctTotal, r.MinPctFree)
}

func (r *MMSA) MemPressure2S(p int) (s string) {
	s = fmt.Sprintf("pressure '%s'", memPressureText[p])
	if a, b := r.swap.Load(), r.swapCriticality.Load(); a != 0 || b != 0 {
		s = fmt.Sprintf("swapping(%s, criticality '%d'), %s", cos.B2S(int64(a), 0), b, s)
	}
	return
}

// defines the type of Slab rings: NumSmallSlabs x 128 vs NumPageSlabs x 4K
func (r *MMSA) isSmall() bool { return r.defBufSize == DefaultSmallBufSize }

// initialize new MMSA instance
func (r *MMSA) Init(panicOnErr bool) (err error) {
	cos.Assert(r.Name != "")
	// 1. environment overrides defaults and MMSA{...} hard-codings
	if err = r.env(); err != nil {
		if panicOnErr {
			panic(err)
		}
	}
	// 2. compute minFree - mem size that must remain free at all times
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
	if r.MinFree == 0 {
		r.MinFree = minMemFree
	} else if r.MinFree < cos.GiB { // warn invalid config
		cos.Printf("Warning: configured minimum free memory %s < %s (actual free %s)\n",
			cos.B2S(int64(r.MinFree), 2), cos.B2S(minMemFree, 2), cos.B2S(int64(mem.Free), 1))
	}
	// 3. validate actual
	required, actual := cos.B2S(int64(r.MinFree), 2), cos.B2S(int64(mem.Free), 2)
	if mem.Free < r.MinFree {
		err = fmt.Errorf("insufficient free memory %s, minimum required %s (see %s for guidance)",
			actual, required, readme)
		if panicOnErr {
			panic(err)
		}
	}
	x := cos.MaxU64(r.MinFree*2, (r.MinFree+mem.Free)/2)
	r.lowWM = cos.MinU64(x, r.MinFree*3) // Heu #1: hysteresis

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
	// 7. HK to call back (and maybe further tune-up the interval)
	d := r.duration
	if mem.Free < r.lowWM || mem.Free < minMemFree {
		d = r.duration / 4
		if d < 10*time.Second {
			d = 10 * time.Second
		}
	}
	hk.Reg(r.Name+".gc", r.garbageCollect, d)
	debug.Func(func() {
		if flag.Parsed() {
			debug.Infof("%s started", r)
		}
	})
	return
}

// terminate this MMSA instance and, possibly, GC as well
func (r *MMSA) Terminate() {
	var (
		freed int64
		gced  string
	)
	hk.Unreg(r.Name + ".gc")
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

// allocate SGL
// - immediateSize: known size, OR minimum expected size, OR size to preallocate
//   immediateSize == 0 translates as DefaultBufSize - for page MMSA,
//   and DefaultSmallBufSize - for small-size MMSA
// - sbufSize: slab buffer size (optional)
func (r *MMSA) NewSGL(immediateSize int64, sbufSize ...int64) *SGL {
	var (
		slab *Slab
		n    int64
		err  error
	)
	// 1. slab
	if len(sbufSize) > 0 {
		slab, err = r.GetSlab(sbufSize[0])
	} else if immediateSize <= r.maxSlabSize {
		// NOTE allocate imm. size in one shot when below max
		if immediateSize == 0 {
			immediateSize = r.defBufSize
		}
		i := cos.DivCeil(immediateSize, r.slabIncStep)
		slab = r.rings[i-1]
	} else {
		slab = r._large2slab(immediateSize)
	}
	debug.AssertNoErr(err)

	// 2. sgl
	z := _allocSGL()
	z.slab = slab
	n = cos.DivCeil(immediateSize, slab.Size())
	if cap(z.sgl) < int(n) {
		z.sgl = make([][]byte, n)
	} else {
		z.sgl = z.sgl[:n]
	}
	slab.muget.Lock()
	for i := 0; i < int(n); i++ {
		z.sgl[i] = slab._alloc()
	}
	slab.muget.Unlock()
	return z
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
	x := (mem.Free - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x <= 25 {
		return MemPressureHigh, swapping
	}
	return MemPressureModerate, swapping
}

// gets Slab for a given fixed buffer size that must be within expected range of sizes
// - the range supported by _this_ MMSA (compare w/ SelectMemAndSlab())
func (r *MMSA) GetSlab(bufSize int64) (s *Slab, err error) {
	a, b := bufSize/r.slabIncStep, bufSize%r.slabIncStep
	if b != 0 {
		err = fmt.Errorf("size %d must be a multiple of %d", bufSize, r.slabIncStep)
		return
	}
	if a < 1 || a > int64(r.numSlabs) {
		err = fmt.Errorf("size %d outside valid range", bufSize)
		return
	}
	s = r.rings[a-1]
	return
}

// uses SelectMemAndSlab to select both MMSA (page or small) and its Slab
func (r *MMSA) AllocSize(size int64) (buf []byte, slab *Slab) {
	_, slab = r.SelectMemAndSlab(size)
	buf = slab.Alloc()
	return
}

func (r *MMSA) Alloc() (buf []byte, slab *Slab) {
	size := r.defBufSize
	_, slab = r.SelectMemAndSlab(size)
	buf = slab.Alloc()
	return
}

func (r *MMSA) Free(buf []byte) {
	size := int64(cap(buf))
	if size > r.maxSlabSize && r.isSmall() {
		r.Sibling.Free(buf)
	} else if size < r.slabIncStep && !r.isSmall() {
		r.Sibling.Free(buf)
	} else {
		debug.Assert(size%r.slabIncStep == 0)
		debug.Assert(size/r.slabIncStep <= int64(r.numSlabs))

		slab := r._selectSlab(size)
		slab.Free(buf)
	}
}

// Given a known, expected or minimum size to allocate, selects MMSA (page or small, if initialized)
// and its Slab
func (r *MMSA) SelectMemAndSlab(size int64) (mmsa *MMSA, slab *Slab) {
	if size > r.maxSlabSize && r.isSmall() {
		return r.Sibling, r.Sibling._selectSlab(size)
	}
	if size < r.slabIncStep && !r.isSmall() {
		return r.Sibling, r.Sibling._selectSlab(size)
	}
	mmsa, slab = r, r._selectSlab(size)
	return
}

func (r *MMSA) _selectSlab(size int64) (slab *Slab) {
	if size >= r.maxSlabSize {
		slab = r.rings[len(r.rings)-1]
	} else if size <= r.slabIncStep {
		slab = r.rings[0]
	} else {
		i := (size + r.slabIncStep - 1) / r.slabIncStep
		slab = r.rings[i-1]
	}
	return
}

func (r *MMSA) Append(buf []byte, bytes string) (nbuf []byte) {
	var (
		ll, l, c = len(buf), len(bytes), cap(buf)
		a        = ll + l - c
	)
	if a > 0 {
		nbuf, _ = r.AllocSize(int64(c + a))
		copy(nbuf, buf)
		r.Free(buf)
		nbuf = nbuf[:ll+l]
	} else {
		nbuf = buf[:ll+l]
	}
	copy(nbuf[ll:], bytes)
	return
}

// private

// select slab for SGL given a large immediate size to allocate
func (r *MMSA) _large2slab(immediateSize int64) *Slab {
	size := cos.DivCeil(immediateSize, countThreshold)
	for _, slab := range r.rings {
		if slab.Size() >= size {
			return slab
		}
	}
	return r.rings[len(r.rings)-1]
}

func (r *MMSA) env() (err error) {
	var minfree int64
	if a := os.Getenv("AIS_MINMEM_FREE"); a != "" {
		if minfree, err = cos.S2B(a); err != nil {
			return fmt.Errorf("cannot parse AIS_MINMEM_FREE %q", a)
		}
		r.MinFree = uint64(minfree)
	}
	if a := os.Getenv("AIS_MINMEM_PCT_TOTAL"); a != "" {
		if r.MinPctTotal, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("cannot parse AIS_MINMEM_PCT_TOTAL %q", a)
		}
		if r.MinPctTotal < 0 || r.MinPctTotal > 100 {
			return fmt.Errorf("invalid AIS_MINMEM_PCT_TOTAL %q", a)
		}
	}
	if a := os.Getenv("AIS_MINMEM_PCT_FREE"); a != "" {
		if r.MinPctFree, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("cannot parse AIS_MINMEM_PCT_FREE %q", a)
		}
		if r.MinPctFree < 0 || r.MinPctFree > 100 {
			return fmt.Errorf("invalid AIS_MINMEM_PCT_FREE %q", a)
		}
	}
	return
}

//////////
// Slab //
//////////

func (s *Slab) Size() int64 { return s.bufSize }
func (s *Slab) Tag() string { return s.tag }
func (s *Slab) MMSA() *MMSA { return s.m }

func (s *Slab) Alloc() (buf []byte) {
	s.muget.Lock()
	buf = s._alloc()
	s.muget.Unlock()
	return
}

func (s *Slab) Free(buf []byte) {
	s.muput.Lock()
	debug.Assert(int64(cap(buf)) == s.Size())
	deadbeef(buf[:cap(buf)])
	s.put = append(s.put, buf[:cap(buf)]) // always freeing the original size
	s.muput.Unlock()
}

func (s *Slab) _alloc() (buf []byte) {
	if len(s.get) > s.pos { // fast path
		buf = s.get[s.pos]
		s.pos++
		s.hitsInc()
		return
	}
	return s._allocSlow()
}

func (s *Slab) _allocSlow() (buf []byte) {
	curMinDepth := int(s.pMinDepth.Load())
	debug.Assert(curMinDepth > 0)
	debug.Assert(len(s.get) == s.pos)
	s.muput.Lock()
	lput := len(s.put)
	if cnt := curMinDepth - lput; cnt > 0 {
		s.grow(cnt)
	}
	s.get, s.put = s.put, s.get

	debug.Assert(len(s.put) == s.pos)
	debug.Assert(len(s.get) >= curMinDepth)

	s.put = s.put[:0]
	s.muput.Unlock()

	s.pos = 0
	buf = s.get[s.pos]
	s.pos++
	s.hitsInc()
	return
}

func (s *Slab) grow(cnt int) {
	if verbose {
		glog.Infof("%s: grow by %d => %d", s.tag, cnt, len(s.put)+cnt)
	}
	for ; cnt > 0; cnt-- {
		buf := make([]byte, s.Size())
		s.put = append(s.put, buf)
	}
}

func (s *Slab) reduce(todepth int, isIdle, force bool) (freed int64) {
	s.muput.Lock()
	lput := len(s.put)
	cnt := lput - todepth
	if isIdle {
		if force {
			cnt = cos.Max(cnt, lput/2) // Heu #6
		} else {
			cnt = cos.Min(cnt, lput/2) // Heu #7
		}
	}
	if cnt > 0 {
		if verbose {
			glog.Infof("%s(f=%t, i=%t): reduce lput %d => %d", s.tag, force, isIdle, lput, lput-cnt)
		}
		for ; cnt > 0; cnt-- {
			lput--
			s.put[lput] = nil
			freed += s.Size()
		}
		s.put = s.put[:lput]
	}
	s.muput.Unlock()

	s.muget.Lock()
	lget := len(s.get) - s.pos
	cnt = lget - todepth
	if isIdle {
		if force {
			cnt = cos.Max(cnt, lget/2) // Heu #9
		} else {
			cnt = cos.Min(cnt, lget/2) // Heu #10
		}
	}
	if cnt > 0 {
		if verbose {
			glog.Infof("%s(f=%t, i=%t): reduce lget %d => %d", s.tag, force, isIdle, lget, lget-cnt)
		}
		for ; cnt > 0; cnt-- {
			s.get[s.pos] = nil
			s.pos++
			freed += s.Size()
		}
	}
	s.muget.Unlock()
	return
}

func (s *Slab) cleanup() (freed int64) {
	s.muget.Lock()
	s.muput.Lock()
	for i := s.pos; i < len(s.get); i++ {
		s.get[i] = nil
		freed += s.Size()
	}
	for i := range s.put {
		s.put[i] = nil
		freed += s.Size()
	}
	s.get = s.get[:0]
	s.put = s.put[:0]
	s.pos = 0

	debug.Assert(len(s.get) == 0 && len(s.put) == 0)
	s.muput.Unlock()
	s.muget.Unlock()
	return
}

func (s *Slab) ringIdx() int { return int(s.bufSize/s.m.slabIncStep) - 1 }
func (s *Slab) hitsInc()     { s.m.slabStats.hits[s.ringIdx()].Inc() }
func (s *Slab) idleDur(statsSnapshot *Stats) (d time.Duration) {
	idx := s.ringIdx()
	d = statsSnapshot.Idle[idx]
	if d == 0 {
		return
	}
	if statsSnapshot.Hits[idx] != s.m.slabStats.hits[idx].Load() {
		d = 0
	}
	return
}
