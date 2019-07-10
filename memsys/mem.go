// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"flag"
	"fmt"
	"hash"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/sys"
)

// ===================== Theory Of Operations (TOO) =============================
//
// Mem2 is, simultaneously, a) Slab and SGL allocator, and b) memory manager
// responsible to optimize memory usage between different (more vs less) utilized
// Slabs.
//
// Multiple Mem2 instances may coexist in the system, each having its own
// constraints and managing its own Slabs and SGLs.
//
// There will be use cases, however, when actually running a Mem2 instance
// won't be necessary: e.g., when an app utilizes a single (or a few distinct)
// Slab size(s) for the duration of its relatively short lifecycle,
// while at the same time preferring minimal interference with other running apps.
//
// In that sense, a typical initialization sequence includes 2 steps, e.g.:
// 1) construct:
// 	mem2 := &memsys.Mem2{TimeIval: ..., MinPctFree: ..., Name: ..., Debug: ...}
// 2) initialize:
// 	err := mem2.Init()
// 	if err != nil {
//		...
// 	}
//
// Mem register itself in hk which will periodically call garbageCollect
// function which tries to free reclaim unused memory.
//
// Release:
//
// To clean up a Mem2 instance after it is used, call Release() on the Mem2 instance.
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
// (Mem2, for shortness) can be exercised via its public API that includes
// GetSlab2(), SelectSlab2() and AllocEstimated().
//
// NOTE the difference between the first and the second:
// - GetSlab2(memsys.MaxSlabSize) returns the Slab of MaxSlabSize buffers
// - SelectSlab2(256 * 1024) - the Slab that is considered optimal
//   for the estimated *total* size 256KB.
//
// Once selected, each Slab2 instance can be used via its own public API that
// includes Alloc() and Free() methods. In addition, each allocated SGL internally
// utilizes one of the existing enumerated slabs to "grow" (that is, allocate more
// buffers from the slab) on demand. For details, look for "grow" in the iosgl.go.
//
// Near `go build` or `go install` one can find `GODEBUG=madvdontneed=1`. This
// is to revert behavior which was changed in 1.12: https://golang.org/doc/go1.12#runtime
// More specifically Go maintainers decided to use new `MADV_FREE` flag which
// may prevent memory being freed to kernel in case Go's runtime decide to run
// GC. But this doesn't stop kernel to kill the process in case of OOM - it is
// quite grotesque, kernel decides not to get pages from the process and at the
// same time it may kill it for not returning pages... Since memory is critical
// in couple of operations we need to make sure that we it is returned to the
// kernel so `aisnode` will not blow up and this is why `madvdontneed` is
// needed. More: https://golang.org/src/runtime/mem_linux.go (func sysUnused,
// lines 100-120).
//
// ========================== end of TOO ========================================

const (
	MaxSlabSize = 128 * cmn.KiB
	NumSlabs    = MaxSlabSize / cmn.PageSize // [4K - 256K] at 4K increments

	deadBEEF       = "DEADBEEF"
	globalMem2Name = "GMem2"
	pkgName        = "memsys"
)

// mem subsystem defaults (potentially, tunables)
const (
	minDepth      = 128             // ring cap min; default ring growth increment
	maxDepth      = 1024 * 24       // ring cap max
	sizeToGC      = cmn.GiB * 2     // see heuristics ("Heu")
	loadAvg       = 10              // "idle" load average to deallocate Slabs when below
	minMemFree    = cmn.GiB         // default minimum memory size that must remain available - see AIS_MINMEM_*
	memCheckAbove = time.Minute     // default memory checking frequency when above low watermark (see lowwm, setTimer())
	freeIdleMin   = memCheckAbove   // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero  = freeIdleMin * 2 // ... to zero
)

// slab constants
const (
	countThreshold = 32 // exceeding this scatter-gather count warrants selecting a larger-size Slab
	minSizeUnknown = 32 * cmn.KiB
)

const SwappingMax = 4 // make sure that `swapping` condition, once noted, lingers for a while
const (
	MemPressureLow = iota
	MemPressureModerate
	MemPressureHigh
	MemPressureExtreme
	OOM
)

var memPressureText = map[int]string{
	MemPressureLow:      "low",
	MemPressureModerate: "moderate",
	MemPressureHigh:     "high",
	MemPressureExtreme:  "extreme",
	OOM:                 "OOM",
}

//
// API types
//
type (
	Slab2 struct {
		m *Mem2 // pointer to the parent/creator

		bufSize      int64
		tag          string
		get, put     [][]byte
		muget, muput sync.Mutex
		stats        struct {
			Hits atomic.Int64
		}
		pMinDepth *atomic.Int64
		pos       int
		debug     bool
	}
	Stats2 struct {
		Hits    [NumSlabs]int64
		Adeltas [NumSlabs]int64
		Idle    [NumSlabs]time.Time
	}
	ReqStats2 struct {
		Wg    *sync.WaitGroup
		Stats *Stats2
	}
	Mem2 struct {
		time struct {
			d time.Duration
		}
		lowWM    uint64
		rings    [NumSlabs]*Slab2
		stats    Stats2
		sorted   []sortpair
		toGC     atomic.Int64 // accumulates over time and triggers GC upon reaching the spec-ed limit
		minDepth atomic.Int64 // minimum ring depth aka length
		// for user to specify at construction time
		Name        string
		MinFree     uint64        // memory that must be available at all times
		TimeIval    time.Duration // interval of time to watch for low memory and make steps
		swap        atomic.Uint64 // actual swap size
		Swapping    atomic.Int32  // max = SwappingMax; halves every r.time.d unless swapping
		MinPctTotal int           // same, via percentage of total
		MinPctFree  int           // ditto, as % of free at init time
		Debug       bool
	}
	FreeSpec struct {
		IdleDuration time.Duration // reduce only the slabs that are idling for at least as much time
		MinSize      int64         // minimum freed size that'd warrant calling GC (default = sizetoGC)
		Totally      bool          // true: free all slabs regardless of their idle-ness and size
		ToOS         bool          // GC and then return the memory to the operating system
	}
)

// private types
type sortpair struct {
	s *Slab2
	v int64
}

var (
	// gmm is the global memory manager used in various packages outside ais.
	// Its runtime params are set below and is intended to remain so.
	gmm     = &Mem2{Name: globalMem2Name, TimeIval: time.Minute * 2, MinPctFree: 50}
	gmmOnce sync.Once // ensures that there is only one initialization
)

// Global memory manager getter
func GMM() *Mem2 {
	gmmOnce.Do(func() {
		_ = gmm.Init(true /*panicOnErr*/)
	})
	return gmm
}

//
// API methods
//

func (r *Mem2) NewSGL(immediateSize int64 /* size to preallocate */, sbufSize ...int64 /* slab buffer size */) *SGL {
	var (
		slab *Slab2
		n    int64
		sgl  [][]byte
	)
	if len(sbufSize) > 0 {
		var err error
		slab, err = r.GetSlab2(sbufSize[0])
		cmn.AssertNoErr(err)
	} else {
		slab = r.SelectSlab2(immediateSize)
	}
	n = cmn.DivCeil(immediateSize, slab.Size())
	sgl = make([][]byte, n)

	slab.muget.Lock()
	for i := 0; i < int(n); i++ {
		sgl[i] = slab._alloc()
	}
	slab.muget.Unlock()
	return &SGL{sgl: sgl, slab: slab}
}

func (r *Mem2) NewSGLWithHash(immediateSize int64, hash hash.Hash64, sbufSize ...int64) *SGL {
	sgl := r.NewSGL(immediateSize, sbufSize...)
	sgl.hash = hash
	return sgl
}

// returns an estimate for the current memory pressured expressed as one of the enumerated values
func (r *Mem2) MemPressure() int {
	mem, _ := sys.Mem()
	if mem.SwapUsed > r.swap.Load() {
		r.Swapping.Store(SwappingMax)
	}
	if r.Swapping.Load() > 0 {
		return OOM
	}
	if mem.ActualFree > r.lowWM {
		return MemPressureLow
	}
	if mem.ActualFree <= r.MinFree {
		return MemPressureExtreme
	}
	x := (mem.ActualFree - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x <= 25 {
		return MemPressureHigh
	}
	return MemPressureModerate
}
func MemPressureText(v int) string { return memPressureText[v] }

// NOTE: On error behavior is defined by the panicOnErr argument
//  true:  proceed regardless and return error
//  false: panic with error message
func (r *Mem2) Init(panicOnErr bool) (err error) {
	cmn.Assert(r.Name != "")

	// 1. environment overrides defaults and Mem2{...} hard-codings
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
			r.MinFree = cmn.MinU64(r.MinFree, x)
		}
	}
	if r.MinPctFree > 0 {
		x := mem.ActualFree * uint64(r.MinPctFree) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = cmn.MinU64(r.MinFree, x)
		}
	}
	if r.MinFree == 0 {
		r.MinFree = minMemFree
	}
	if r.MinFree < cmn.GiB {
		if flag.Parsed() {
			glog.Warningf("Warning: configured minimum free memory %s < 1GiB (actual free %s)\n",
				cmn.B2S(int64(r.MinFree), 2), cmn.B2S(int64(mem.ActualFree), 1))
		}
	}
	// 3. validate and compute free memory "low watermark"
	m, f := cmn.B2S(int64(r.MinFree), 2), cmn.B2S(int64(mem.ActualFree), 2)
	if mem.ActualFree < r.MinFree {
		err = fmt.Errorf("insufficient free memory %s, minimum required %s", f, m)
		if panicOnErr {
			panic(err)
		}
	}
	x := cmn.MaxU64(r.MinFree*2, (r.MinFree+mem.ActualFree)/2)
	r.lowWM = cmn.MinU64(x, r.MinFree*3) // Heu #1: hysteresis

	// 4. timer
	if r.TimeIval == 0 {
		r.TimeIval = memCheckAbove
	}
	r.time.d = r.TimeIval

	// 5. final construction steps
	r.sorted = make([]sortpair, NumSlabs)
	r.swap.Store(mem.SwapUsed)
	r.minDepth.Store(minDepth)
	r.toGC.Store(0)

	// init slabs
	for i := range &r.rings {
		bufSize := int64(cmn.PageSize * (i + 1))
		slab := &Slab2{
			m:       r,
			tag:     r.Name + "." + cmn.B2S(bufSize, 0),
			bufSize: bufSize,
			get:     make([][]byte, 0, minDepth),
			put:     make([][]byte, 0, minDepth),
		}
		slab.pMinDepth = &r.minDepth
		slab.debug = r.Debug
		r.rings[i] = slab
	}

	// 6. always GC at init time
	runtime.GC()
	hk.Housekeeper.Register(r.Name+".gc", r.garbageCollect)
	return
}

// on-demand memory freeing to the user-provided specification
func (r *Mem2) Free(spec FreeSpec) {
	var freed int64
	if spec.Totally {
		for _, s := range &r.rings {
			freed += s.cleanup()
		}
	} else {
		if spec.IdleDuration == 0 {
			spec.IdleDuration = freeIdleMin // using default
		}

		currStats := r.GetStats()
		for i, idle := range &currStats.Idle {
			if idle.IsZero() {
				continue
			}
			elapsed := time.Since(idle)
			if elapsed > spec.IdleDuration {
				s := r.rings[i]
				x := s.cleanup()
				freed += x
				if x > 0 && bool(glog.FastV(4, glog.SmoduleMemsys)) {
					glog.Infof("%s: idle for %v - cleanup", s.tag, elapsed)
				}
			}
		}
	}
	if freed > 0 {
		r.toGC.Add(freed)
		if spec.MinSize == 0 {
			spec.MinSize = sizeToGC // using default
		}
		mem, _ := sys.Mem()
		r.doGC(mem.ActualFree, spec.MinSize, spec.ToOS /* force */, false)
	}
}

func (r *Mem2) GetSlab2(bufSize int64) (s *Slab2, err error) {
	a, b := bufSize/cmn.PageSize, bufSize%cmn.PageSize
	if b != 0 {
		err = fmt.Errorf("buf_size %d must be multiple of 4K", bufSize)
		return
	}
	if a < 1 || a > NumSlabs {
		err = fmt.Errorf("buf_size %d outside valid range", bufSize)
		return
	}
	s = r.rings[a-1]
	return
}

func (r *Mem2) SelectSlab2(estimatedSize int64) *Slab2 {
	if estimatedSize == 0 {
		estimatedSize = minSizeUnknown
	}
	size := cmn.DivCeil(estimatedSize, countThreshold)
	for _, slab := range &r.rings {
		if slab.Size() >= size {
			return slab
		}
	}
	return r.rings[len(r.rings)-1]
}

func (r *Mem2) AllocEstimated(estimSize int64) (buf []byte, slab *Slab2) {
	slab = r.SelectSlab2(estimSize)
	buf = slab.Alloc()
	return
}
func (r *Mem2) AllocForSize(size int64) (buf []byte, slab *Slab2) {
	if size >= MaxSlabSize {
		slab = r.rings[len(r.rings)-1]
	} else if size <= cmn.PageSize {
		slab = r.rings[0]
	} else {
		a := (size + cmn.PageSize - 1) / cmn.PageSize
		slab = r.rings[a-1]
	}
	buf = slab.Alloc()
	return
}

func (r *Mem2) GetStats() *Stats2 {
	now := time.Now()
	for i, s := range &r.rings {
		prev := r.stats.Hits[i]
		r.stats.Hits[i] = s.stats.Hits.Load()
		r.stats.Adeltas[i] = r.stats.Hits[i] - prev
		isZero := r.stats.Idle[i].IsZero()
		if r.stats.Adeltas[i] == 0 && isZero {
			r.stats.Idle[i] = now
		} else if r.stats.Adeltas[i] > 0 && !isZero {
			r.stats.Idle[i] = time.Time{}
		}
	}

	resp := &Stats2{}
	for i := 0; i < NumSlabs; i++ {
		resp.Hits[i] = r.stats.Hits[i]
		resp.Adeltas[i] = r.stats.Adeltas[i]
		resp.Idle[i] = r.stats.Idle[i]
	}

	return resp
}

///////////////
// Slab2 API //
///////////////

func (s *Slab2) Size() int64 { return s.bufSize }
func (s *Slab2) Tag() string { return s.tag }

func (s *Slab2) Alloc() (buf []byte) {
	s.muget.Lock()
	buf = s._alloc()
	s.muget.Unlock()
	return
}

func (s *Slab2) Free(bufs ...[]byte) {
	// NOTE: races are expected between getting length of the `s.put` slice
	// and putting something in it. But since `maxdepth` is not hard limit,
	// we cannot ever exceed we are trading this check in favor of maybe bigger
	// slices. Also freeing buffers to the same slab at the same point in time
	// is rather unusual we don't expect this happen often.
	if len(s.put) < maxDepth {
		s.muput.Lock()
		for _, buf := range bufs {
			if s.debug {
				cmn.Assert(int64(len(buf)) == s.Size())
				for i := 0; i < len(buf); i += len(deadBEEF) {
					copy(buf[i:], deadBEEF)
				}
			}
			s.put = append(s.put, buf)
		}
		s.muput.Unlock()
	} else {
		// When we just discard buffer, since the `s.put` cache is full, then
		// we need to remember how much memory we discarded and take it into
		// account when determining if we should return memory to the system.
		s.m.toGC.Add(s.Size() * int64(len(bufs)))
	}
}

/////////////////////
// PRIVATE METHODS //
/////////////////////

func (r *Mem2) env() (err error) {
	var minfree int64
	if a := os.Getenv("AIS_MINMEM_FREE"); a != "" {
		if minfree, err = cmn.S2B(a); err != nil {
			return fmt.Errorf("cannot parse AIS_MINMEM_FREE '%s'", a)
		}
		r.MinFree = uint64(minfree)
	}
	if a := os.Getenv("AIS_MINMEM_PCT_TOTAL"); a != "" {
		if r.MinPctTotal, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("cannot parse AIS_MINMEM_PCT_TOTAL '%s'", a)
		}
		if r.MinPctTotal < 0 || r.MinPctTotal > 100 {
			return fmt.Errorf("invalid AIS_MINMEM_PCT_TOTAL '%s'", a)
		}
	}
	if a := os.Getenv("AIS_MINMEM_PCT_FREE"); a != "" {
		if r.MinPctFree, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("cannot parse AIS_MINMEM_PCT_FREE '%s'", a)
		}
		if r.MinPctFree < 0 || r.MinPctFree > 100 {
			return fmt.Errorf("invalid AIS_MINMEM_PCT_FREE '%s'", a)
		}
	}
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		r.Debug = true
		glog.SetV(glog.SmoduleMemsys, logLvl)
	}
	return
}

// NOTE: notice enumerated heuristics
func (r *Mem2) garbageCollect() time.Duration {
	var (
		depth int               // => current ring depth tbd
		limit = int64(sizeToGC) // minimum accumulated size that triggers GC
	)
	mem, _ := sys.Mem()
	swapping := mem.SwapUsed > r.swap.Load()
	if swapping {
		r.Swapping.Store(SwappingMax)
	} else {
		r.Swapping.Store(r.Swapping.Load() / 2)
	}
	r.swap.Store(mem.SwapUsed)

	// 1. enough => free idle
	if mem.ActualFree > r.lowWM && !swapping {
		r.minDepth.Store(minDepth)
		if delta := r.freeIdle(freeIdleMin); delta > 0 {
			r.toGC.Add(delta)
			r.doGC(mem.ActualFree, sizeToGC, false, false)
		}
		goto timex
	}
	if mem.ActualFree <= r.MinFree || swapping { // 2. mem too low indicates "high watermark"
		depth = minDepth / 4
		if mem.ActualFree < r.MinFree {
			depth = minDepth / 8
		}
		if swapping {
			depth = 1
		}
		r.minDepth.Store(int64(depth))
		limit = sizeToGC / 2
	} else { // 3. in-between hysteresis
		x := uint64(maxDepth-minDepth) * (mem.ActualFree - r.MinFree)
		depth = minDepth + int(x/(r.lowWM-r.MinFree)) // Heu #2
		cmn.Dassert(depth >= minDepth && depth <= maxDepth, pkgName)
		r.minDepth.Store(minDepth / 4)
	}
	// idle first
	for i, s := range &r.rings {
		r.sorted[i] = sortpair{s, r.stats.Adeltas[i]}
	}
	sort.Slice(r.sorted, r.fsort)
	for _, pair := range r.sorted {
		s := pair.s
		if delta := s.reduce(depth, pair.v == 0 /* idle */, true /* force */); delta > 0 {
			if r.doGC(mem.ActualFree, limit, true, swapping) {
				goto timex
			}
		}
	}
	// 4. red
	if mem.ActualFree <= r.MinFree || swapping {
		r.doGC(mem.ActualFree, limit, true, swapping)
	}
timex:
	return r.getNextInterval(mem.ActualFree, mem.Total, swapping)
}

func (r *Mem2) getNextInterval(free, total uint64, swapping bool) time.Duration {
	var changed bool
	switch {
	case free > r.lowWM && free > total-total/5:
		if r.time.d != r.TimeIval*2 {
			r.time.d = r.TimeIval * 2
			changed = true
		}
	case free <= r.MinFree || swapping:
		if r.time.d != r.TimeIval/4 {
			r.time.d = r.TimeIval / 4
			changed = true
		}
	case free <= r.lowWM:
		if r.time.d != r.TimeIval/2 {
			r.time.d = r.TimeIval / 2
			changed = true
		}
	default:
		if r.time.d != r.TimeIval {
			r.time.d = r.TimeIval
			changed = true
		}
	}
	if changed && bool(glog.FastV(4, glog.SmoduleMemsys)) {
		glog.Infof("timer %v, free %s", r.time.d, cmn.B2S(int64(free), 1))
	}
	return r.time.d
}

// freeIdle traverses and deallocates idle slabs- those that were not used for at
// least the specified duration; returns freed size
func (r *Mem2) freeIdle(duration time.Duration) (freed int64) {
	for i, idle := range &r.stats.Idle {
		if idle.IsZero() {
			continue
		}
		elapsed := time.Since(idle)
		if elapsed > duration {
			s := r.rings[i]
			if elapsed > freeIdleZero {
				x := s.cleanup()
				freed += x
				if x > 0 && glog.FastV(4, glog.SmoduleMemsys) {
					glog.Infof("%s: idle for %v - cleanup", s.tag, elapsed)
				}
			} else {
				x := s.reduce(minDepth, true /* idle */, false /* force */)
				freed += x
				if x > 0 && (bool(glog.FastV(4, glog.SmoduleMemsys)) || r.Debug) {
					glog.Infof("%s: idle for %v - reduced %s", s.tag, elapsed, cmn.B2S(x, 1))
				}
			}
		}
	}
	return
}

// The method is called in a few different cases:
// 1) upon periodic freeing of idle slabs
// 2) after forceful reduction of the /less/ active slabs (done when memory is running low)
// 3) on demand via Mem2.Free()
func (r *Mem2) doGC(free uint64, minsize int64, force, swapping bool) (gced bool) {
	avg, err := sys.LoadAverage()
	if err != nil {
		glog.Errorf("Failed to load averages, err: %v", err)
		avg.One = 999 // fall thru on purpose
	}
	if avg.One > loadAvg && !force && !swapping { // Heu #3
		return
	}
	toGC := r.toGC.Load()
	if toGC > minsize {
		str := fmt.Sprintf(
			"GC(force: %t, swapping: %t); load: %.2f; free: %s; toGC_size: %s",
			force, swapping, avg.One, cmn.B2S(int64(free), 1), cmn.B2S(toGC, 2),
		)
		if force || swapping { // Heu #4
			glog.Warningf("%s - freeing memory to the OS...", str)
			cmn.FreeMemToOS() // forces GC followed by an attempt to return memory to the OS
		} else { // Heu #5
			if glog.FastV(4, glog.SmoduleMemsys) {
				glog.Infof(str)
			}
			runtime.GC()
		}
		gced = true
		r.toGC.Store(0)
	}
	return
}

func (r *Mem2) fsort(i, j int) bool {
	return r.sorted[i].v < r.sorted[j].v
}

func (r *Mem2) Release() {
	hk.Housekeeper.Unregister(r.Name + ".gc")
	for _, s := range &r.rings {
		_ = s.cleanup()
	}
}

func (s *Slab2) _alloc() (buf []byte) {
	if len(s.get) > s.pos { // fast path
		buf = s.get[s.pos]
		s.pos++
		s.stats.Hits.Inc()
		return
	}
	return s._allocSlow()
}

func (s *Slab2) _allocSlow() (buf []byte) {
	curMinDepth := s.pMinDepth.Load()
	if curMinDepth == 0 {
		curMinDepth = 1
	}

	cmn.Dassert(len(s.get) == s.pos, pkgName)

	s.muput.Lock()
	lput := len(s.put)
	if cnt := int(curMinDepth) - lput; cnt > 0 {
		s.grow(cnt)
	}
	s.get, s.put = s.put, s.get

	cmn.Dassert(len(s.put) == s.pos, pkgName)
	cmn.Dassert(len(s.get) >= int(curMinDepth), pkgName)

	s.put = s.put[:0]
	s.muput.Unlock()

	s.pos = 0
	buf = s.get[s.pos]
	s.pos++
	s.stats.Hits.Inc()
	return
}

func (s *Slab2) grow(cnt int) {
	if bool(glog.FastV(4, glog.SmoduleMemsys)) || s.debug {
		lput := len(s.put)
		glog.Infof("%s: grow by %d => %d", s.tag, cnt, lput+cnt)
	}
	for ; cnt > 0; cnt-- {
		buf := make([]byte, s.Size())
		s.put = append(s.put, buf)
	}
}

func (s *Slab2) reduce(todepth int, isidle, force bool) (freed int64) {
	s.muput.Lock()
	lput := len(s.put)
	cnt := lput - todepth
	if isidle {
		if force {
			cnt = cmn.Max(cnt, lput/2) // Heu #6
		} else {
			cnt = cmn.Min(cnt, lput/2) // Heu #7
		}
	}
	if cnt > 0 {
		if bool(glog.FastV(4, glog.SmoduleMemsys)) || s.debug {
			glog.Infof("%s(f=%t, i=%t): reduce lput %d => %d", s.tag, force, isidle, lput, lput-cnt)
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
	if isidle {
		if force {
			cnt = cmn.Max(cnt, lget/2) // Heu #9
		} else {
			cnt = cmn.Min(cnt, lget/2) // Heu #10
		}
	}
	if cnt > 0 {
		if bool(glog.FastV(4, glog.SmoduleMemsys)) || s.debug {
			glog.Infof("%s(f=%t, i=%t): reduce lget %d => %d", s.tag, force, isidle, lget, lget-cnt)
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

func (s *Slab2) cleanup() (freed int64) {
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

	cmn.Dassert(len(s.get) == 0 && len(s.put) == 0, pkgName)
	s.muput.Unlock()
	s.muget.Unlock()
	return
}
