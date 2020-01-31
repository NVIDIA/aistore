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
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/sys"
)

// ================= Memory Manager Slab Allocator =============================
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
// 	mm := &memsys.MMSA{TimeIval: ..., MinPctFree: ..., Name: ..., Debug: ...}
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

const readme = "https://github.com/NVIDIA/aistore/blob/master/memsys/README.md"
const (
	PageSize            = cmn.KiB * 4
	DefaultBufSize      = PageSize * 8
	DefaultSmallBufSize = cmn.KiB
)

const (
	defaultPageMM  = "DefaultPageMM"
	defaultSmallMM = "DefaultSmallMM"
	pkgName        = "memsys"
	deadBEEF       = "DEADBEEF" // debug mode
)

// page slabs: pagesize increments up to MaxPageSlabSize
const (
	MaxPageSlabSize = 128 * cmn.KiB
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
//         mm := &memsys.MMSA{MinPctTotal: 4, MinFree: cmn.GiB * 2}
//     )
//  4) finally, if none of the above is specified, the constant `minMemFree` below is used
//  Other important defaults are also commented below.
// =================================== MMSA config defaults ==========================================
const (
	minDepth      = 128                   // ring cap min; default ring growth increment
	maxDepth      = 1024 * 24             // ring cap max
	loadAvg       = 10                    // "idle" load average to deallocate Slabs when below
	sizeToGC      = cmn.GiB * 2           // see heuristics ("Heu")
	minMemFree    = cmn.GiB + cmn.MiB*256 // default minimum memory - see extended comment above
	memCheckAbove = 90 * time.Second      // default memory checking frequency when above low watermark (see lowwm, setTimer())
	freeIdleMin   = memCheckAbove         // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero  = freeIdleMin * 2       // ... to zero
)

// slab constants
const (
	countThreshold = 16 // exceeding this scatter-gather count warrants selecting a larger-size Slab
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
	Slab struct {
		m            *MMSA // parent
		bufSize      int64
		tag          string
		get, put     [][]byte
		muget, muput sync.Mutex
		pMinDepth    *atomic.Int64
		pos          int
		hits         atomic.Uint32
	}
	Stats struct {
		Idle      [NumStats]time.Time
		Hits      [NumStats]uint32
		HitsDelta [NumStats]uint32
	}
	ReqStats struct {
		Wg    *sync.WaitGroup
		Stats *Stats
	}
	MMSA struct {
		// public
		Name        string
		MinFree     uint64        // memory that must be available at all times
		TimeIval    time.Duration // interval of time to watch for low memory and make steps
		MinPctTotal int           // same, via percentage of total
		MinPctFree  int           // ditto, as % of free at init time
		// private
		duration    time.Duration
		lowWM       uint64
		rings       []*Slab
		ringsSorted []*Slab
		stats       Stats
		toGC        atomic.Int64  // accumulates over time and triggers GC upon reaching the spec-ed limit
		minDepth    atomic.Int64  // minimum ring depth aka length
		swap        atomic.Uint64 // actual swap size
		slabIncStep int64
		maxSlabSize int64
		numSlabs    int
		// public - aligned
		Swapping atomic.Int32 // max = SwappingMax; halves every r.duration unless swapping
		Small    bool         // defines the type of Slab rings (NumSmallSlabs x 128 | NumPageSlabs x 4K)
		Debug    bool         // debug mode
	}
	FreeSpec struct {
		IdleDuration time.Duration // reduce only the slabs that are idling for at least as much time
		MinSize      int64         // minimum freed size that'd warrant calling GC (default = sizetoGC)
		Totally      bool          // true: free all slabs regardless of their idle-ness and size
		ToOS         bool          // GC and then return the memory to the operating system
	}
)

var (
	// gmm is the global memory manager used in packages outside ais.
	gmm    = &MMSA{Name: defaultPageMM, MinPctFree: 50}
	smm    = &MMSA{Name: defaultSmallMM, MinPctFree: 50, Small: true}
	mmOnce sync.Once // ensures that there is only one initialization
)

// Global MMSA for usage by external clients and tools
func DefaultPageMM() *MMSA {
	mmOnce.Do(func() {
		_ = gmm.Init(true /*panicOnErr*/)
	})
	return gmm
}
func DefaultSmallMM() *MMSA { return smm } // TODO -- FIXME

//////////////
// MMSA API //
//////////////

// initialize new MMSA instance
func (r *MMSA) Init(panicOnErr bool) (err error) {
	cmn.Assert(r.Name != "")

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
	required, actual := cmn.B2S(int64(r.MinFree), 2), cmn.B2S(int64(mem.ActualFree), 2)
	if mem.ActualFree < r.MinFree {
		err = fmt.Errorf("insufficient free memory %s, minimum required %s (see %s for guidance)", actual, required, readme)
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
	r.duration = r.TimeIval

	// 5. final construction steps
	r.swap.Store(mem.SwapUsed)
	r.minDepth.Store(minDepth)
	r.toGC.Store(0)

	// init rings and slabs
	r.slabIncStep, r.maxSlabSize, r.numSlabs = PageSlabIncStep, MaxPageSlabSize, NumPageSlabs
	if r.Small {
		r.slabIncStep, r.maxSlabSize, r.numSlabs = SmallSlabIncStep, MaxSmallSlabSize, NumSmallSlabs
	}
	r.rings = make([]*Slab, r.numSlabs)
	r.ringsSorted = make([]*Slab, r.numSlabs)
	for i := 0; i < r.numSlabs; i++ {
		bufSize := r.slabIncStep * int64(i+1)
		slab := &Slab{
			m:       r,
			tag:     r.Name + "." + cmn.B2S(bufSize, 0),
			bufSize: bufSize,
			get:     make([][]byte, 0, minDepth),
			put:     make([][]byte, 0, minDepth),
		}
		slab.pMinDepth = &r.minDepth
		r.rings[i] = slab
		r.ringsSorted[i] = slab
	}

	// 6. NOTE: always GC at init time
	runtime.GC()
	hk.Housekeeper.Register(r.Name+".gc", r.garbageCollect, r.TimeIval)
	return
}

// terminate this MMSA
func (r *MMSA) Terminate() {
	hk.Housekeeper.Unregister(r.Name + ".gc")
	for _, s := range r.rings {
		_ = s.cleanup()
	}
}

// allocate SGL
func (r *MMSA) NewSGL(immediateSize int64 /* size to preallocate */, sbufSize ...int64 /* slab buffer size */) *SGL {
	var (
		slab *Slab
		n    int64
		sgl  [][]byte
	)
	cmn.Assert(!r.Small) // no need

	if len(sbufSize) > 0 {
		var err error
		slab, err = r.GetSlab(sbufSize[0])
		cmn.AssertNoErr(err)
	} else {
		slab = r.slabForSGL(immediateSize)
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

// returns an estimate for the current memory pressured expressed as one of the enumerated values
func (r *MMSA) MemPressure() int {
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

func (r *MMSA) GetSlab(bufSize int64) (s *Slab, err error) {
	a, b := bufSize/r.slabIncStep, bufSize%r.slabIncStep
	if b != 0 {
		err = fmt.Errorf("buf_size %d must be multiple of 4K", bufSize)
		return
	}
	if a < 1 || a > int64(r.numSlabs) {
		err = fmt.Errorf("buf_size %d outside valid range", bufSize)
		return
	}
	s = r.rings[a-1]
	return
}

func (r *MMSA) Alloc(sizes ...int64) (buf []byte, slab *Slab) {
	var size int64
	if len(sizes) == 0 {
		if !r.Small {
			size = DefaultBufSize
		} else {
			size = DefaultSmallBufSize
		}
	}
	if size >= r.maxSlabSize {
		slab = r.rings[len(r.rings)-1]
	} else if size <= r.slabIncStep {
		slab = r.rings[0]
	} else {
		a := (size + r.slabIncStep - 1) / r.slabIncStep
		slab = r.rings[a-1]
	}
	buf = slab.Alloc()
	return
}

func (r *MMSA) GetStats() *Stats {
	var resp = &Stats{}
	r.refreshStats()
	for i := 0; i < r.numSlabs; i++ {
		resp.Hits[i] = r.stats.Hits[i]
		resp.HitsDelta[i] = r.stats.HitsDelta[i]
		resp.Idle[i] = r.stats.Idle[i]
	}
	return resp
}

///////////////
// Slab API //
///////////////

func (s *Slab) Size() int64 { return s.bufSize }
func (s *Slab) Tag() string { return s.tag }

func (s *Slab) Alloc() (buf []byte) {
	s.muget.Lock()
	buf = s._alloc()
	s.muget.Unlock()
	return
}

func (s *Slab) Free(bufs ...[]byte) {
	// NOTE: races are expected between getting length of the `s.put` slice
	// and putting something in it. But since `maxdepth` is not hard limit,
	// we cannot ever exceed we are trading this check in favor of maybe bigger
	// slices. Also freeing buffers to the same slab at the same point in time
	// is rather unusual we don't expect this happen often.
	if len(s.put) < maxDepth {
		s.muput.Lock()
		for _, buf := range bufs {
			if s.m.Debug {
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

// select a slab for SGL given its immediate size to allocate
func (r *MMSA) slabForSGL(immediateSize int64) *Slab {
	if immediateSize == 0 {
		if !r.Small {
			immediateSize = DefaultBufSize
		} else {
			immediateSize = DefaultSmallBufSize
		}
		slab, _ := r.GetSlab(immediateSize)
		return slab
	}
	size := cmn.DivCeil(immediateSize, countThreshold)
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

// freeIdle traverses and deallocates idle slabs- those that were not used for at
// least the specified duration; returns freed size
func (r *MMSA) freeIdle(duration time.Duration) (freed int64) {
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

func (s *Slab) _alloc() (buf []byte) {
	if len(s.get) > s.pos { // fast path
		buf = s.get[s.pos]
		s.pos++
		s.hits.Inc()
		return
	}
	return s._allocSlow()
}

func (s *Slab) _allocSlow() (buf []byte) {
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
	s.hits.Inc()
	return
}

func (s *Slab) grow(cnt int) {
	if bool(glog.FastV(4, glog.SmoduleMemsys)) || s.m.Debug {
		lput := len(s.put)
		glog.Infof("%s: grow by %d => %d", s.tag, cnt, lput+cnt)
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
			cnt = cmn.Max(cnt, lput/2) // Heu #6
		} else {
			cnt = cmn.Min(cnt, lput/2) // Heu #7
		}
	}
	if cnt > 0 {
		if bool(glog.FastV(4, glog.SmoduleMemsys)) || s.m.Debug {
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
			cnt = cmn.Max(cnt, lget/2) // Heu #9
		} else {
			cnt = cmn.Min(cnt, lget/2) // Heu #10
		}
	}
	if cnt > 0 {
		if bool(glog.FastV(4, glog.SmoduleMemsys)) || s.m.Debug {
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

	cmn.Dassert(len(s.get) == 0 && len(s.put) == 0, pkgName)
	s.muput.Unlock()
	s.muget.Unlock()
	return
}
