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
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	sigar "github.com/cloudfoundry/gosigar"
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
// Mem2 is a "runner" that can be Run() to monitor system resources, automatically
// adjust Slab sizes based on their respective usages, and incrementally
// deallocate idle Slabs.
//
// There will be use cases, however, when actually running a Mem2 instance
// won't be necessary: e.g., when an app utilizes a single (or a few distinct)
// Slab size(s) for the duration of its relatively short lifecycle,
// while at the same time preferring minimal interference with other running apps.
//
// In that sense, a typical initialization sequence includes 2 or 3 steps, e.g.:
// 1) construct:
// 	mem2 := &memsys.Mem2{TimeIval: ..., MinPctFree: ..., Name: ..., Debug: ...}
// 2) initialize:
// 	err := mem2.Init()
// 	if err != nil {
//		...
// 	}
// 3) optionally, run:
// 	go mem2.Run()
//
// Cleanup:
//
// To clean up a Mem2 instance after it is used, call Stop(error) on the Mem2 instance.
// Note that a Mem2 instance can be Stopped as long as it had been initialized.
// This will free all the slabs that were allocated to the memory manager.
// Since the stop is being performed intentionally, nil should be passed as the error.
//
// In addition, there are several environment variables that can be used
// (to circumvent the need to change the code, for instance):
// 	"AIS_MINMEM_FREE"
// 	"AIS_MINMEM_PCT_TOTAL"
// 	"AIS_MINMEM_PCT_FREE"
// 	"AIS_MEM_DEBUG"
// These names must be self-explanatory.
//
// Once constructed and initialized, memory-manager-and-slab-allocator
// (Mem2, for shortness) can be exercised via its public API that includes
// GetSlab2(), SelectSlab2() and AllocFromSlab2(). Notice the difference between
// the first and the second: GetSlab2(128KB) will return the Slab that contains
// size=128KB reusable buffers, while SelectSlab2(128KB) - the Slab that is
// considered optimal for the (estimated) total size 128KB.
//
// Once selected, each Slab2 instance can be used via its own public API that
// includes Alloc() and Free() methods. In addition, each allocated SGL internally
// utilizes one of the existing enumerated slabs to "grow" (that is, allocate more
// buffers from the slab) on demand. For details, look for "grow" in the iosgl.go.
//
// When being run (as in: go mem2.Run()), the memory manager periodically evaluates
// the remaining free memory resource and adjusts its slabs accordingly.
// The entire logic is consolidated in one work() method that can, for instance,
// "cleanup" (see cleanup()) an existing "idle" slab,
// or forcefully "reduce" (see reduce()) one if and when the amount of free
// memory falls below watermark.
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

const Numslabs = 128 / 4 // [4K - 128K] at 4K increments
const DEADBEEF = "DEADBEEF"
const GlobalMem2Name = "GMem2"

// mem subsystem defaults (potentially, tunables)
const (
	mindepth      = 128             // ring cap min; default ring growth increment
	maxdepth      = 1024 * 24       // ring cap max
	sizetoGC      = cmn.GiB * 2     // see heuristics ("Heu")
	loadavg       = 10              // "idle" load average to deallocate Slabs when below
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

// mem2 usage levels; one-indexed to avoid zero value check issues
const (
	Mem2Initialized = iota + 1
	Mem2Running
	Mem2Stopped
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

		bufsize      int64
		tag          string
		get, put     [][]byte
		muget, muput sync.Mutex
		stats        struct {
			Hits int64
		}
		pmindepth *int64
		pos       int
		debug     bool
	}
	Stats2 struct {
		Hits    [Numslabs]int64
		Adeltas [Numslabs]int64
		Idle    [Numslabs]time.Time
	}
	ReqStats2 struct {
		Wg    *sync.WaitGroup
		Stats *Stats2
	}
	Mem2 struct {
		cmn.Named
		stopCh chan struct{}
		statCh chan ReqStats2
		time   struct {
			d time.Duration
			t *time.Timer
		}
		lowwm    uint64
		rings    [Numslabs]*Slab2
		stats    Stats2
		sorted   []sortpair
		toGC     int64 // accumulates over time and triggers GC upon reaching the spec-ed limit
		mindepth int64 // minimum ring depth aka length
		usageLvl int64 // integer values corresponding to Mem2Initialized, Mem2Running, or Mem2Stopped
		// for user to specify at construction time
		Name        string
		MinFree     uint64        // memory that must be available at all times
		TimeIval    time.Duration // interval of time to watch for low memory and make steps
		swap        uint64        // actual swap size
		Swapping    int32         // max = SwappingMax; halves every r.time.d unless swapping
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
	// gMem2 is the global memory manager used in various packages outside ais.
	// Its runtime params are set below and is intended to remain so.
	gMem2 = &Mem2{Name: GlobalMem2Name, TimeIval: time.Minute * 2, MinPctFree: 50}
	// Mapping of usageLvl values to corresponding strings for log messages
	usageLvls = map[int64]string{Mem2Initialized: "Initialized", Mem2Running: "Running", Mem2Stopped: "Stopped"}
)

// Global memory manager getter
func Init() *Mem2 {
	gMem2.Init(false /* don't ignore init-time errors */)
	go gMem2.Run()
	return gMem2
}

//
// API methods
//

func (r *Mem2) NewSGL(immediateSize int64 /* size to allocate at construction time */) *SGL {
	r.assertReadyForUse()
	slab := r.SelectSlab2(immediateSize)
	n := cmn.DivCeil(immediateSize, slab.Size())
	sgl := make([][]byte, n)
	slab.muget.Lock()
	for i := 0; i < int(n); i++ {
		sgl[i] = slab._alloc()
	}
	slab.muget.Unlock()
	return &SGL{sgl: sgl, slab: slab}
}

func (r *Mem2) NewSGLWithHash(immediateSize int64, hash hash.Hash64) *SGL {
	sgl := r.NewSGL(immediateSize)
	sgl.hash = hash
	return sgl
}

// returns an estimate for the current memory pressured expressed as one of the enumerated values
func (r *Mem2) MemPressure() int {
	mem := sigar.Mem{}
	mem.Get()
	swap := sigar.Swap{}
	swap.Get()
	if swap.Used > atomic.LoadUint64(&r.swap) {
		atomic.StoreInt32(&r.Swapping, SwappingMax)
	}
	if atomic.LoadInt32(&r.Swapping) > 0 {
		return OOM
	}
	if mem.ActualFree > r.lowwm {
		return MemPressureLow
	}
	if mem.ActualFree <= r.MinFree {
		return MemPressureExtreme
	}
	x := (mem.ActualFree - r.MinFree) * 100 / (r.lowwm - r.MinFree)
	if x <= 25 {
		return MemPressureHigh
	}
	return MemPressureModerate
}
func MemPressureText(v int) string { return memPressureText[v] }

//
// on error behavior is defined by the ignorerr argument
// true:  print error message and proceed regardless
// false: print error message and panic
//
func (r *Mem2) Init(ignorerr bool) (err error) {
	// CAS implemented to enforce only one invocation of gMem2.Init()
	// for possible concurrent calls to memsys.Init() in multi-threaded context
	if !atomic.CompareAndSwapInt64(&r.usageLvl, 0, Mem2Initialized) {
		if r.usageLvl != Mem2Running {
			logMsg(fmt.Sprintf("%s is already %s", r.Name, usageLvls[r.usageLvl]))
		}
		return
	}
	if r.Name != "" {
		r.Setname(r.Name)
	}
	// 1. environment overrides defaults and Mem2{...} hard-codings
	if err = r.env(); err != nil {
		if !ignorerr {
			panic(err)
		}
	}
	// 2. compute minfree - mem size that must remain free at all times
	mem := sigar.Mem{}
	mem.Get()
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
		if !ignorerr {
			panic(err)
		}
	}
	x := cmn.MaxU64(r.MinFree*2, (r.MinFree+mem.ActualFree)/2)
	r.lowwm = cmn.MinU64(x, r.MinFree*3) // Heu #1: hysteresis

	// 4. timer
	if r.TimeIval == 0 {
		r.TimeIval = memCheckAbove
	}
	r.time.d = r.TimeIval
	r.setTimer(mem.ActualFree, mem.Total, false /*swapping */, false /* reset */)

	// 5. final construction steps
	r.sorted = make([]sortpair, Numslabs)
	swap := sigar.Swap{}
	swap.Get()
	atomic.StoreUint64(&r.swap, swap.Used)
	atomic.StoreInt64(&r.mindepth, int64(mindepth))
	atomic.StoreInt64(&r.toGC, 0)

	r.stopCh = make(chan struct{}, 1)
	r.statCh = make(chan ReqStats2, 1)

	// init slabs
	for i := range r.rings {
		slab := &Slab2{
			m:       r,
			bufsize: int64(cmn.KiB * 4 * (i + 1)),
			get:     make([][]byte, 0, mindepth),
			put:     make([][]byte, 0, mindepth),
		}
		slab.tag = r.Getname() + "." + cmn.B2S(slab.bufsize, 0)
		slab.pmindepth = &r.mindepth
		slab.debug = r.Debug
		r.rings[i] = slab
	}

	// 6. always GC at init time
	runtime.GC()

	return
}

// on-demand memory freeing to the user-provided specification
func (r *Mem2) Free(spec FreeSpec) {
	r.assertReadyForUse()
	var freed int64
	if spec.Totally {
		for _, s := range r.rings {
			freed += s.cleanup()
		}
	} else {
		if spec.IdleDuration == 0 {
			spec.IdleDuration = freeIdleMin // using default
		}
		currStats := Stats2{}
		req := ReqStats2{Wg: &sync.WaitGroup{}, Stats: &currStats}
		req.Wg.Add(1)
		r.GetStats(req)
		req.Wg.Wait()

		for i, idle := range currStats.Idle {
			if idle.IsZero() {
				continue
			}
			elapsed := time.Since(idle)
			if elapsed > spec.IdleDuration {
				s := r.rings[i]
				x := s.cleanup()
				freed += x
				if x > 0 {
					glog.Infof("%s: idle for %v - cleanup", s.tag, elapsed)
				}
			}
		}
	}
	if freed > 0 {
		atomic.AddInt64(&r.toGC, freed)
		if spec.MinSize == 0 {
			spec.MinSize = sizetoGC // using default
		}
		mem := sigar.Mem{}
		mem.Get()
		r.doGC(mem.ActualFree, spec.MinSize, spec.ToOS /* force */, false)
	}
}

// as a cmn.Runner
func (r *Mem2) Run() error {
	// if the usageLvl of GMem2 is Mem2Initialized, swaps its usageLvl to Mem2Running atomically
	// CAS implemented to enforce only one invocation of GMem2.Run()
	if !atomic.CompareAndSwapInt64(&r.usageLvl, Mem2Initialized, Mem2Running) {
		return fmt.Errorf("%s needs to be at initialized level, is currently %s", r.Name, usageLvls[r.usageLvl])
	}
	r.time.t = time.NewTimer(r.time.d)
	mem := sigar.Mem{}
	mem.Get()
	m, l := cmn.B2S(int64(r.MinFree), 2), cmn.B2S(int64(r.lowwm), 2)
	logMsg(fmt.Sprintf("Starting %s, minfree %s, low %s, timer %v", r.Getname(), m, l, r.time.d))
	f := cmn.B2S(int64(mem.ActualFree), 2)
	if mem.ActualFree > mem.Total-mem.Total/5 { // more than 80%
		logMsg(fmt.Sprintf("%s: free memory %s > 80%% total", r.Getname(), f))
	} else if mem.ActualFree < r.lowwm {
		logMsg(fmt.Sprintf("Warning: free memory %s below low watermark %s at %s startup", f, l, r.Getname()))
	}

	for {
		select {
		case <-r.time.t.C:
			r.work()
		case req := <-r.statCh:
			r.doStats()
			for i := 0; i < Numslabs; i++ {
				req.Stats.Hits[i] = r.stats.Hits[i]
				req.Stats.Adeltas[i] = r.stats.Adeltas[i]
				req.Stats.Idle[i] = r.stats.Idle[i]
			}
			req.Wg.Done()
		case <-r.stopCh:
			r.time.t.Stop()
			r.stop()
			return nil
		}
	}
}

func (r *Mem2) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	if atomic.CompareAndSwapInt64(&r.usageLvl, Mem2Initialized, Mem2Stopped) {
		r.stop()
	} else if atomic.CompareAndSwapInt64(&r.usageLvl, Mem2Running, Mem2Stopped) {
		r.stopCh <- struct{}{}
		close(r.stopCh)
	} else {
		glog.Warningf("%s already stopped", r.Name)
	}
}

func (r *Mem2) stop() {
	for _, s := range r.rings {
		_ = s.cleanup()
	}
}

func (r *Mem2) GetSlab2(bufsize int64) (s *Slab2, err error) {
	a, b := bufsize/(cmn.KiB*4), bufsize%(cmn.KiB*4)
	if b != 0 {
		err = fmt.Errorf("bufsize %d must be multiple of 4K", bufsize)
		return
	}
	if a < 1 || a > Numslabs {
		err = fmt.Errorf("bufsize %d outside valid range", bufsize)
		return
	}
	s = r.rings[a-1]
	return
}

func (r *Mem2) SelectSlab2(estimatedSize int64) *Slab2 {
	r.assertReadyForUse()
	if estimatedSize == 0 {
		estimatedSize = minSizeUnknown
	}
	size := cmn.DivCeil(estimatedSize, countThreshold)
	for _, slab := range r.rings {
		if slab.Size() >= size {
			return slab
		}
	}
	return r.rings[len(r.rings)-1]
}

func (r *Mem2) AllocFromSlab2(estimSize int64) ([]byte, *Slab2) {
	slab := r.SelectSlab2(estimSize)
	return slab.Alloc(), slab
}

func (r *Mem2) GetStats(req ReqStats2) {
	r.statCh <- req
}

//
// Slab2 API
//

func (s *Slab2) Size() int64 { return s.bufsize }
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
	if len(s.put) < maxdepth {
		s.muput.Lock()
		for _, buf := range bufs {
			if s.debug {
				cmn.Assert(len(buf) == int(s.bufsize))
				for i := 0; i < len(buf); i += len(DEADBEEF) {
					copy(buf[i:], []byte(DEADBEEF))
				}
			}
			s.put = append(s.put, buf)
		}
		s.muput.Unlock()
	} else {
		// When we just discard buffer, since the `s.put` cache is full, then
		// we need to remember how much memory we discarded and take it into
		// account when determining if we should return memory to the system.
		atomic.AddInt64(&s.m.toGC, s.bufsize*int64(len(bufs)))
	}
}

//============================================================================
//
// private methods
//
//============================================================================

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
	if a := os.Getenv("AIS_MEM_DEBUG"); a != "" {
		r.Debug = true
	}
	return
}

// NOTE: notice enumerated heuristics below denoted as "Heu"
func (r *Mem2) work() {
	var (
		depth int               // => current ring depth tbd
		limit = int64(sizetoGC) // minimum accumulated size that triggers GC
	)
	mem := sigar.Mem{}
	mem.Get()
	swap := sigar.Swap{}
	swap.Get()
	swapping := swap.Used > r.swap
	if swapping {
		atomic.StoreInt32(&r.Swapping, SwappingMax)
	} else {
		atomic.StoreInt32(&r.Swapping, atomic.LoadInt32(&r.Swapping)/2)
	}
	r.swap = swap.Used

	r.doStats()

	// 1. enough => free idle
	if mem.ActualFree > r.lowwm && !swapping {
		atomic.StoreInt64(&r.mindepth, int64(mindepth))
		if delta := r.freeIdle(freeIdleMin); delta > 0 {
			atomic.AddInt64(&r.toGC, delta)
			r.doGC(mem.ActualFree, sizetoGC, false, false)
		}
		goto timex
	}
	if mem.ActualFree <= r.MinFree || swapping { // 2. mem too low indicates "high watermark"
		depth = mindepth / 4
		if mem.ActualFree < r.MinFree {
			depth = mindepth / 8
		}
		if swapping {
			depth = 1
		}
		atomic.StoreInt64(&r.mindepth, int64(depth))
		limit = sizetoGC / 2
	} else { // 3. in-between hysteresis
		x := uint64(maxdepth-mindepth) * (mem.ActualFree - r.MinFree)
		depth = mindepth + int(x/(r.lowwm-r.MinFree)) // Heu #2
		if r.Debug {
			cmn.Assert(depth >= mindepth && depth <= maxdepth)
		}
		atomic.StoreInt64(&r.mindepth, int64(mindepth/4))
	}
	// idle first
	for i, s := range r.rings {
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
	r.setTimer(mem.ActualFree, mem.Total, swapping, true /* reset */)
}

func (r *Mem2) setTimer(free, total uint64, swapping, reset bool) {
	var changed bool
	switch {
	case free > r.lowwm && free > total-total/5:
		if r.time.d != r.TimeIval*2 {
			r.time.d = r.TimeIval * 2
			changed = true
		}
	case free <= r.MinFree || swapping:
		if r.time.d != r.TimeIval/4 {
			r.time.d = r.TimeIval / 4
			changed = true
		}
	case free <= r.lowwm:
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
	if reset {
		if changed {
			glog.Infof("timer %v, free %s", r.time.d, cmn.B2S(int64(free), 1))
		}
		r.time.t.Reset(r.time.d)
	}
}

// freeIdle traverses and deallocates idle slabs- those that were not used for at
// least the specified duration; returns freed size
func (r *Mem2) freeIdle(duration time.Duration) (freed int64) {
	for i, idle := range r.stats.Idle {
		if idle.IsZero() {
			continue
		}
		elapsed := time.Since(idle)
		if elapsed > duration {
			s := r.rings[i]
			if elapsed > freeIdleZero {
				x := s.cleanup()
				freed += x
				if x > 0 {
					glog.Infof("%s: idle for %v - cleanup", s.tag, elapsed)
				}
			} else {
				x := s.reduce(mindepth, true /* idle */, false /* force */)
				freed += x
				if x > 0 && (bool(glog.V(4)) || r.Debug) {
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
	c := sigar.ConcreteSigar{}
	avg, err := c.GetLoadAverage()
	if err != nil {
		glog.Errorf("Failed to load averages, err: %v", err)
		avg.One = 999 // fall thru on purpose
	}
	if avg.One > loadavg && !force && !swapping { // Heu #3
		return
	}
	toGC := atomic.LoadInt64(&r.toGC)
	if toGC > minsize {
		str := fmt.Sprintf(
			"GC(force: %t, swapping: %t); load: %.2f; free: %s; toGC_size: %s",
			force, swapping, avg.One, cmn.B2S(int64(free), 1), cmn.B2S(toGC, 2),
		)
		if force || swapping { // Heu #4
			glog.Warningf("%s - freeing memory to the OS...", str)
			runtime.GC()
			debug.FreeOSMemory() // forces GC followed by an attempt to return memory to the OS
		} else { // Heu #5
			glog.Infof(str)
			runtime.GC()
		}
		gced = true
		atomic.StoreInt64(&r.toGC, 0)
	}
	return
}

func (r *Mem2) fsort(i, j int) bool {
	return r.sorted[i].v < r.sorted[j].v
}

func (r *Mem2) doStats() {
	now := time.Now()
	for i, s := range r.rings {
		prev := r.stats.Hits[i]
		r.stats.Hits[i] = atomic.LoadInt64(&s.stats.Hits)
		r.stats.Adeltas[i] = r.stats.Hits[i] - prev
		isZero := r.stats.Idle[i].IsZero()
		if r.stats.Adeltas[i] == 0 && isZero {
			r.stats.Idle[i] = now
		} else if r.stats.Adeltas[i] > 0 && !isZero {
			r.stats.Idle[i] = time.Time{}
		}
	}
}

func (r *Mem2) assertReadyForUse() {
	if r.Debug {
		errstr := fmt.Sprintf("%s is not initialized nor running", r.Name)
		cmn.AssertMsg(r.usageLvl == Mem2Initialized || r.usageLvl == Mem2Running, errstr)
	}
}

func (s *Slab2) _alloc() (buf []byte) {
	if len(s.get) > s.pos { // fast path
		buf = s.get[s.pos]
		s.pos++
		atomic.AddInt64(&s.stats.Hits, 1)
		return
	}
	return s._allocSlow()
}

func (s *Slab2) _allocSlow() (buf []byte) {
	curmindepth := atomic.LoadInt64(s.pmindepth)
	if curmindepth == 0 {
		curmindepth = 1
	}
	if s.debug {
		cmn.Assert(len(s.get) == s.pos)
	}
	s.muput.Lock()
	lput := len(s.put)
	if cnt := int(curmindepth) - lput; cnt > 0 {
		s.grow(cnt)
	}
	s.get, s.put = s.put, s.get
	if s.debug {
		cmn.Assert(len(s.put) == s.pos)
		cmn.Assert(len(s.get) >= int(curmindepth))
	}
	s.put = s.put[:0]
	s.muput.Unlock()

	s.pos = 0
	buf = s.get[s.pos]
	s.pos++
	atomic.AddInt64(&s.stats.Hits, 1)
	return
}

func (s *Slab2) grow(cnt int) {
	if bool(glog.V(4)) || s.debug {
		lput := len(s.put)
		glog.Infof("%s: grow by %d => %d", s.tag, cnt, lput+cnt)
	}
	for ; cnt > 0; cnt-- {
		buf := make([]byte, s.bufsize)
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
		if bool(glog.V(4)) || s.debug {
			glog.Infof("%s(f=%t, i=%t): reduce lput %d => %d", s.tag, force, isidle, lput, lput-cnt)
		}
		for ; cnt > 0; cnt-- {
			lput--
			s.put[lput] = nil
			freed += s.bufsize
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
		if bool(glog.V(4)) || s.debug {
			glog.Infof("%s(f=%t, i=%t): reduce lget %d => %d", s.tag, force, isidle, lget, lget-cnt)
		}
		for ; cnt > 0; cnt-- {
			s.get[s.pos] = nil
			s.pos++
			freed += s.bufsize
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
		freed += s.bufsize
	}
	for i := range s.put {
		s.put[i] = nil
		freed += s.bufsize
	}
	s.get = s.get[:0]
	s.put = s.put[:0]
	s.pos = 0
	if s.debug {
		cmn.Assert(len(s.get) == 0 && len(s.put) == 0)
	}
	s.muput.Unlock()
	s.muget.Unlock()
	return
}

func logMsg(arg interface{}) {
	if flag.Parsed() {
		glog.Infoln(arg)
	}
}
