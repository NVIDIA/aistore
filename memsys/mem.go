/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
package memsys

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/cloudfoundry/gosigar"
)

// ===================== Theory Of Operations (TOO) =============================
//
// Mem2 is, simultaneously, a 1) Slab and SGL allocator and 2) memory manager
// responsible to optimize memory usage between different (more and less) used
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
// Slab size(s) for the duration of its relatively short lifecycle
// while at the same time preferring minimal interference with other running apps.
//
// ========================== end of TOO ========================================

const Numslabs = 128 / 4 // [4K - 128K] at 4K increments

// mem subsystem defaults (potentially, tunables)
const (
	mindepth      = 128             // ring cap min; default ring growth increment
	maxdepth      = 1024 * 24       // ring cap max
	sizetoGC      = common.GiB * 2  // see heuristics ("Heu")
	loadavg1      = 1               // load average type 1
	loadavg2      = 3               // load average type 2
	maxMemPct     = 50              // % of total RAM that can be used by the subsystem unless specified via DFC_MINMEM_*
	minMemFree    = common.GiB      // default minimum memory size that must remain available - see DFC_MINMEM_*
	memCheckAbove = time.Minute     // default memory checking frequency when above low watermark (see lowwm, setTimer())
	freeIdleMin   = memCheckAbove   // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero  = freeIdleMin * 2 // ... to zero
)

//
// API types
//
type (
	Slab2 struct {
		bufsize      int64
		tag          string
		get, put     [][]byte
		muget, muput sync.Mutex
		l2cache      sync.Pool
		stats        struct {
			Hits, Miss int64
		}
		pmindepth       *int64
		pos             int
		usespool, debug bool
	}
	Stats2 struct {
		Hits, Miss [Numslabs]int64
		Adeltas    [Numslabs]int64
		Idle       [Numslabs]time.Time
	}
	ReqStats2 struct {
		Wg    *sync.WaitGroup
		Stats *Stats2
	}
	Mem2 struct {
		common.Named
		swap   uint64
		stopCh chan struct{}
		statCh chan ReqStats2
		time   struct {
			d time.Duration
			t *time.Timer
		}
		lowwm          uint64
		rings          [Numslabs]*Slab2
		stats          Stats2
		sorted         []sortpair
		toGC, mindepth int64
		// for user to specify at construction time
		Name        string
		MinFree     uint64        // memory that must be available at all times
		Period      time.Duration // interval of time to watch for low memory and make steps
		MinPctTotal int           // same, via percentage of total
		MinPctFree  int           // ditto, as % of free at init time
		Debug       bool
	}
)

// private types
type sortpair struct {
	s *Slab2
	v int64
}

//
// API methods
//

func (r *Mem2) NewSGL(immediateSize int64 /* size to allocate at construction time */) *SGL {
	slab := r.SelectSlab2(immediateSize)
	n := common.DivCeil(immediateSize, slab.Size())
	sgl := make([][]byte, n)
	slab.muget.Lock()
	for i := 0; i < int(n); i++ {
		sgl[i] = slab._alloc()
	}
	slab.muget.Unlock()
	return &SGL{sgl: sgl, slab: slab}
}

func (r *Mem2) Init() (err error) {
	// 1. environment overrides defaults and Mem2{...} hard-codings
	if err = r.env(); err != nil {
		return
	}
	// 2. compute minfree - mem size that must remain free at all times
	mem := sigar.Mem{}
	mem.Get()
	if r.MinPctTotal > 0 {
		x := mem.Total * uint64(r.MinPctTotal) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = common.MinU64(r.MinFree, x)
		}
	}
	if r.MinPctFree > 0 {
		x := mem.Free * uint64(r.MinPctFree) / 100
		if r.MinFree == 0 {
			r.MinFree = x
		} else {
			r.MinFree = common.MinU64(r.MinFree, x)
		}
	}
	if r.MinFree == 0 {
		r.MinFree = minMemFree
	}
	if r.MinFree < common.GiB {
		fmt.Printf("Warning: minimum free memory %s cannot be lower than 1GB\n", common.B2S(int64(r.MinFree), 2))
		r.MinFree = common.GiB // at least
	}
	if r.Name != "" {
		r.Setname(r.Name)
	}
	// 3. validate and compute free memory "low watermark"
	m, f := common.B2S(int64(r.MinFree), 2), common.B2S(int64(mem.Free), 2)
	if mem.Free < r.MinFree {
		return fmt.Errorf("Insufficient free memory %s, minimum required %s", f, m)
	}
	r.lowwm = common.MinU64(r.MinFree*3, (r.MinFree+mem.Total)/2) // Heu #1: hysteresis

	// 4. timer
	if r.Period == 0 {
		r.Period = memCheckAbove
	}
	_ = r.setTimer(mem.Free, mem.Total, false /*swapping */, false /* reset */)

	// 5. final construction steps
	r.sorted = make([]sortpair, Numslabs)
	swap := sigar.Swap{}
	swap.Get()
	r.swap = swap.Used
	atomic.StoreInt64(&r.mindepth, int64(mindepth))

	r.stopCh = make(chan struct{}, 1)
	r.statCh = make(chan ReqStats2, 1)

	// init slabs
	for i := range r.rings {
		slab := &Slab2{bufsize: int64(common.KiB * 4 * (i + 1)),
			get: make([][]byte, 0, mindepth),
			put: make([][]byte, 0, mindepth),
		}
		slab.l2cache = sync.Pool{New: nil}
		slab.tag = r.Getname() + "." + common.B2S(slab.bufsize, 0)
		slab.pmindepth = &r.mindepth
		slab.debug = r.Debug
		slab.usespool = false // NOTE: not using sync.Pool as l2
		r.rings[i] = slab
	}

	// 6. always GC at init time
	runtime.GC()
	return
}

// as a common.Runner
func (r *Mem2) Run() error {
	if !flag.Parsed() {
		flag.Parse()
	}
	r.time.t = time.NewTimer(r.time.d)
	m, l := common.B2S(int64(r.MinFree), 2), common.B2S(int64(r.lowwm), 2)
	glog.Infof("Starting %s, minfree %s, low %s", r.Getname(), m, l)
	mem := sigar.Mem{}
	mem.Get()
	f := common.B2S(int64(mem.Free), 2)
	if mem.Free > mem.Total-mem.Total/5 { // more than 80%
		glog.Infof("%s: free memory %s > 80%% total", r.Getname(), f)
	} else if mem.Free < r.lowwm {
		glog.Warningf("Warning: free memory %s below low watermark %s at %s startup", f, l, r.Getname())
	}
	for {
		select {
		case <-r.time.t.C:
			r.work()
		case req := <-r.statCh:
			for i := 0; i < Numslabs; i++ {
				req.Stats.Hits[i] = r.stats.Hits[i]
				req.Stats.Miss[i] = r.stats.Miss[i]
				req.Stats.Adeltas[i] = r.stats.Adeltas[i]
				req.Stats.Idle[i] = r.stats.Idle[i]
			}
			req.Wg.Done()
		case <-r.stopCh:
			r.time.t.Stop()
			for _, s := range r.rings {
				_ = s.cleanup()
			}
			return nil
		}
	}
}

func (r *Mem2) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

func (r *Mem2) GetSlab2(bufsize int64) (s *Slab2, err error) {
	a, b := bufsize/(common.KiB*4), bufsize%(common.KiB*4)
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
	if estimatedSize == 0 {
		estimatedSize = minSizeUnknown
	}
	size := common.DivCeil(estimatedSize, countThreshold)
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

func (s *Slab2) Free(buf []byte) {
	s.muput.Lock()
	s._free(buf)
	s.muput.Unlock()
}

//============================================================================
//
// private methods
//
//============================================================================

func (r *Mem2) env() (err error) {
	var minfree int64
	if a := os.Getenv("DFC_MINMEM_FREE"); a != "" {
		if minfree, err = common.S2B(a); err != nil {
			return fmt.Errorf("Cannot parse DFC_MINMEM_FREE '%s'", a)
		}
		r.MinFree = uint64(minfree)
	}
	if a := os.Getenv("DFC_MINMEM_PCT_TOTAL"); a != "" {
		if r.MinPctTotal, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("Cannot parse DFC_MINMEM_PCT_TOTAL '%s'", a)
		}
		if r.MinPctTotal < 0 || r.MinPctTotal > 100 {
			return fmt.Errorf("Invalid DFC_MINMEM_PCT_TOTAL '%s'", a)
		}
	}
	if a := os.Getenv("DFC_MINMEM_PCT_FREE"); a != "" {
		if r.MinPctFree, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("Cannot parse DFC_MINMEM_PCT_FREE '%s'", a)
		}
		if r.MinPctFree < 0 || r.MinPctFree > 100 {
			return fmt.Errorf("Invalid DFC_MINMEM_PCT_FREE '%s'", a)
		}
	}
	if a := os.Getenv("DFC_MEM_DEBUG"); a != "" {
		r.Debug = true
	}
	return
}

// NOTE: notice enumerated heuristics below denoted as "Heu"
func (r *Mem2) work() {
	var depth int

	mem := sigar.Mem{}
	mem.Get()
	swap := sigar.Swap{}
	swap.Get()
	swapping := swap.Used > r.swap
	r.swap = swap.Used

	r.doStats() // TODO more frequently(?)

	// 1. enough => free idle
	if mem.Free > r.lowwm && !swapping {
		atomic.StoreInt64(&r.mindepth, int64(mindepth))
		r.freeIdleGC(mem.Free, false, false)
		goto timex
	}
	if mem.Free <= r.MinFree { // 2. mem too low indicates "high watermark"
		depth = mindepth / 4
		if mem.ActualFree < r.MinFree {
			depth = mindepth / 8
		}
		if swapping {
			depth = 1
		}
		atomic.StoreInt64(&r.mindepth, int64(depth))
	} else { // 3. in-between hysteresis
		x := uint64(maxdepth-mindepth) * (mem.Free - r.MinFree)
		depth = mindepth + int(x/(r.lowwm-r.MinFree)) // Heu #2
		if r.Debug {
			common.Assert(depth >= mindepth && depth <= maxdepth)
		}
		atomic.StoreInt64(&r.mindepth, int64(mindepth/4))
	}
	// idle - first
	for i, s := range r.rings {
		r.sorted[i] = sortpair{s, r.stats.Adeltas[i]}
	}
	sort.Slice(r.sorted, r.fsort)
	for _, pair := range r.sorted {
		s := pair.s
		r.toGC += s.reduce(depth, pair.v == 0 /* idle */, true /* force */)
		if r.toGC > sizetoGC {
			r.freeIdleGC(mem.Free, true, swapping)
			goto timex
		}
	}
	// 4. when too low call it anyway if weren't called
	if mem.Free <= r.MinFree {
		r.freeIdleGC(mem.Free, true, swapping)
	}
timex:
	if r.setTimer(mem.Free, mem.Total, swapping, true /* reset */) {
		glog.Infof("timer %v", r.time.d)
	}
}

func (r *Mem2) setTimer(free, total uint64, swapping, reset bool) (flipped bool) {
	if r.time.d != r.Period/4 && (free <= r.lowwm || swapping) {
		r.time.d = r.Period / 4
		flipped = true
	} else if r.time.d != r.Period*2 && free > r.lowwm && free > total-total/5 {
		r.time.d = r.Period * 2
		flipped = true
	} else if r.time.d != r.Period && free > r.lowwm {
		r.time.d = r.Period
		flipped = true
	}
	if reset {
		r.time.t.Reset(r.time.d)
	}
	return
}

func (r *Mem2) freeIdleGC(free uint64, force, swapping bool) {
	if !force {
		for i, idle := range r.stats.Idle {
			if idle.IsZero() {
				continue
			}
			elapsed := time.Since(idle)
			if elapsed > freeIdleMin {
				s := r.rings[i]
				if elapsed > freeIdleZero {
					glog.Infof("%s: idle for %v - cleaning up", s.tag, elapsed)
					r.toGC += s.cleanup()
				} else {
					glog.Infof("%s: idle for %v - minimizing", s.tag, elapsed)
					r.toGC += s.reduce(mindepth, true /* idle */, false /* force */)
				}
			}
		}
	}
	if r.toGC < sizetoGC { // Heu #3
		return
	}
	c := sigar.ConcreteSigar{}
	avg, err := c.GetLoadAverage()
	if err != nil {
		glog.Errorf("Failed to load averages, err: %v", err)
		if force {
			runtime.GC()
			r.toGC = 0
		}
		return
	}
	if avg.One < loadavg1 || (avg.One < loadavg2 && force) || swapping { // Heu #4
		glog.Infof("GC(f=%t, swap=%t) load %.2f free %s GC %s", force, swapping, avg.One,
			common.B2S(int64(free), 1), common.B2S(int64(r.toGC), 2))
		runtime.GC()
		r.toGC = 0
	}
}

func (r *Mem2) fsort(i, j int) bool {
	return r.sorted[i].v < r.sorted[j].v
}

func (r *Mem2) doStats() {
	now := time.Now()
	for i, s := range r.rings {
		prev := r.stats.Hits[i] + r.stats.Miss[i]
		r.stats.Hits[i] = atomic.LoadInt64(&s.stats.Hits)
		r.stats.Miss[i] = atomic.LoadInt64(&s.stats.Miss)
		curr := r.stats.Hits[i] + r.stats.Miss[i]
		r.stats.Adeltas[i] = curr - prev
		isZero := r.stats.Idle[i].IsZero()
		if r.stats.Adeltas[i] == 0 && isZero {
			r.stats.Idle[i] = now
		} else if r.stats.Adeltas[i] > 0 && !isZero {
			r.stats.Idle[i] = time.Time{}
		}
	}
}

func (s *Slab2) _alloc() (buf []byte) {
	if len(s.get) > s.pos { // fast path
		buf = s.get[s.pos]
		s.pos++
		atomic.AddInt64(&s.stats.Hits, 1)
		return
	}
	// try l2
	if s.usespool {
		x := s.l2cache.Get()
		if x != nil {
			buf = x.([]byte)
			atomic.AddInt64(&s.stats.Hits, 1)
			return
		}
	}
	return s._allocSlow()
}

func (s *Slab2) _allocSlow() (buf []byte) {
	var (
		lput   int
		missed bool
	)
	curmindepth := atomic.LoadInt64(s.pmindepth)
	if curmindepth == 0 {
		curmindepth = 1
	}
	if s.debug {
		common.Assert(len(s.get) == s.pos)
	}
	s.muput.Lock()
	lput = len(s.put)
	if cnt := int(curmindepth) - lput; cnt > 0 {
		missed = s.grow(cnt)
	}
	s.get, s.put = s.put, s.get
	s.muput.Unlock()

	s.pos = 0
	buf = s.get[s.pos]
	s.pos++
	if missed {
		atomic.AddInt64(&s.stats.Miss, 1)
	} else {
		atomic.AddInt64(&s.stats.Hits, 1)
	}
	return
}

func (s *Slab2) grow(cnt int) bool {
	if bool(glog.V(4)) || s.debug {
		lput := len(s.put)
		glog.Infof("%s: grow by %d => %d", s.tag, cnt, lput+cnt)
	}
	if s.usespool {
		for ; cnt > 0; cnt-- {
			x := s.l2cache.Get()
			if x == nil {
				break
			}
			s.put = append(s.put, x.([]byte))
		}
		if cnt == 0 {
			return false
		}
	}
	for ; cnt > 0; cnt-- {
		buf := make([]byte, s.bufsize)
		s.put = append(s.put, buf)
	}
	return true
}

func (s *Slab2) _free(buf []byte) {
	if len(s.put) < maxdepth {
		s.put = append(s.put, buf)
	} else if s.usespool {
		s.l2cache.Put(buf)
	}
}

func (s *Slab2) reduce(todepth int, isidle, force bool) (freed int64) {
	s.muput.Lock()
	lput := len(s.put)
	cnt := lput - todepth
	if isidle {
		if force {
			cnt = common.Max(cnt, lput/2) // Heu #5
		} else {
			cnt = common.Min(cnt, lput/2) // Heu #6
		}
	}
	if cnt > 0 {
		if bool(glog.V(4)) || s.debug {
			glog.Infof("%s(f=%t, i=%t): reduce lput %d => %d", s.tag, force, isidle, lput, lput-cnt)
		}
		for ; cnt > 0; cnt-- {
			lput--
			if s.usespool && todepth > 0 && freed < sizetoGC/4 { // Heu #7
				buf := s.put[lput]
				s.l2cache.Put(buf)
			}
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
			cnt = common.Max(cnt, lget/2) // Heu #5
		} else {
			cnt = common.Min(cnt, lget/2) // Heu #6
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
	lget := len(s.get)
	for lget > s.pos {
		s.get[s.pos] = nil
		s.pos++
		freed += s.bufsize
	}
	for i := range s.put {
		freed += s.bufsize
		s.put[i] = nil
	}
	s.get = s.get[:0]
	s.put = s.put[:0]
	s.pos = 0
	if s.debug {
		common.Assert(len(s.get) == 0 && len(s.put) == 0)
	}
	s.muput.Unlock()
	s.muget.Unlock()
	return
}
