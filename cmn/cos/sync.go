// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	onexxh "github.com/OneOfOne/xxhash"
)

type (
	// TimeoutGroup is similar to sync.WaitGroup with the difference on Wait
	// where we only allow timing out.
	//
	// WARNING: It should not be used in critical code as it may have worse
	// performance than sync.WaitGroup - use only if its needed.
	//
	// WARNING: It is not safe to wait on completion in multiple threads!
	//
	// WARNING: It is not recommended to reuse the TimeoutGroup - it was not
	// designed for that and bugs can be expected, especially when previous
	// group was not called with successful (without timeout) WaitTimeout.
	TimeoutGroup struct {
		fin       chan struct{}
		pending   atomic.Int32
		postedFin atomic.Int32
	}

	// StopCh is a channel for stopping running things.
	StopCh struct {
		ch      chan struct{}
		stopped atomic.Bool
	}

	// Semaphore is a textbook _sempahore_ implemented as a wrapper on `chan struct{}`.
	Semaphore struct {
		s chan struct{}
	}

	// DynSemaphore implements semaphore which can change its size.
	DynSemaphore struct {
		c    *sync.Cond
		size int
		cur  int
		mu   sync.Mutex
	}

	// WG is an interface for wait group
	WG interface {
		Add(int)
		Done()
		Wait()
	}

	// LimitedWaitGroup is helper struct which combines standard wait group and
	// semaphore to limit the number of goroutines created.
	LimitedWaitGroup struct {
		wg   *sync.WaitGroup
		sema *DynSemaphore
	}

	NopLocker struct{}
)

type (
	// Commonly known as "sharded mutex"
	SharMutex16 struct {
		m [16]sync.Mutex
	}
)

// interface guard
var (
	_ WG = (*LimitedWaitGroup)(nil)
	_ WG = (*TimeoutGroup)(nil)
)

///////////////
// NopLocker //
///////////////

func (NopLocker) Lock()   {}
func (NopLocker) Unlock() {}

//////////////////
// TimeoutGroup //
//////////////////

func NewTimeoutGroup() *TimeoutGroup {
	return &TimeoutGroup{
		fin: make(chan struct{}, 1),
	}
}

func (twg *TimeoutGroup) Add(n int) {
	twg.pending.Add(int32(n))
}

// Wait waits until the Added pending count goes to zero.
// NOTE: must be invoked after _all_ Adds.
func (twg *TimeoutGroup) Wait() {
	twg.WaitTimeoutWithStop(24*time.Hour, nil)
}

// Wait waits until the Added pending count goes to zero _or_ timeout.
// NOTE: must be invoked after _all_ Adds.
func (twg *TimeoutGroup) WaitTimeout(timeout time.Duration) bool {
	timed, _ := twg.WaitTimeoutWithStop(timeout, nil)
	return timed
}

// Wait waits until the Added pending count goes to zero _or_ timeout _or_ stop.
// NOTE: must be invoked after _all_ Adds.
func (twg *TimeoutGroup) WaitTimeoutWithStop(timeout time.Duration, stop <-chan struct{}) (timed, stopped bool) {
	t := time.NewTimer(timeout)
	select {
	case <-twg.fin:
		twg.postedFin.Store(0)
	case <-t.C:
		timed, stopped = true, false
	case <-stop:
		timed, stopped = false, true
	}
	t.Stop()
	return
}

// Done decrements number of jobs left to do. Panics if the number jobs left is
// less than 0.
func (twg *TimeoutGroup) Done() {
	if n := twg.pending.Dec(); n == 0 {
		if posted := twg.postedFin.Swap(1); posted == 0 {
			twg.fin <- struct{}{}
		}
	} else if n < 0 {
		debug.Assertf(false, "invalid num pending %d", n)
	}
}

////////////
// StopCh //
////////////

func NewStopCh() *StopCh {
	return &StopCh{ch: make(chan struct{}, 1)}
}

func (sch *StopCh) Init() {
	debug.Assert(sch.ch == nil && !sch.stopped.Load())
	sch.ch = make(chan struct{}, 1)
}

func (sch *StopCh) Listen() <-chan struct{} {
	return sch.ch
}

func (sch *StopCh) Close() {
	if sch.stopped.CAS(false, true) {
		close(sch.ch)
	}
}

///////////////
// Semaphore //
///////////////

func NewSemaphore(n int) *Semaphore {
	s := &Semaphore{s: make(chan struct{}, n)}
	for range n {
		s.s <- struct{}{}
	}
	return s
}
func (s *Semaphore) TryAcquire() <-chan struct{} { return s.s }
func (s *Semaphore) Acquire()                    { <-s.TryAcquire() }
func (s *Semaphore) Release()                    { s.s <- struct{}{} }

func NewDynSemaphore(n int) *DynSemaphore {
	sema := &DynSemaphore{size: n}
	sema.c = sync.NewCond(&sema.mu)
	return sema
}

//////////////////
// DynSemaphore //
//////////////////

func (s *DynSemaphore) Size() int {
	s.mu.Lock()
	size := s.size
	s.mu.Unlock()
	return size
}

func (s *DynSemaphore) SetSize(n int) {
	debug.Assert(n >= 1, n)
	s.mu.Lock()
	s.size = n
	s.mu.Unlock()
}

func (s *DynSemaphore) Acquire(cnts ...int) {
	cnt := 1
	if len(cnts) > 0 {
		cnt = cnts[0]
	}
	s.mu.Lock()
check:
	if s.cur+cnt <= s.size {
		s.cur += cnt
		s.mu.Unlock()
		return
	}

	// Wait for vacant place(s)
	s.c.Wait()
	goto check
}

func (s *DynSemaphore) Release(cnts ...int) {
	cnt := 1
	if len(cnts) > 0 {
		cnt = cnts[0]
	}

	s.mu.Lock()

	debug.Assert(s.cur >= cnt, s.cur, " vs ", cnt)

	s.cur -= cnt
	s.c.Broadcast()
	s.mu.Unlock()
}

//////////////////////
// LimitedWaitGroup //
//////////////////////

// usage: no more than `limit` (e.g., sys.NumCPU()) goroutines in parallel
func NewLimitedWaitGroup(limit, wanted int) WG {
	debug.Assert(limit > 0 || wanted > 0, limit, " ", wanted)
	if wanted == 0 || wanted > limit {
		return &LimitedWaitGroup{wg: &sync.WaitGroup{}, sema: NewDynSemaphore(limit)}
	}
	return &sync.WaitGroup{}
}

func (lwg *LimitedWaitGroup) Add(n int) {
	lwg.sema.Acquire(n)
	lwg.wg.Add(n)
}

func (lwg *LimitedWaitGroup) Done() {
	lwg.sema.Release()
	lwg.wg.Done()
}

func (lwg *LimitedWaitGroup) Wait() {
	lwg.wg.Wait()
}

//
// common channel-full helper
//

const (
	chanFullSleep = 100 * time.Millisecond
)

type (
	ChanFull struct {
		atomic.Int64
	}
)

var ErrWorkChanFull = errors.New("work channel full")

func _threshold(c int) int { return c - c>>3 }

// where l = len(workCh), c = cap(workCh)
// - returns true on error and warning, both
// - may resched and sleep
func (u *ChanFull) Check(l, c int) bool {
	switch {
	case l < _threshold(c):
		return false
	case l == c:
		cnt := u.Inc()
		if (cnt > 5 && cnt < 10) || cnt%1000 == 999 {
			nlog.WarningDepth(1, ErrWorkChanFull, "[ len:", l, "cap:", c, "]")
		}
		time.Sleep(chanFullSleep)
	default:
		if l == _threshold(c) {
			nlog.WarningDepth(1, ErrWorkChanFull)
		} else {
			runtime.Gosched()
		}
	}
	return true
}

func (u *ChanFull) Load() int64 { return u.Int64.Load() }

/////////////////
// SharMutex16 //
/////////////////

func (shar *SharMutex16) Lock(i int)   { shar.m[i].Lock() }
func (shar *SharMutex16) Unlock(i int) { shar.m[i].Unlock() }
func (shar *SharMutex16) Len() int     { return cap(shar.m) }

func (shar *SharMutex16) Index(id string) int {
	hash := onexxh.Checksum64S(UnsafeB(id), MLCG32)
	return int(hash & uint64(cap(shar.m)-1))
}
