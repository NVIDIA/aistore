// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	initialCap     = 256             // initial capacity: actions (heap and map)
	warnPendingLen = initialCap << 2 // log a warning at each power-of-2 multiple (compare with cos.SparseWarn)
	NameSuffix     = ".gc"           // reg name suffix (callers must use it for consistency across codebase)
)

const (
	DayInterval   = 24 * time.Hour
	UnregInterval = 365 * DayInterval // to unregister upon return from the callback
)

type (
	HKCB func(now int64) time.Duration
	op   struct {
		f        HKCB
		name     string
		interval time.Duration
	}
	timedAction struct {
		f          HKCB
		name       string
		updateTime int64
	}
	timedActions struct {
		byName map[string]int // action name => heap index
		heap   []timedAction
	}

	hk struct {
		sigCh    chan os.Signal
		actions  *timedActions
		timer    *time.Timer
		nextWake int64
		stopCh   cos.StopCh
		running  atomic.Bool

		// unbounded mailbox (callers (Reg/Unreg/UnregIf) never block)
		mtx     sync.Mutex
		pending []op
		free    []op          // recycled drain buffer
		workCh  chan struct{} // doorbell
	}
)

// singleton
var HK *hk

// interface guard
var _ cos.Runner = (*hk)(nil)

func Init(mustRun bool) {
	HK = &hk{
		workCh:  make(chan struct{}, 1),
		sigCh:   make(chan os.Signal, 1),
		pending: make([]op, 0, initialCap),
		free:    make([]op, 0, initialCap),
		actions: &timedActions{
			byName: make(map[string]int, initialCap),
			heap:   make([]timedAction, 0, initialCap),
		},
	}
	HK.stopCh.Init()
	HK.running.Store(!mustRun)
}

func WaitStarted() {
	for !HK.running.Load() {
		time.Sleep(time.Second)
	}
}

func Reg(name string, f HKCB, interval time.Duration) {
	debug.Assert(nlog.Stopping() || HK.running.Load())
	debug.Assert(interval != UnregInterval)
	HK.enq(op{name: name, f: f, interval: interval})
}

func Unreg(name string) {
	debug.Assert(nlog.Stopping() || HK.running.Load())
	HK.enq(op{name: name, interval: UnregInterval})
}

// non-presence is fine
func UnregIf(name string, f HKCB) {
	HK.enq(op{name: name, f: f, interval: UnregInterval})
}

// add +/-3% pseudo-random jitter to the housekeeping interval `d`
func Jitter(d time.Duration, now int64) time.Duration {
	step := int64(d >> 7)
	n := (now & 0x7) - 4
	return d + time.Duration(n*step)
}

////////
// hk //
////////

func (*hk) Name() string { return "hk" }

func (hk *hk) terminate() {
	hk.timer.Stop()
	hk.nextWake = 0
	hk.running.Store(false)
}

func (*hk) Stop(error) { HK.stopCh.Close() }

func (hk *hk) Run() error {
	hk.setSignal() // SIGINT, et al. (see hk.handleSignal)

	hk.timer = time.NewTimer(time.Hour)
	hk.timer.Stop()
	hk.running.Store(true)

	err := hk._run()

	hk.terminate()
	return err
}

// enqueue (without blocking on chan cap or anything else; similar transport/collect pattern)
func (hk *hk) enq(op op) {
	hk.mtx.Lock()
	hk.pending = append(hk.pending, op)
	l := len(hk.pending)
	hk.mtx.Unlock()

	select {
	case hk.workCh <- struct{}{}:
	default: // doorbell already rung
	}

	if l >= warnPendingLen && l&(l-1) == 0 {
		nlog.Errorln("pending queue not draining [ len:", l, "]")
	}
}

func (hk *hk) _run() error {
	for {
		hk.drain()
		hk.updateTimer()
		select {
		case <-hk.stopCh.Listen():
			return nil

		case <-hk.timer.C:
			hk.nextWake = 0 // timer fired; force re-arm on next pass
			hk.runDue()

		case <-hk.workCh:
			// doorbell: drain at the top of the loop

		case s, ok := <-hk.sigCh:
			if ok {
				if err := hk.handleSignal(s.(syscall.Signal)); err != nil {
					return err
				}
			}
		}
	}
}

func (hk *hk) drain() {
	hk.mtx.Lock()
	if len(hk.pending) == 0 {
		hk.mtx.Unlock()
		return
	}
	ops := hk.pending

	// swap to reuse existing (free) cap; preserve max cap
	if cap(hk.free) < cap(ops) {
		hk.pending = make([]op, 0, cap(ops))
	} else {
		hk.pending = hk.free[:0]
	}
	hk.mtx.Unlock()

	for i := range ops {
		hk.handle(&ops[i])
	}
	clear(ops)
	hk.free = ops[:0]
}

func (hk *hk) handle(op *op) {
	idx := hk.actions.find(op.name)
	if op.interval == UnregInterval {
		if idx >= 0 {
			hk.actions.remove(idx)
		} else if op.f == nil {
			nlog.Warningln(op.name, "not found (already removed?)")
			debug.Assert(false, op.name)
		}
		// op.f != nil via UnregIf()
		return
	}
	if idx >= 0 {
		nlog.Errorln("duplicated name [", op.name, "] - not registering")
		return
	}
	var (
		ival = op.interval
		now  = mono.NanoTime()
	)
	if op.interval == 0 {
		// calling right away
		ival = op.f(now)
		if ival == UnregInterval {
			nlog.Errorln("illegal usage [", op.name, "] - not registering")
			debug.Assert(false)
			return
		}
	}
	if ival <= 0 {
		debug.Assert(false, op.name, " invalid interval: ", ival)
		ival = time.Second
	}
	// next time
	nt := now + ival.Nanoseconds()
	hk.actions.push(timedAction{name: op.name, f: op.f, updateTime: nt})
}

// call and update the heap - all due actions, not just the top one
func (hk *hk) runDue() {
	for hk.actions.Len() > 0 {
		var (
			item    = hk.actions.Peek()
			started = mono.NanoTime()
		)
		if item.updateTime > started {
			return
		}
		ival := item.f(started)
		if ival == UnregInterval {
			hk.actions.remove(0)
			continue
		}
		name := item.name
		if ival <= 0 {
			debug.Assert(false, name, " invalid interval: ", ival)
			ival = time.Second
		}
		now := mono.NanoTime()
		item.updateTime = now + ival.Nanoseconds()
		hk.actions.fix(0)

		// either extremely loaded or
		// lock/sleep type contention inside the callback
		if d := time.Duration(now - started); d > time.Second {
			nlog.Warningln("call[", name, "] duration exceeds 1s:", d.String())
		}
	}
}

func (hk *hk) updateTimer() {
	if hk.actions.Len() == 0 {
		if hk.nextWake != 0 {
			hk.timer.Stop()
			hk.nextWake = 0
		}
		return
	}

	next := hk.actions.Peek().updateTime
	if next == hk.nextWake {
		return // Wake-up target didn't change; avoid runtime timer reset
	}

	hk.nextWake = next
	d := time.Duration(next - mono.NanoTime())
	hk.timer.Reset(d)
}

//////////////////
// timedActions //
//////////////////

func (tc *timedActions) Len() int           { return len(tc.heap) }
func (tc *timedActions) Peek() *timedAction { return &tc.heap[0] }

func (tc *timedActions) find(name string) int {
	if idx, ok := tc.byName[name]; ok {
		return idx
	}
	return -1
}

func (tc *timedActions) push(item timedAction) {
	tc.byName[item.name] = len(tc.heap)
	tc.heap = append(tc.heap, item)
	tc.up(len(tc.heap) - 1)
}

func (tc *timedActions) remove(i int) {
	n := len(tc.heap) - 1
	if n != i {
		tc.swap(i, n)
		if !tc.down(i, n) {
			tc.up(i)
		}
	}
	item := tc.heap[n]
	tc.heap[n] = timedAction{} // clear reference for GC
	tc.heap = tc.heap[:n]
	delete(tc.byName, item.name)
}

func (tc *timedActions) fix(i int) {
	if !tc.down(i, len(tc.heap)) {
		tc.up(i)
	}
}

func (tc *timedActions) swap(i, j int) {
	h := tc.heap
	h[i], h[j] = h[j], h[i]
	tc.byName[h[i].name] = i
	tc.byName[h[j].name] = j
}

func (tc *timedActions) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || tc.heap[j].updateTime >= tc.heap[i].updateTime {
			break
		}
		tc.swap(i, j)
		j = i
	}
}

func (tc *timedActions) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && tc.heap[j2].updateTime < tc.heap[j1].updateTime {
			j = j2
		}
		if tc.heap[j].updateTime >= tc.heap[i].updateTime {
			break
		}
		tc.swap(i, j)
		i = j
	}
	return i > i0
}
