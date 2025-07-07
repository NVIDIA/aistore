// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"container/heap"
	"os"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const workChanCap = 128

const NameSuffix = ".gc" // reg name suffix

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
	timedActions []timedAction

	hk struct {
		stopCh  cos.StopCh
		sigCh   chan os.Signal
		actions *timedActions
		timer   *time.Timer
		workCh  chan op
		running atomic.Bool
	}
)

// singleton
var HK *hk

// interface guard
var _ cos.Runner = (*hk)(nil)

func Init(mustRun bool) {
	HK = &hk{
		workCh:  make(chan op, workChanCap),
		sigCh:   make(chan os.Signal, 1),
		actions: &timedActions{},
	}
	HK.stopCh.Init()
	HK.running.Store(!mustRun)
	heap.Init(HK.actions)
}

func WaitStarted() {
	for !HK.running.Load() {
		time.Sleep(time.Second)
	}
}

func Reg(name string, f HKCB, interval time.Duration) {
	debug.Assert(nlog.Stopping() || HK.running.Load())
	debug.Assert(interval != UnregInterval)

	HK.workCh <- op{name: name, f: f, interval: interval}

	if l, c := len(HK.workCh), workChanCap; l >= (c - c>>3) {
		nlog.Errorln(cos.ErrWorkChanFull, "len", l, "cap", c)
	}
}

func Unreg(name string) {
	debug.Assert(nlog.Stopping() || HK.running.Load())
	HK.workCh <- op{name: name, interval: UnregInterval}
}

// non-presence is fine
func UnregIf(name string, f HKCB) {
	HK.workCh <- op{name: name, f: f, interval: UnregInterval}
}

////////
// hk //
////////

func (*hk) Name() string { return "hk" }

func (hk *hk) terminate() {
	hk.timer.Stop()
	hk.running.Store(false)
}

func (*hk) Stop(error) { HK.stopCh.Close() }

func (hk *hk) Run() error {
	hk.setSignal() // SIGINT, et al. (see hk.handleSignal)

	hk.timer = time.NewTimer(time.Hour)
	hk.running.Store(true)

	err := hk._run()

	hk.terminate()
	return err
}

func (hk *hk) _run() error {
	for {
		select {
		case <-hk.stopCh.Listen():
			return nil

		case <-hk.timer.C:
			if hk.actions.Len() == 0 {
				break
			}
			// call and update the heap
			var (
				item    = hk.actions.Peek()
				started = mono.NanoTime()
				ival    = item.f(started)
			)
			if ival == UnregInterval {
				heap.Remove(hk.actions, 0)
			} else {
				now := mono.NanoTime()
				item.updateTime = now + ival.Nanoseconds()
				heap.Fix(hk.actions, 0)

				// either extremely loaded or
				// lock/sleep type contention inside the callback
				if d := time.Duration(now - started); d > time.Second {
					nlog.Warningln("call[", item.name, "] duration exceeds 1s:", d.String())
				}
			}
			hk.updateTimer()

		case op := <-hk.workCh:
			idx := hk.byName(op.name)
			if op.interval != UnregInterval {
				if idx >= 0 {
					nlog.Errorln("duplicated name [", op.name, "] - not registering")
					break
				}
				ival := op.interval
				now := mono.NanoTime()
				if op.interval == 0 {
					// calling right away
					ival = op.f(now)
					if ival == UnregInterval {
						nlog.Errorln("illegal usage [", op.name, "] - not registering")
						debug.Assert(false)
						break
					}
				}
				// next time
				nt := now + ival.Nanoseconds()
				heap.Push(hk.actions, timedAction{name: op.name, f: op.f, updateTime: nt})
			} else {
				if idx >= 0 {
					heap.Remove(hk.actions, idx)
				} else if op.f == nil {
					nlog.Warningln(op.name, "not found (already removed?)")
					debug.Assert(false, op.name)
				}
				// op.f != nil via UnregIf()
			}
			hk.updateTimer()

		case s, ok := <-hk.sigCh:
			if ok {
				if err := hk.handleSignal(s.(syscall.Signal)); err != nil {
					return err
				}
			}
		}
	}
}

func (hk *hk) updateTimer() {
	if hk.actions.Len() == 0 {
		hk.timer.Stop()
		return
	}
	d := hk.actions.Peek().updateTime - mono.NanoTime()
	hk.timer.Reset(time.Duration(d))
}

func (hk *hk) byName(name string) int {
	for i, tc := range *hk.actions {
		if tc.name == name {
			return i
		}
	}
	return -1
}

//////////////////
// timedActions //
//////////////////

func (tc timedActions) Len() int           { return len(tc) }
func (tc timedActions) Less(i, j int) bool { return tc[i].updateTime < tc[j].updateTime }
func (tc timedActions) Swap(i, j int)      { tc[i], tc[j] = tc[j], tc[i] }
func (tc timedActions) Peek() *timedAction { return &tc[0] }
func (tc *timedActions) Push(x any)        { *tc = append(*tc, x.(timedAction)) }

func (tc *timedActions) Pop() any {
	old := *tc
	n := len(old)
	item := old[n-1]
	*tc = old[0 : n-1]
	return item
}
