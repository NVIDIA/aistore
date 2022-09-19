// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"container/heap"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

const NameSuffix = ".gc" // reg name suffix

const (
	DayInterval   = 24 * time.Hour
	UnregInterval = 365 * DayInterval // to unregister upon return from the callback
)

type (
	Action interface {
		fmt.Stringer
		Housekeep() time.Duration
	}
	request struct {
		f               CleanupFunc
		name            string
		initialInterval time.Duration
		registering     bool
	}
	timedAction struct {
		f          CleanupFunc
		name       string
		updateTime int64
	}
	timedActions []timedAction

	housekeeper struct {
		stopCh   *cos.StopCh
		sigCh    chan os.Signal
		actions  *timedActions
		timer    *time.Timer
		workCh   chan request
		stopping *atomic.Bool
		running  atomic.Bool
	}

	CleanupFunc = func() time.Duration
)

var DefaultHK *housekeeper

// interface guard
var _ cos.Runner = (*housekeeper)(nil)

func TestInit() {
	_init(false)
	DefaultHK.stopping = &atomic.Bool{} // dummy
}

func Init(stopping *atomic.Bool) {
	_init(true)
	DefaultHK.stopping = stopping
}

func _init(mustRun bool) {
	DefaultHK = &housekeeper{
		workCh:  make(chan request, 512),
		stopCh:  cos.NewStopCh(),
		sigCh:   make(chan os.Signal, 1),
		actions: &timedActions{},
	}
	if mustRun {
		DefaultHK.running.Store(false)
	} else {
		DefaultHK.running.Store(true) // tests only
	}
	heap.Init(DefaultHK.actions)
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

/////////////////
// housekeeper //
/////////////////

func WaitStarted() {
	for !DefaultHK.running.Load() {
		time.Sleep(time.Second)
	}
}

func Reg(name string, f CleanupFunc, interval time.Duration) {
	debug.Assert(DefaultHK.stopping.Load() || DefaultHK.running.Load())
	DefaultHK.workCh <- request{
		registering:     true,
		name:            name,
		f:               f,
		initialInterval: interval,
	}
}

func Unreg(name string) {
	debug.Assert(DefaultHK.stopping.Load() || DefaultHK.running.Load())
	DefaultHK.workCh <- request{
		registering: false,
		name:        name,
	}
}

func (*housekeeper) Name() string { return "housekeeper" }

func (hk *housekeeper) terminate() {
	hk.timer.Stop()
	hk.running.Store(false)
}

func (hk *housekeeper) Run() (err error) {
	signal.Notify(hk.sigCh,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGTERM, // kill -SIGTERM XXXX
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
	)
	hk.timer = time.NewTimer(time.Hour)
	defer hk.terminate()

	hk.running.Store(true)
	for {
		select {
		case <-hk.stopCh.Listen():
			return
		case <-hk.timer.C:
			if hk.actions.Len() == 0 {
				break
			}
			// Run callback and update the item in the heap.
			item := hk.actions.Peek()
			interval := item.f()
			if interval == UnregInterval {
				heap.Remove(hk.actions, 0)
			} else {
				item.updateTime = mono.NanoTime() + interval.Nanoseconds()
				heap.Fix(hk.actions, 0)
			}
			hk.updateTimer()
		case req := <-hk.workCh:
			if req.registering {
				debug.AssertMsg(req.f != nil, req.name)
				debug.AssertMsg(req.initialInterval != UnregInterval, req.name) // cannot reg w/unreg
				debug.AssertMsg(hk.byName(req.name) == -1, req.name)            // duplicate name

				initialInterval := req.initialInterval
				if req.initialInterval == 0 {
					initialInterval = req.f()
				}
				nt := mono.NanoTime() + initialInterval.Nanoseconds() // next time
				heap.Push(hk.actions, timedAction{name: req.name, f: req.f, updateTime: nt})
			} else {
				idx := hk.byName(req.name)
				if idx >= 0 {
					heap.Remove(hk.actions, idx)
				} else {
					debug.AssertMsg(false, req.name)
					glog.Warningln(req.name, "already removed")
				}
			}
			hk.updateTimer()
		case s, ok := <-hk.sigCh:
			if ok {
				signal.Stop(hk.sigCh)
				err := cos.NewSignalError(s.(syscall.Signal))
				hk.Stop(err)
				return err
			}
		}
	}
}

func (hk *housekeeper) updateTimer() {
	if hk.actions.Len() == 0 {
		hk.timer.Stop()
		return
	}
	d := hk.actions.Peek().updateTime - mono.NanoTime()
	hk.timer.Reset(time.Duration(d))
}

func (hk *housekeeper) byName(name string) int {
	for i, tc := range *hk.actions {
		if tc.name == name {
			return i
		}
	}
	return -1
}

func (*housekeeper) Stop(_ error) { DefaultHK.stopCh.Close() }
