// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"container/heap"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	DayInterval = 24 * time.Hour
)

type (
	request struct {
		registering     bool
		name            string
		f               CleanupFunc
		initialInterval time.Duration
	}

	timedCleanup struct {
		name       string
		f          CleanupFunc
		updateTime time.Time
	}
	timedCleanups []timedCleanup

	housekeeper struct {
		stopCh   *cmn.StopCh
		cleanups *timedCleanups
		timer    *time.Timer
		workCh   chan request
	}

	CleanupFunc = func() time.Duration
)

var (
	Housekeeper *housekeeper
)

func init() {
	initCleaner()
}

func initCleaner() {
	Housekeeper = &housekeeper{
		workCh:   make(chan request, 10),
		stopCh:   cmn.NewStopCh(),
		cleanups: &timedCleanups{},
	}
	heap.Init(Housekeeper.cleanups)

	go Housekeeper.run()
}

func (tc timedCleanups) Len() int            { return len(tc) }
func (tc timedCleanups) Less(i, j int) bool  { return tc[i].updateTime.After(tc[j].updateTime) }
func (tc timedCleanups) Swap(i, j int)       { tc[i], tc[j] = tc[j], tc[i] }
func (tc timedCleanups) Peek() *timedCleanup { return &tc[len(tc)-1] }
func (tc *timedCleanups) Push(x interface{}) { *tc = append(*tc, x.(timedCleanup)) }
func (tc *timedCleanups) Pop() interface{} {
	old := *tc
	n := len(old)
	item := old[n-1]
	*tc = old[0 : n-1]
	return item
}

func (hk *housekeeper) Register(name string, f CleanupFunc, initialInterval ...time.Duration) {
	var interval time.Duration
	if len(initialInterval) > 0 {
		interval = initialInterval[0]
	}
	hk.workCh <- request{
		registering:     true,
		name:            name,
		f:               f,
		initialInterval: interval,
	}
}

func (hk *housekeeper) Unregister(name string) {
	hk.workCh <- request{
		registering: false,
		name:        name,
	}
}

func (hk *housekeeper) run() {
	hk.timer = time.NewTimer(time.Hour)
	defer hk.timer.Stop()

	for {
		select {
		case <-hk.stopCh.Listen():
			return
		case <-hk.timer.C:
			if hk.cleanups.Len() == 0 {
				break
			}

			// Run callback and update the item in the heap.
			item := hk.cleanups.Peek()
			interval := item.f()
			item.updateTime = time.Now().Add(interval)
			heap.Fix(hk.cleanups, hk.cleanups.Len()-1)

			hk.updateTimer()
		case req := <-hk.workCh:
			if req.registering {
				cmn.AssertMsg(req.f != nil, req.name)
				initialInterval := req.initialInterval
				if req.initialInterval == 0 {
					initialInterval = req.f()
				}
				heap.Push(hk.cleanups, timedCleanup{
					name:       req.name,
					f:          req.f,
					updateTime: time.Now().Add(initialInterval),
				})
			} else {
				foundIdx := -1
				for idx, tc := range *hk.cleanups {
					if tc.name == req.name {
						foundIdx = idx
						break
					}
				}
				debug.Assertf(foundIdx != -1, "cleanup func %q does not exist", req.name)
				heap.Remove(hk.cleanups, foundIdx)
			}

			hk.updateTimer()
		}
	}
}

func (hk *housekeeper) updateTimer() {
	if hk.cleanups.Len() == 0 {
		hk.timer.Stop()
		return
	}
	hk.timer.Reset(time.Until(hk.cleanups.Peek().updateTime))
}

func (hk *housekeeper) Abort() {
	hk.stopCh.Close()
}
