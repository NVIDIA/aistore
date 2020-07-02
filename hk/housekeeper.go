// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	DayInterval = 24 * time.Hour
)

type (
	Action interface {
		fmt.Stringer
		Housekeep() time.Duration
	}

	request struct {
		registering     bool
		name            string
		f               CleanupFunc
		initialInterval time.Duration
	}

	timedAction struct {
		name       string
		f          CleanupFunc
		updateTime time.Time
	}
	timedActions []timedAction

	housekeeper struct {
		stopCh  *cmn.StopCh
		actions *timedActions
		timer   *time.Timer
		workCh  chan request
	}

	CleanupFunc = func() time.Duration
)

var (
	defaultHK *housekeeper
)

func init() {
	initCleaner()
}

func initCleaner() {
	defaultHK = &housekeeper{
		workCh:  make(chan request, 10),
		stopCh:  cmn.NewStopCh(),
		actions: &timedActions{},
	}
	heap.Init(defaultHK.actions)

	go defaultHK.run()
}

func (tc timedActions) Len() int            { return len(tc) }
func (tc timedActions) Less(i, j int) bool  { return tc[i].updateTime.Before(tc[j].updateTime) }
func (tc timedActions) Swap(i, j int)       { tc[i], tc[j] = tc[j], tc[i] }
func (tc timedActions) Peek() *timedAction  { return &tc[0] }
func (tc *timedActions) Push(x interface{}) { *tc = append(*tc, x.(timedAction)) }
func (tc *timedActions) Pop() interface{} {
	old := *tc
	n := len(old)
	item := old[n-1]
	*tc = old[0 : n-1]
	return item
}

func Reg(name string, f CleanupFunc, initialInterval ...time.Duration) {
	var interval time.Duration
	if len(initialInterval) > 0 {
		interval = initialInterval[0]
	}
	defaultHK.workCh <- request{
		registering:     true,
		name:            name,
		f:               f,
		initialInterval: interval,
	}
}

func Unreg(name string) {
	defaultHK.workCh <- request{
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
			if hk.actions.Len() == 0 {
				break
			}

			// Run callback and update the item in the heap.
			item := hk.actions.Peek()
			interval := item.f()
			item.updateTime = time.Now().Add(interval)
			heap.Fix(hk.actions, 0)

			hk.updateTimer()
		case req := <-hk.workCh:
			if req.registering {
				cmn.AssertMsg(req.f != nil, req.name)
				initialInterval := req.initialInterval
				if req.initialInterval == 0 {
					initialInterval = req.f()
				}
				heap.Push(hk.actions, timedAction{
					name:       req.name,
					f:          req.f,
					updateTime: time.Now().Add(initialInterval),
				})
			} else {
				foundIdx := -1
				for idx, tc := range *hk.actions {
					if tc.name == req.name {
						foundIdx = idx
						break
					}
				}
				debug.Assertf(foundIdx != -1, "cleanup func %q does not exist", req.name)
				heap.Remove(hk.actions, foundIdx)
			}

			hk.updateTimer()
		}
	}
}

func (hk *housekeeper) updateTimer() {
	if hk.actions.Len() == 0 {
		hk.timer.Stop()
		return
	}
	hk.timer.Reset(time.Until(hk.actions.Peek().updateTime))
}

func Abort() {
	defaultHK.stopCh.Close()
}
