// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"fmt"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn"
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

	housekeeper struct {
		cleanups []timedCleanup
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
		workCh: make(chan request, 10),
	}
	go Housekeeper.run()
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
		case <-hk.timer.C:
			if len(hk.cleanups) == 0 {
				break
			}

			interval := hk.cleanups[0].f()
			hk.cleanups[0].updateTime = time.Now().Add(interval)
			hk.updateTimer()
		case req := <-hk.workCh:
			if req.registering {
				initialInterval := req.initialInterval
				if req.initialInterval == 0 {
					cmn.AssertMsg(req.f != nil, req.name)
					initialInterval = req.f()
				}
				hk.cleanups = append(hk.cleanups, timedCleanup{
					name:       req.name,
					f:          req.f,
					updateTime: time.Now().Add(initialInterval),
				})
			} else {
				foundIdx := -1
				for idx, tc := range hk.cleanups {
					if tc.name == req.name {
						cmn.AssertMsg(foundIdx == -1,
							fmt.Sprintf("multiple cleanup funcs with the same name %q", req.name))
						foundIdx = idx
					}
				}
				cmn.AssertMsg(foundIdx != -1, fmt.Sprintf("cleanup func %q does not exist", req.name))
				hk.cleanups = append(hk.cleanups[:foundIdx], hk.cleanups[foundIdx+1:]...)
			}

			hk.updateTimer()
		}
	}
}

func (hk *housekeeper) updateTimer() {
	if len(hk.cleanups) == 0 {
		hk.timer.Reset(time.Hour)
		return
	}

	sort.Slice(hk.cleanups, func(i, j int) bool {
		return hk.cleanups[i].updateTime.Before(hk.cleanups[j].updateTime)
	})
	hk.timer.Reset(time.Until(hk.cleanups[0].updateTime))
}
