// Package housekeeper provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package housekeeper

import (
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	timedCleanup struct {
		f          CleanupFunc
		updateTime time.Time
	}

	housekeeper struct {
		cleanups []timedCleanup
		timer    *time.Timer
		workCh   chan CleanupFunc
		name     string
		stopCh   cmn.StopCh
	}

	CleanupFunc = func() time.Duration
)

var (
	Housekeeper *housekeeper

	// interface guard
	_ cmn.Runner = &housekeeper{}
)

func init() {
	initCleaner()
}

func initCleaner() {
	Housekeeper = &housekeeper{
		workCh: make(chan CleanupFunc),
		stopCh: cmn.NewStopCh(),
	}
	go Housekeeper.Run()
}

func (hk *housekeeper) Register(f CleanupFunc) {
	hk.workCh <- f
}

func (hk *housekeeper) Setname(name string) {
	hk.name = name
}

func (hk *housekeeper) Getname() string {
	return hk.name
}

func (hk *housekeeper) Run() error {
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
		case f := <-hk.workCh:
			interval := f()
			hk.cleanups = append(hk.cleanups, timedCleanup{
				f:          f,
				updateTime: time.Now().Add(interval),
			})
			hk.updateTimer()
		case <-hk.stopCh.Listen():
			return nil
		}
	}
}

func (c *housekeeper) Stop(err error) {
	c.stopCh.Close()
}

func (hk *housekeeper) updateTimer() {
	sort.Slice(hk.cleanups, func(i, j int) bool {
		return hk.cleanups[i].updateTime.Before(hk.cleanups[j].updateTime)
	})
	hk.timer.Reset(time.Until(hk.cleanups[0].updateTime))
}
