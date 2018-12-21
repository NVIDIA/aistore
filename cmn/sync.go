// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"sync/atomic"
	"time"
)

type (
	// TimeoutGroup is similar to sync.WaitGroup with the difference on Wait
	// where we only allow timing out.
	//
	// NOTE: It should not be used in critical code as it may have worse
	// performance than sync.WaitGroup - use only if its needed.
	//
	// NOTE: It is not recommended to reuse the TimeoutGroup - it was not
	// designed for that and bugs can be expected, especially when previous
	// group was not called with successful (without timeout) WaitTimeout.
	TimeoutGroup struct {
		jobsLeft  int64 // counter for jobs left to be done
		postedFin int32 // determines if we have already posted fin signal
		fin       chan struct{}
	}
)

func NewTimeoutGroup() *TimeoutGroup {
	return &TimeoutGroup{
		jobsLeft: 0,
		fin:      make(chan struct{}, 1),
	}
}

func (tg *TimeoutGroup) Add(delta int) {
	atomic.AddInt64(&tg.jobsLeft, int64(delta))
}

// WaitTimeout waits until jobs is finished or times out.
// In case of timeout it returns true.
//
// NOTE: WaitTimeout can be only invoked after all Adds!
func (tg *TimeoutGroup) WaitTimeout(timeout time.Duration) bool {
	select {
	case <-tg.fin:
		atomic.StoreInt32(&tg.postedFin, 0)
		return false
	case <-time.After(timeout):
		return true
	}
}

// Done decrements number of jobs left to do. Panics if the number jobs left is
// less than 0.
func (tg *TimeoutGroup) Done() {
	if left := atomic.AddInt64(&tg.jobsLeft, -1); left == 0 {
		if posted := atomic.SwapInt32(&tg.postedFin, 1); posted == 0 {
			tg.fin <- struct{}{}
		}
	} else if left < 0 {
		panic(left)
	}
}
