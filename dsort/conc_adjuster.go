// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"math"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

// concAdjuster is responsible for finding optimal number of goroutine which
// maximizes the throughput - it is assumed that such optimal number exists and
// can be achieved. For each mountpath there is new 'optimizer' created called
// mapthAdjuster.
//
// When concAdjuster receives new information about current throughput it will
// firstly check if there is enough information to make a decision or not. If
// the decision can be made (by default it is: defaultBatchSize) the particular
// mpathAdjuster will be updated - update will either increase the number of
// concurrent functions by 1 or decrease it by 1. Next decision will require
// more information - the ratio is defined in batchIncRatio:
// newNumberOfInfosToDecision = prevNumberOfInfos * batchIncRatio. Thanks to
// this we will do more adjustments at the beginning (hopefully reaching the
// optimal point) and then later just do adjustments once in a while in case
// something happened. As we need more information to make decision, the final
// decision is only based on couple of last ones (lastInfoCnt).
//
// There is mechanism which resets number of information required to make
// decision. This mechanism is required in case throughput decreases
// significantly (resetRatio).

const (
	// Default value of initial concurrency limit for functions - defined
	// per mountpath. This value is used when user provided `0` as concurrency
	// limit.
	defaultConcFuncLimit = 5

	// Batch corresponds to number of received update information per mountpath.
	// Every time the size of the batch is reached, recalculation of limit is
	// performed.
	defaultBatchSize = 3
	// Defines how much interval size will increase every time it is changed.
	batchIncRatio = 1.2

	lastInfoCnt = 20 // maximum number of latest adjust information

	// oldThroughput / newThroughput ratio which will trigger resetting number
	// of information required to make decision.
	resetRatio = 2
)

type (
	limitInfo struct {
		limit      float64
		throughput float64
		increasing bool
	}

	adjustInfo struct {
		size      int64
		dur       time.Duration
		mpathInfo *fs.MountpathInfo
	}

	mpathAdjuster struct {
		mu sync.RWMutex

		// Determines how often much information must be processed until we
		// adjust the concurrency limits.
		curBatchSize float64 // float64 to not lose precision
		// Current number of received information, as it reaches the
		// curBatchSize limit update is performed.
		receivedCnt  int
		curLimitInfo limitInfo
		lastInfos    []adjustInfo
		// Semaphore for function calls. On update it is swapped with a new one.
		funcCallsSema *cmn.DynSemaphore
	}

	concAdjuster struct {
		mu sync.RWMutex

		workCh chan adjustInfo
		stopCh cmn.StopCh

		defaultLimit int64 // default limit for new mpath adjusters
		adjusters    map[string]*mpathAdjuster

		// Determines how many goroutines should be allowed per one function call.
		goroutineLimitCoef int64
		gorountinesSema    *cmn.DynSemaphore
	}
)

func newMpathAdjuster(limit int64) *mpathAdjuster {
	return &mpathAdjuster{
		curBatchSize: defaultBatchSize,
		curLimitInfo: limitInfo{
			limit: float64(limit),
		},
		lastInfos:     make([]adjustInfo, 0, lastInfoCnt),
		funcCallsSema: cmn.NewDynSemaphore(limit),
	}
}

func newConcAdjuster(limit, goroutineLimitCoef int64) *concAdjuster {
	if limit == 0 {
		limit = defaultConcFuncLimit
	}

	availablePaths, _ := fs.Mountpaths.Get()
	adjusters := make(map[string]*mpathAdjuster, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		adjusters[mpathInfo.Path] = newMpathAdjuster(limit)
	}

	return &concAdjuster{
		workCh: make(chan adjustInfo, 1000),
		stopCh: cmn.NewStopCh(),

		defaultLimit: limit,
		adjusters:    adjusters,

		goroutineLimitCoef: goroutineLimitCoef,
		gorountinesSema:    cmn.NewDynSemaphore(goroutineLimitCoef * int64(len(availablePaths)) * limit),
	}
}

func (ca *concAdjuster) start() {
	go ca.run()
}

func (ca *concAdjuster) run() {
	for {
		select {
		case info := <-ca.workCh:
			ca.mu.RLock()
			adjuster, ok := ca.adjusters[info.mpathInfo.Path]
			// Adjuster should be added in `acquireSema`, so before info will be
			// put/received on workCh - that is why we can assert here.
			cmn.Assert(ok)
			ca.mu.RUnlock()

			adjuster.mu.Lock()

			adjuster.lastInfos = append(adjuster.lastInfos, info)
			if len(adjuster.lastInfos) > lastInfoCnt {
				adjuster.lastInfos = adjuster.lastInfos[1:]
			}
			adjuster.receivedCnt++
			if float64(adjuster.receivedCnt) >= adjuster.curBatchSize {
				var (
					totalSize int64
					totalDur  float64
					prevLimit = adjuster.curLimitInfo.limit
				)

				// Calculate average of lastInfos and use it as throughput
				for _, info := range adjuster.lastInfos {
					totalSize += info.size
					totalDur += info.dur.Seconds()
				}
				newThroughput := float64(totalSize) / totalDur
				reset := adjuster.curLimitInfo.recalc(newThroughput)

				if adjuster.curLimitInfo.limit != prevLimit {
					adjuster.funcCallsSema.SetSize(int64(adjuster.curLimitInfo.limit))

					diff := ca.goroutineLimitCoef * int64(adjuster.curLimitInfo.limit-prevLimit)
					ca.gorountinesSema.SetSize(ca.gorountinesSema.Size() + diff)
				}

				if reset {
					adjuster.curBatchSize = defaultBatchSize
				} else {
					adjuster.curBatchSize = batchIncRatio * adjuster.curBatchSize
				}

				adjuster.receivedCnt = 0
			}

			adjuster.mu.Unlock()
		case <-ca.stopCh.Listen():
			return
		}
	}
}

func (ca *concAdjuster) stop() {
	ca.stopCh.Close()
}

func (ca *concAdjuster) inform(size int64, dur time.Duration, mpathInfo *fs.MountpathInfo) {
	ca.workCh <- adjustInfo{size: size, dur: dur, mpathInfo: mpathInfo}
}

func (ca *concAdjuster) acquireSema(mpathInfo *fs.MountpathInfo) {
	ca.mu.Lock()
	adjuster, ok := ca.adjusters[mpathInfo.Path]
	if !ok {
		adjuster = newMpathAdjuster(ca.defaultLimit)
		ca.adjusters[mpathInfo.Path] = adjuster

		// Also we need to update goroutine semaphore size
		diff := ca.goroutineLimitCoef * ca.defaultLimit
		ca.gorountinesSema.SetSize(ca.gorountinesSema.Size() + diff)
	}
	ca.mu.Unlock()
	adjuster.funcCallsSema.Acquire()
}

func (ca *concAdjuster) releaseSema(mpathInfo *fs.MountpathInfo) {
	ca.mu.RLock()
	adjuster := ca.adjusters[mpathInfo.Path]
	ca.mu.RUnlock()
	adjuster.funcCallsSema.Release()
}

func (ca *concAdjuster) acquireGoroutineSema() {
	ca.gorountinesSema.Acquire()
}

func (ca *concAdjuster) releaseGoroutineSema() {
	ca.gorountinesSema.Release()
}

func (li *limitInfo) recalc(newThroughput float64) (reset bool) {
	if newThroughput-li.throughput < 0 {
		// Change direction - throughput has decreased.
		li.increasing = !li.increasing

		// If new throughput is significantly lower than previous one, request reset.
		if li.throughput/newThroughput <= resetRatio {
			reset = true
		}
	}

	// Change the decision
	if li.increasing {
		li.limit++
	} else {
		li.limit--
	}

	li.limit = math.Max(li.limit, 1)
	li.throughput = newThroughput
	return
}
