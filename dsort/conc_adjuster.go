// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
)

// concAdjuster is responsible for finding optimal number of goroutine which
// maximizes the utilization - it is assumed that such optimal number exists and
// can be achieved. For each mountpath there is new 'optimizer' created called
// mapthAdjuster.
//
// Once in a while concAdjuster asks for current disks utilization. It saves
// them for future use. When it has enough information to make decision it will
// average previously collected disk utilization and based on new utilization
// it will adjust the concurrency limit. Number of information which are needed
// to make decision increases every time. This mechanism prevents from doing
// a lot of adjustments in short time but rather tries to find the best value
// at the very beginning (doing a lot of small steps) and then just keeps
// oscillating around the desired watermark.
//
// There is mechanism which resets number of information required to make
// decision. This mechanism is required in case utilization decreases
// significantly (resetRatio).

const (
	// Default value of initial concurrency limit for functions - defined per mountpath.
	// This value is used when user provided `0` as concurrency limit.
	defaultConcFuncLimit = 10
	// Determines maximal concurrency limit per disk.
	//
	// Setting this too high can result in big number of goroutines in the system
	// what can put tremendous pressure on scheduler and other goroutines in the
	// system - eg. http handlers which are spawned in new goroutines.
	maxConcFuncPerDiskLimit = 15
	// Determines maximal concurrency limit per dSort per node.
	maxConcFuncPerDSortLimit = 200
	// TODO: add global max concurrency limit - should be preserved between
	//  all dSorts on single node.

	// Batch corresponds to number of received update information per mountpath.
	// Every time the size of the batch is reached, recalculation of limit is
	// performed.
	defaultBatchSize = 10
	// Defines how much interval size will increase every time it is changed.
	batchIncRatio = 1.2
	// Defines the maximal batch size - this prevents from growing batch indefinitely.
	maxBatchSize = 200

	lastInfoCnt = 30 // maximum number of latest adjust information
)

type (
	mpathAdjuster struct {
		// Determines how often much information must be processed until we
		// adjust the concurrency limits.
		curBatchSize float64 // float64 to not lose precision
		// Current number updates, as it reaches the curBatchSize
		// limit update is performed.
		tickCnt   int
		limit     int
		maxLimit  int
		lastUtils []int64
		// Semaphore for function calls. On update it is swapped with a new one.
		funcCallsSema *cos.DynSemaphore
	}

	concAdjuster struct {
		mu sync.RWMutex

		stopCh *cos.StopCh

		defaultLimit int // default limit for new mpath adjusters
		adjusters    map[string]*mpathAdjuster

		// Determines how many goroutines should be allowed per one function call.
		goroutineLimitCoef int
		gorountinesSema    *cos.DynSemaphore
	}
)

func calcMaxLimit() int {
	availablePaths := fs.GetAvail()
	maxLimitPerDisk := cos.Min(
		maxConcFuncPerDiskLimit,
		maxConcFuncPerDSortLimit/cos.Max(len(availablePaths), 1),
	)
	return maxLimitPerDisk
}

func newMpathAdjuster(limit, maxLimit int) *mpathAdjuster {
	return &mpathAdjuster{
		curBatchSize:  defaultBatchSize,
		limit:         limit,
		maxLimit:      maxLimit,
		lastUtils:     make([]int64, 0, lastInfoCnt),
		funcCallsSema: cos.NewDynSemaphore(limit),
	}
}

func newConcAdjuster(maxLimit, goroutineLimitCoef int) *concAdjuster {
	availablePaths := fs.GetAvail()
	adjusters := make(map[string]*mpathAdjuster, len(availablePaths))
	if maxLimit == 0 {
		maxLimit = calcMaxLimit()
	}
	limit := cos.Min(defaultConcFuncLimit, maxLimit)
	for _, mpathInfo := range availablePaths {
		adjusters[mpathInfo.Path] = newMpathAdjuster(limit, maxLimit)
	}

	return &concAdjuster{
		stopCh:             cos.NewStopCh(),
		defaultLimit:       limit,
		adjusters:          adjusters,
		goroutineLimitCoef: goroutineLimitCoef,
		gorountinesSema:    cos.NewDynSemaphore(goroutineLimitCoef * len(availablePaths) * limit),
	}
}

func (ca *concAdjuster) start() {
	go ca.run()
}

func (ca *concAdjuster) run() {
	ticker := time.NewTicker(cmn.GCO.Get().Disk.IostatTimeShort.D())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var (
				config = cmn.GCO.Get()
				utils  = fs.GetAllMpathUtils()
			)
			ca.mu.RLock()
			for mpath, adjuster := range ca.adjusters {
				util := utils.Util(mpath)

				adjuster.lastUtils = append(adjuster.lastUtils, util)
				if len(adjuster.lastUtils) > lastInfoCnt {
					adjuster.lastUtils = adjuster.lastUtils[1:]
				}
				adjuster.tickCnt++

				if float64(adjuster.tickCnt) >= adjuster.curBatchSize {
					var totalUtil int64
					for _, util := range adjuster.lastUtils {
						totalUtil += util
					}
					newUtil := totalUtil / int64(len(adjuster.lastUtils))

					prevLimit, newLimit := adjuster.recalc(newUtil, config)
					if prevLimit != newLimit {
						adjuster.funcCallsSema.SetSize(newLimit)
						diff := ca.goroutineLimitCoef * (newLimit - prevLimit)
						ca.gorountinesSema.SetSize(ca.gorountinesSema.Size() + diff)
					}

					adjuster.tickCnt = 0
				}
			}
			ca.mu.RUnlock()
		case <-ca.stopCh.Listen():
			return
		}
	}
}

func (ca *concAdjuster) stop() {
	ca.stopCh.Close()
}

func (ca *concAdjuster) acquireSema(mpathInfo *fs.MountpathInfo) {
	ca.mu.Lock()
	adjuster, ok := ca.adjusters[mpathInfo.Path]
	if !ok {
		maxLimit := calcMaxLimit()
		adjuster = newMpathAdjuster(ca.defaultLimit, maxLimit)
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

func (adjuster *mpathAdjuster) recalc(newUtil int64, config *cmn.Config) (prevLimit, newLimit int) {
	prevLimit = adjuster.limit
	switch {
	case newUtil < config.Disk.DiskUtilLowWM:
		adjuster.limit *= 2
		adjuster.curBatchSize *= batchIncRatio
	case newUtil <= config.Disk.DiskUtilHighWM:
		adjuster.limit += 2
		adjuster.curBatchSize *= batchIncRatio
	case newUtil > config.Disk.DiskUtilMaxWM:
		adjuster.limit -= 2
	default:
		adjuster.limit++
		adjuster.curBatchSize *= 2 * batchIncRatio
	}

	adjuster.curBatchSize = cos.MinF64(adjuster.curBatchSize, maxBatchSize)
	if adjuster.limit < 1 {
		adjuster.limit = 1
	} else if adjuster.limit > adjuster.maxLimit {
		adjuster.limit = adjuster.maxLimit
	}
	return prevLimit, adjuster.limit
}
