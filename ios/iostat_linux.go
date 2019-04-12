// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"sync"

	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

// IostatContext tracks the iostats for a single mountpath
//   this includes multiple disks if raid is used

const (
	ioStatIntervalStale = time.Second * 20 // how long without a request before stopping logging
)

type (
	IostatContext struct {
		disks            cmn.StringSet
		lastIostatAccess *atomic.Int64
		cache            atomic.Pointer
		updateLock       *sync.Cond
		updating         bool
	}
	ioStatCache struct {
		expireTime     time.Time
		timestamp      time.Time
		cachedDiskIOms map[string]int64
		cachedDiskUtil int64
	}
)

func NewIostatContext(fs string) *IostatContext {
	newIostatCtx := &IostatContext{
		lastIostatAccess: atomic.NewInt64(0),
		updateLock:       sync.NewCond(&sync.Mutex{}),
	}

	newIostatCtx.putCache(&ioStatCache{
		cachedDiskIOms: make(map[string]int64),
	})

	// find the disks corresponding to fs
	disks := Fs2disks(fs)
	if len(disks) == 0 {
		glog.Errorf("filesystem (%+v) - no disks?", fs)
	}
	newIostatCtx.disks = disks

	return newIostatCtx
}

//
// public methods
//

func (ctx *IostatContext) GetDiskUtil(timestamp ...time.Time) int64 {
	var timestampVar time.Time
	if len(timestamp) > 0 {
		timestampVar = timestamp[0]
	} else {
		timestampVar = time.Now()
	}

	ctx.lastIostatAccess.Store(timestampVar.UnixNano())
	cache := ctx.fetchIostatCache(timestampVar)
	return cache.cachedDiskUtil
}

// ToString is used for logging and should not be in datapath
func (ctx *IostatContext) ToString() string {
	lastAccess := ctx.lastIostatAccess.Load()
	timestamp := time.Now()
	elapsed := timestamp.UnixNano() - lastAccess
	if elapsed < ioStatIntervalStale.Nanoseconds() {
		return ""
	}

	cache := ctx.fetchIostatCache(timestamp)
	return fmt.Sprintf("util=%v", cache.cachedDiskUtil)
}

//
// private methods
//

// helper function for checking if it's time to update Iostat
//  assumes that the timestamp passed in is close enough to the current time
func (ctx *IostatContext) fetchIostatCache(timestamp time.Time) *ioStatCache {
	cache := ctx.getCache()
	if cache.expireTime.After(timestamp) {
		return cache
	}

	ctx.updateLock.L.Lock()
	if ctx.updating {
		ctx.updateLock.Wait()
		ctx.updateLock.L.Unlock()
		return ctx.getCache()
	}
	ctx.updating = true
	ctx.updateLock.L.Unlock()

	newCache := &ioStatCache{timestamp: timestamp, cachedDiskIOms: make(map[string]int64, len(cache.cachedDiskIOms))}

	fetchedDiskStats := GetDiskStats(ctx.disks)
	msElapsed := timestamp.Sub(cache.timestamp).Nanoseconds() / time.Millisecond.Nanoseconds()

	var totalPctUtil, numPctUtil int64

	missingInfo := false
	for disk := range ctx.disks {
		stat, ok := fetchedDiskStats[disk]
		if !ok {
			continue
		}

		newCache.cachedDiskIOms[disk] = stat.IOMs

		if _, ok := cache.cachedDiskIOms[disk]; !ok {
			missingInfo = true
			continue
		}
		timeSpentDoingIO := stat.IOMs - cache.cachedDiskIOms[disk]

		pctUtil := timeSpentDoingIO * 100 / msElapsed
		pctUtil = cmn.MinI64(pctUtil, 100)
		totalPctUtil += pctUtil
		numPctUtil++
	}

	if numPctUtil > 0 {
		totalPctUtil /= numPctUtil
	}
	newCache.cachedDiskUtil = totalPctUtil

	config := cmn.GCO.Get()
	var nextInterval time.Duration
	if missingInfo {
		nextInterval = config.Disk.IostatTimeShort
	} else {
		highWM := cmn.MinI64(config.Disk.DiskUtilHighWM, 100)
		lowWM := cmn.MaxI64(config.Disk.DiskUtilLowWM, 1)
		minNext := config.Disk.IostatTimeShort
		diffNext := config.Disk.IostatTimeLong - config.Disk.IostatTimeShort

		utilRatio := cmn.RatioPct(highWM, lowWM, cache.cachedDiskUtil)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		nextInterval = minNext + time.Duration(int64(diffNext)*(100-utilRatio)/100)
	}
	newCache.expireTime = timestamp.Add(nextInterval)

	ctx.putCache(newCache)

	ctx.updateLock.L.Lock()
	ctx.updating = false
	ctx.updateLock.L.Unlock()
	ctx.updateLock.Broadcast()

	return newCache
}

func (ctx *IostatContext) getCache() *ioStatCache {
	cache := (*ioStatCache)(ctx.cache.Load())
	return cache
}

func (ctx *IostatContext) putCache(cache *ioStatCache) {
	ctx.cache.Store(unsafe.Pointer(cache))
}
