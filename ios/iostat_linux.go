// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"math"
	"sync"

	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	writesPerRead = 2 // [TODO] the weight of the write queue compared to read queue
	doubleThresh  = 2 // [TODO] mountpath gets double the requests if the other is more than this many times busy
)
const (
	millis = int64(time.Millisecond)
	second = int64(time.Second)
)

type (
	FQNResolver interface {
		FQN2mountpath(fqn string) (mpath string)
	}
	// IostatContext tracks the iostats for a single mountpath (that may include multiple disks)
	IostatContext struct {
		fqnr        FQNResolver
		mpathLock   sync.Mutex
		mpath2disks map[string]fsDisks
		disk2mpath  cmn.SimpleKVs
		cache       atomic.Pointer
		updateLock  *sync.Cond
		cacheHst    [16]*ioStatCache // history TODO
		cacheIdx    int
		updating    bool
	}
	ioStatCache struct {
		expireTime time.Time
		timestamp  time.Time

		diskIOms map[string]int64
		diskRms  map[string]int64
		diskRSec map[string]int64
		diskRBps map[string]int64
		diskWms  map[string]int64
		diskWSec map[string]int64
		diskWBps map[string]int64

		mpathUtil map[string]int64         // average utilization of the disks, max 100
		mpathQue  map[string]int64         // average queue size of the disks, writes have extra weight
		mpathRR   map[string]*atomic.Int64 // round robin counter
	}
)

func NewIostatContext(fqnr FQNResolver) (ctx *IostatContext) {
	ctx = &IostatContext{
		fqnr:        fqnr,
		mpath2disks: make(map[string]fsDisks),
		disk2mpath:  make(cmn.SimpleKVs),
		updateLock:  sync.NewCond(&sync.Mutex{}),
	}
	for i := 0; i < len(ctx.cacheHst); i++ {
		ctx.cacheHst[i] = newIostatCache()
	}
	ctx.putStatsCache(ctx.cacheHst[0])
	ctx.cacheIdx = 0
	return
}

func newIostatCache() *ioStatCache {
	return &ioStatCache{
		diskIOms:  make(map[string]int64),
		diskRms:   make(map[string]int64),
		diskRSec:  make(map[string]int64),
		diskRBps:  make(map[string]int64),
		diskWms:   make(map[string]int64),
		diskWSec:  make(map[string]int64),
		diskWBps:  make(map[string]int64),
		mpathUtil: make(map[string]int64),
		mpathQue:  make(map[string]int64),
		mpathRR:   make(map[string]*atomic.Int64),
	}
}

//
// public methods
//

func (ctx *IostatContext) AddMpath(mpath, fs string) {
	ctx.RemoveMpath(mpath) // called to cleanup old

	ctx.mpathLock.Lock()
	defer ctx.mpathLock.Unlock()

	disks := fs2disks(fs)
	if len(disks) == 0 {
		return
	}
	ctx.mpath2disks[mpath] = disks
	for disk := range disks {
		ctx.disk2mpath[disk] = mpath
	}
}

func (ctx *IostatContext) RemoveMpath(mpath string) {
	ctx.mpathLock.Lock()
	defer ctx.mpathLock.Unlock()
	if oldDisks, ok := ctx.mpath2disks[mpath]; ok {
		for disk := range oldDisks {
			if mappedMpath, ok := ctx.disk2mpath[disk]; ok && mappedMpath == mpath {
				delete(ctx.disk2mpath, disk)
			}
		}
	}
	delete(ctx.mpath2disks, mpath)
}

// GetRoundRobin provides load balancing on defaultFQN and copyFQN
//   load balancing works by round robining the mountpoints, breaking all ties by selecting the least busy mountpath
//   mountpoints that are 'significantly busy' have half weight in round robin
//   the round robin is approximate because it doesn't lock since it's in datapath
func (ctx *IostatContext) GetRoundRobin(defaultFQN string, copyFQN []string) (fqn string) {
	cmn.Assert(len(copyFQN) > 0)
	fqn = defaultFQN
	mpath := ctx.fqnr.FQN2mountpath(defaultFQN)
	var (
		fqnRRCount   int64
		fqnQueueSize int64
		rrCountWrap  *atomic.Int64
		ok           bool
	)
	cache := ctx.refreshIostatCache(time.Now())
	if rrCountWrap, ok = cache.mpathRR[mpath]; !ok {
		fqnRRCount = int64(math.MaxInt64)
	} else {
		fqnRRCount = rrCountWrap.Load()
	}
	fqnQueueSize = cache.mpathQue[mpath]

	for _, newFQN := range copyFQN {
		newMpath := ctx.fqnr.FQN2mountpath(newFQN)
		var newQueueSize int64

		if newQueueSize, ok = cache.mpathQue[newMpath]; !ok {
			continue
		}
		if rrCountWrap, ok = cache.mpathRR[newMpath]; !ok {
			continue
		}
		newRRCount := rrCountWrap.Load()

		// determine if newMpath is the next item in round robin
		// newRRCount has half the weight if 'significantly busy'
		// this, as of v2.1, means that newRRCount needs to be half of fqnRRCount to be chosen
		// if newMpath is doubleThresh times as busy as mpath
		if newRRCount*2 < fqnRRCount ||
			newRRCount < fqnRRCount && !(fqnQueueSize*doubleThresh < newQueueSize) ||
			newRRCount == fqnRRCount && newQueueSize < fqnQueueSize ||
			newRRCount < fqnRRCount*2 && newQueueSize*doubleThresh < fqnQueueSize {
			fqn = newFQN
			mpath = newMpath
			fqnRRCount = newRRCount
			fqnQueueSize = newQueueSize
		}
	}

	if counter, ok := cache.mpathRR[mpath]; ok {
		counter.Inc()
	}
	if fqn == "" {
		return defaultFQN
	}
	return
}

func (ctx *IostatContext) GetDiskUtil(mpath string, timestamp ...time.Time) int64 {
	var timestampVar time.Time
	if len(timestamp) > 0 {
		timestampVar = timestamp[0]
	} else {
		timestampVar = time.Now()
	}
	cache := ctx.refreshIostatCache(timestampVar)
	return cache.mpathUtil[mpath]
}

func (ctx *IostatContext) LogAppend(lines []string, timestamp time.Time) []string {
	var cache = ctx.refreshIostatCache(timestamp)
	for disk := range cache.diskIOms {
		mpath := ctx.disk2mpath[disk]
		util := cache.mpathUtil[mpath]
		if util == 0 {
			continue
		}
		rbps := cmn.B2S(cache.diskRBps[disk], 0)
		wbps := cmn.B2S(cache.diskWBps[disk], 0)
		line := fmt.Sprintf("%s: %s/s, %s/s, %d%%", disk, rbps, wbps, util)
		lines = append(lines, line)
	}
	return lines
}

//
// private methods
//

// helper function for fetching and updating the Iostat cache
//  assumes that the timestamp passed in is close enough to the current time
func (ctx *IostatContext) refreshIostatCache(timestamp time.Time) *ioStatCache {
	statsCache := ctx.getStatsCache()
	if statsCache.expireTime.After(timestamp) {
		return statsCache
	}

	ctx.updateLock.L.Lock()
	if ctx.updating {
		ctx.updateLock.Wait()
		ctx.updateLock.L.Unlock()
		return ctx.getStatsCache()
	}
	ctx.updating = true
	ctx.updateLock.L.Unlock()

	// ensure update has the most recent everything
	statsCache = ctx.getStatsCache()
	if statsCache.expireTime.After(timestamp) {
		ctx.updateLock.L.Lock()
		ctx.updating = false
		ctx.updateLock.L.Unlock()
		ctx.updateLock.Broadcast()
		return statsCache
	}
	fetchedDiskStats := GetDiskStats(ctx.disk2mpath)

	ctx.mpathLock.Lock()
	ctx.cacheIdx++
	ctx.cacheIdx %= len(ctx.cacheHst)
	var (
		ncache  = ctx.cacheHst[ctx.cacheIdx]
		elapsed = int64(timestamp.Sub(statsCache.timestamp))
	)
	ncache.timestamp = timestamp
	for mpath := range ctx.mpath2disks {
		if rr, ok := ncache.mpathRR[mpath]; ok {
			rr.Store(0)
		} else {
			ncache.mpathRR[mpath] = atomic.NewInt64(0)
		}
		ncache.mpathUtil[mpath] = 0
		ncache.mpathQue[mpath] = 0
	}

	missingInfo := false
	for disk, mpath := range ctx.disk2mpath {
		stat, ok := fetchedDiskStats[disk]
		if !ok {
			continue
		}
		ncache.diskIOms[disk] = stat.IOMs
		ncache.diskRms[disk] = stat.ReadMs
		ncache.diskRSec[disk] = stat.ReadSectors
		ncache.diskWms[disk] = stat.WriteMs
		ncache.diskWSec[disk] = stat.WriteSectors
		if _, ok := statsCache.diskIOms[disk]; !ok {
			missingInfo = true
			continue
		}
		var (
			util = (stat.IOMs - statsCache.diskIOms[disk]) * 100 * millis / elapsed
			rque = (stat.ReadMs - statsCache.diskRms[disk]) * millis / elapsed
			wque = (stat.WriteMs - statsCache.diskWms[disk]) * millis * writesPerRead / elapsed
		)
		ncache.mpathUtil[mpath] += util
		disks := ctx.mpath2disks[mpath]
		if sectorSize, ok := disks[disk]; ok {
			ncache.diskRBps[disk] = (ncache.diskRSec[disk] - statsCache.diskRSec[disk]) * sectorSize * second / elapsed
			ncache.diskWBps[disk] = (ncache.diskWSec[disk] - statsCache.diskWSec[disk]) * sectorSize * second / elapsed
		}
		// from https://www.kernel.org/doc/Documentation/block/stat.txt
		// These values count the number of milliseconds that I/O requests have
		// waited on this block device.  If there are multiple I/O requests waiting,
		// these values will increase at a rate greater than 1000/second; for
		// example, if 60 read requests wait for an average of 30 ms, the read_ticks
		// field will increase by 60*30 = 1800
		ncache.mpathQue[mpath] += rque + wque
	}

	// average and max
	var maxUtil int64
	for mpath, disks := range ctx.mpath2disks {
		numDisk := int64(len(disks))
		if numDisk == 0 {
			continue
		}
		ncache.mpathUtil[mpath] /= numDisk
		maxUtil = cmn.MaxI64(maxUtil, ncache.mpathUtil[mpath])
		ncache.mpathQue[mpath] /= numDisk
		ncache.mpathRR[mpath] = atomic.NewInt64(0)
	}
	ctx.mpathLock.Unlock()

	config := cmn.GCO.Get()
	var expireTime time.Duration
	if missingInfo {
		expireTime = config.Disk.IostatTimeShort
	} else { // use the maximum utilization to determine next refresh time
		var (
			lowWM     = cmn.MaxI64(config.Disk.DiskUtilLowWM, 1)
			highWM    = cmn.MinI64(config.Disk.DiskUtilHighWM, 100)
			delta     = int64(config.Disk.IostatTimeLong - config.Disk.IostatTimeShort)
			utilRatio = cmn.RatioPct(highWM, lowWM, maxUtil)
		)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		expireTime = config.Disk.IostatTimeShort + time.Duration(delta*(100-utilRatio)/100)
	}
	ncache.expireTime = timestamp.Add(expireTime)
	ctx.putStatsCache(ncache)

	ctx.updateLock.L.Lock()
	ctx.updating = false
	ctx.updateLock.L.Unlock()
	ctx.updateLock.Broadcast()

	return ncache
}

func (ctx *IostatContext) getStatsCache() *ioStatCache {
	cache := (*ioStatCache)(ctx.cache.Load())
	return cache
}

func (ctx *IostatContext) putStatsCache(cache *ioStatCache) {
	ctx.cache.Store(unsafe.Pointer(cache))
}
