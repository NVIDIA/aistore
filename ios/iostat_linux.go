// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"math"
	"strings"
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
	ioStatIntervalStale = time.Second * 20 // how long without a request before stopping logging, should be greater than config.Disk.IostatTimeLong

	// FIXME: optimize and make configurable
	writesPerRead = 2 // the weight of the write queue compared to read queue
	doubleThresh  = 2 // mountpath gets double the requests if the other is more than this many times busy
)

type (
	FQNResolver interface {
		FQN2mountpath(fqn string) (mpath string)
	}
	IostatContext struct {
		fqnr        FQNResolver
		mpathLock   sync.Mutex
		mpath2disks map[string]cmn.StringSet
		disk2mpath  cmn.SimpleKVs
		disks       cmn.StringSet
		cache       atomic.Pointer
		updateLock  *sync.Cond
		updating    bool
	}
	ioStatCache struct {
		expireTime time.Time
		timestamp  time.Time

		diskIOms    map[string]int64
		diskReadMs  map[string]int64
		diskWriteMs map[string]int64

		mpathPctUtil   map[string]int64         // average utilization of the disks, max 100
		mpathQueueSize map[string]int64         // average queue size of the disks, writes have extra weight
		mpathRRCounter map[string]*atomic.Int64 // round robin counter
	}
)

func NewIostatContext(fqnr FQNResolver) *IostatContext {
	newIostatCtx := &IostatContext{
		mpath2disks: make(map[string]cmn.StringSet),
		disk2mpath:  make(cmn.SimpleKVs),

		updateLock: sync.NewCond(&sync.Mutex{}),
	}
	newIostatCtx.fqnr = fqnr
	newIostatCtx.putCache(&ioStatCache{
		diskIOms:    make(map[string]int64),
		diskReadMs:  make(map[string]int64),
		diskWriteMs: make(map[string]int64),

		mpathPctUtil:   make(map[string]int64),
		mpathQueueSize: make(map[string]int64),
		mpathRRCounter: make(map[string]*atomic.Int64),
	})

	return newIostatCtx
}

//
// public methods
//

func (ctx *IostatContext) AddMpath(mpath, fs string) {
	ctx.RemoveMpath(mpath) // called to cleanup old

	ctx.mpathLock.Lock()
	defer ctx.mpathLock.Unlock()

	disks := Fs2disks(fs)
	if len(disks) == 0 {
		glog.Errorf("filesystem (%+v) - no disks?", fs)
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
				delete(ctx.disks, disk)
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
	cmn.Assert(len(copyFQN) > 0) // called only if there's at least 1 copy

	fqn = defaultFQN
	mpath := ctx.fqnr.FQN2mountpath(defaultFQN)
	var (
		fqnRRCount   int64
		fqnQueueSize int64

		rrCountWrap *atomic.Int64
		ok          bool
	)

	cache := ctx.refreshIostatCache(time.Now())
	if rrCountWrap, ok = cache.mpathRRCounter[mpath]; !ok {
		fqnRRCount = int64(math.MaxInt64)
	} else {
		fqnRRCount = rrCountWrap.Load()
	}
	fqnQueueSize = cache.mpathQueueSize[mpath]

	for _, newFQN := range copyFQN {
		newMpath := ctx.fqnr.FQN2mountpath(newFQN)
		var newQueueSize int64

		if newQueueSize, ok = cache.mpathQueueSize[newMpath]; !ok {
			continue
		}
		if rrCountWrap, ok = cache.mpathRRCounter[newMpath]; !ok {
			continue
		}
		newRRCount := rrCountWrap.Load()

		// determine if newMpath is the next item in round robin
		//  newRRCount has half the weight if 'significantly busy'
		//  this, as of v2.1, means that newRRCount needs to be half of fqnRRCount to be chosen
		//  if newMpath is doubleThresh times as busy as mpath
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

	if counter, ok := cache.mpathRRCounter[mpath]; ok {
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
	return cache.mpathPctUtil[mpath]
}

// ToString is used for logging and should not be in datapath
func (ctx *IostatContext) ToString() string {
	cache := ctx.getCache()
	timestamp := time.Now()
	if cache.expireTime.Add(ioStatIntervalStale).Before(timestamp) {
		return ""
	}

	lines := make([]string, 0)

	for mpath, util := range cache.mpathPctUtil {
		line := fmt.Sprintf("%s util=%v", mpath, util)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

//
// private methods
//

// helper function for fetching and updating the Iostat cache
//  assumes that the timestamp passed in is close enough to the current time
func (ctx *IostatContext) refreshIostatCache(timestamp time.Time) *ioStatCache {
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

	// ensure update has the most recent everything
	cache = ctx.getCache()
	if cache.expireTime.After(timestamp) {
		ctx.updateLock.L.Lock()
		ctx.updating = false
		ctx.updateLock.L.Unlock()
		ctx.updateLock.Broadcast()
		return cache
	}
	timestamp = time.Now()
	fetchedDiskStats := GetDiskStats(ctx.disk2mpath)

	ctx.mpathLock.Lock()
	numDisks := len(ctx.disk2mpath)
	numPaths := len(ctx.mpath2disks)
	newCache := &ioStatCache{
		timestamp:   timestamp,
		diskIOms:    make(map[string]int64, numDisks),
		diskReadMs:  make(map[string]int64, numDisks),
		diskWriteMs: make(map[string]int64, numDisks),

		mpathPctUtil:   make(map[string]int64, numPaths),
		mpathQueueSize: make(map[string]int64, numPaths),
		mpathRRCounter: make(map[string]*atomic.Int64, numPaths),
	}

	msElapsed := int64(timestamp.Sub(cache.timestamp) / time.Millisecond)
	missingInfo := false
	for disk, mpath := range ctx.disk2mpath {
		stat, ok := fetchedDiskStats[disk]
		if !ok {
			continue
		}

		newCache.diskIOms[disk] = stat.IOMs
		newCache.diskReadMs[disk] = stat.ReadMs
		newCache.diskWriteMs[disk] = stat.WriteMs
		if _, ok := cache.diskIOms[disk]; !ok {
			missingInfo = true
			continue
		}

		timeSpentDoingIO := stat.IOMs - cache.diskIOms[disk]

		pctUtil := timeSpentDoingIO * 100 / msElapsed
		pctUtil = cmn.MinI64(pctUtil, 100)
		newCache.mpathPctUtil[mpath] += pctUtil

		// from https://www.kernel.org/doc/Documentation/block/stat.txt
		/* read ticks, write ticks, discard ticks
		======================================
		These values count the number of milliseconds that I/O requests have
		waited on this block device.  If there are multiple I/O requests waiting,
		these values will increase at a rate greater than 1000/second; for
		example, if 60 read requests wait for an average of 30 ms, the read_ticks
		field will increase by 60*30 = 1800. */
		// the read/write ticks are divided by time elapsed to determine queue size
		// note that stat.ReadMs and stat.WriteMs cumulative and continually growing,
		// which is why we keep the previous values in the iostat cache.
		readQueueSize := (stat.ReadMs - cache.diskReadMs[disk]) / msElapsed
		writeQueueSize := (stat.WriteMs - cache.diskWriteMs[disk]) * writesPerRead / msElapsed

		newCache.mpathQueueSize[mpath] += readQueueSize + writeQueueSize
	}

	var maxUtil int64 // use the maximum utilization to calculate the expire time
	for mpath, disks := range ctx.mpath2disks {
		numDisk := int64(len(disks))
		if numDisk == 0 {
			continue
		}

		pctUtil := newCache.mpathPctUtil[mpath] / numDisk
		newCache.mpathPctUtil[mpath] = pctUtil
		maxUtil = cmn.MaxI64(maxUtil, pctUtil)

		newCache.mpathQueueSize[mpath] /= numDisk
		newCache.mpathRRCounter[mpath] = atomic.NewInt64(0)
	}
	ctx.mpathLock.Unlock()

	config := cmn.GCO.Get()
	var expireTime time.Duration
	if missingInfo {
		expireTime = config.Disk.IostatTimeShort
	} else {
		lowWM := cmn.MaxI64(config.Disk.DiskUtilLowWM, 1)
		highWM := cmn.MinI64(config.Disk.DiskUtilHighWM, 100)

		minNext := config.Disk.IostatTimeShort
		diffNext := config.Disk.IostatTimeLong - config.Disk.IostatTimeShort

		utilRatio := cmn.RatioPct(highWM, lowWM, maxUtil)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		expireTime = minNext + time.Duration(int64(diffNext)*(100-utilRatio)/100)
	}
	newCache.expireTime = timestamp.Add(expireTime)

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
