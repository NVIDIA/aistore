// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
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

// The "sectors" in question are the standard UNIX 512-byte sectors, not any device- or filesystem-specific block size
// (from https://www.kernel.org/doc/Documentation/block/stat.txt)
const sectorSize = int64(512)

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
		sorted      []string
		blockStats  diskBlockStats
		disk2sysfn  cmn.SimpleKVs
		cache       atomic.Pointer
		cacheLock   *sync.Mutex
		cacheHst    [16]*ioStatCache // history TODO
		cacheIdx    int
	}
	SelectedDiskStats struct {
		RBps, WBps, Util int64
	}
	ioStatCache struct {
		expireTime time.Time
		timestamp  time.Time

		diskIOms map[string]int64
		diskUtil map[string]int64
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
		mpath2disks: make(map[string]fsDisks, 10),
		disk2mpath:  make(cmn.SimpleKVs, 10),
		sorted:      make([]string, 0, 10),
		blockStats:  make(diskBlockStats, 10),
		disk2sysfn:  make(cmn.SimpleKVs, 10),
		cacheLock:   &sync.Mutex{},
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
		diskUtil:  make(map[string]int64),
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
	ctx.mpathLock.Lock()
	defer ctx.mpathLock.Unlock()

	config := cmn.GCO.Get()
	fsdisks := fs2disks(fs)
	if len(fsdisks) == 0 {
		return
	}
	if dd, ok := ctx.mpath2disks[mpath]; ok {
		s := fmt.Sprintf("mountpath %s already added, disks %+v %+v", mpath, dd, fsdisks)
		cmn.AssertMsg(false, s)
	}
	ctx.mpath2disks[mpath] = fsdisks
	for disk := range fsdisks {
		if mp, ok := ctx.disk2mpath[disk]; ok && !config.TestingEnv() {
			s := fmt.Sprintf("disk sharing is not permitted: mp %s, add fs %s mp %s, disk %s", mp, fs, mpath, disk)
			cmn.AssertMsg(false, s)
			return
		}
		ctx.disk2mpath[disk] = mpath
	}
	ctx.sorted = ctx.sorted[:0]
	for disk := range ctx.disk2mpath {
		ctx.sorted = append(ctx.sorted, disk)
		if _, ok := ctx.disk2sysfn[disk]; !ok {
			ctx.disk2sysfn[disk] = fmt.Sprintf("/sys/class/block/%v/stat", disk)
		}
	}
	sort.Strings(ctx.sorted) // log
	if len(ctx.disk2sysfn) != len(ctx.disk2mpath) {
		for disk := range ctx.disk2sysfn {
			if _, ok := ctx.disk2mpath[disk]; !ok {
				delete(ctx.disk2sysfn, disk)
			}
		}
	}
}

func (ctx *IostatContext) RemoveMpath(mpath string) {
	ctx.mpathLock.Lock()
	defer ctx.mpathLock.Unlock()

	config := cmn.GCO.Get()
	oldDisks, ok := ctx.mpath2disks[mpath]
	if !ok {
		glog.Warningf("mountpath %s already removed", mpath)
		return
	}
	for disk := range oldDisks {
		if mp, ok := ctx.disk2mpath[disk]; ok {
			if mp != mpath && !config.TestingEnv() {
				s := fmt.Sprintf("(mpath %s => disk %s => mpath %s) violation", mp, disk, mpath)
				cmn.AssertMsg(false, s)
			}
			delete(ctx.disk2mpath, disk)
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
	cache := ctx.refreshIostatCache()
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

func (ctx *IostatContext) GetDiskUtil(mpath string, now time.Time) int64 {
	cache := ctx.refreshIostatCache(now)
	return cache.mpathUtil[mpath]
}

func (ctx *IostatContext) GetSelectedDiskStats() (m map[string]*SelectedDiskStats) {
	var cache = ctx.refreshIostatCache()
	m = make(map[string]*SelectedDiskStats)
	for disk := range cache.diskIOms {
		m[disk] = &SelectedDiskStats{
			RBps: cache.diskRBps[disk],
			WBps: cache.diskWBps[disk],
			Util: cache.diskUtil[disk]}
	}
	return
}

func (ctx *IostatContext) LogAppend(lines []string) []string {
	var cache = ctx.refreshIostatCache()
	for _, disk := range ctx.sorted {
		if _, ok := cache.diskIOms[disk]; !ok {
			continue
		}
		util := cache.diskUtil[disk]
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
func (ctx *IostatContext) refreshIostatCache(nows ...time.Time) *ioStatCache {
	var now time.Time
	if len(nows) > 0 {
		now = nows[0]
	} else {
		now = time.Now()
	}
	statsCache := ctx.getStatsCache()
	if statsCache.expireTime.After(now) {
		return statsCache
	}

	ctx.cacheLock.Lock()
	statsCache = ctx.getStatsCache()
	if statsCache.expireTime.After(now) {
		ctx.cacheLock.Unlock()
		return statsCache
	}
	//
	// begin
	//
	ctx.mpathLock.Lock() // nested lock
	now = time.Now()
	readDiskStats(ctx.disk2mpath, ctx.disk2sysfn, ctx.blockStats)

	ctx.cacheIdx++
	ctx.cacheIdx %= len(ctx.cacheHst)
	var (
		ncache         = ctx.cacheHst[ctx.cacheIdx]
		elapsed        = int64(now.Sub(statsCache.timestamp))
		elapsedSeconds = cmn.DivRound(elapsed, second)
		elapsedMillis  = cmn.DivRound(elapsed, millis)
	)
	ncache.timestamp = now
	for mpath := range ctx.mpath2disks {
		if rr, ok := ncache.mpathRR[mpath]; ok {
			rr.Store(0)
		} else {
			ncache.mpathRR[mpath] = atomic.NewInt64(0)
		}
		ncache.mpathUtil[mpath] = 0
		ncache.mpathQue[mpath] = 0
	}
	for disk := range ncache.diskIOms {
		if _, ok := ctx.disk2mpath[disk]; !ok {
			ncache = newIostatCache()
			ctx.cacheHst[ctx.cacheIdx] = ncache
		}
	}
	missingInfo := false
	for disk, mpath := range ctx.disk2mpath {
		ncache.diskRBps[disk] = 0
		ncache.diskWBps[disk] = 0
		ncache.diskUtil[disk] = 0
		stat, ok := ctx.blockStats[disk]
		if !ok {
			glog.Errorf("no block stats for disk %s", disk) // TODO: remove
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
		// deltas
		var (
			ioms   = stat.IOMs - statsCache.diskIOms[disk]
			rqms   = stat.ReadMs - statsCache.diskRms[disk]
			wqms   = stat.WriteMs - statsCache.diskWms[disk]
			rdsect = stat.ReadSectors - statsCache.diskRSec[disk]
			wdsect = stat.WriteSectors - statsCache.diskWSec[disk]
		)
		if elapsedMillis > 0 {
			ncache.diskUtil[disk] = cmn.DivRound(ioms*100, elapsedMillis)
		} else {
			ncache.diskUtil[disk] = statsCache.diskUtil[disk]
		}
		ncache.mpathUtil[mpath] += ncache.diskUtil[disk]
		if elapsedSeconds > 0 {
			ncache.diskRBps[disk] = cmn.DivRound(rdsect*sectorSize, elapsedSeconds)
			ncache.diskWBps[disk] = cmn.DivRound(wdsect*sectorSize, elapsedSeconds)
		} else {
			ncache.diskRBps[disk] = statsCache.diskRBps[disk]
			ncache.diskWBps[disk] = statsCache.diskWBps[disk]
		}
		// These values count the number of milliseconds that I/O requests have
		// waited on this block device.  If there are multiple I/O requests waiting,
		// these values will increase at a rate greater than 1000/second; for
		// example, if 60 read requests wait for an average of 30 ms, the read_ticks
		// field will increase by 60*30 = 1800
		// (from https://www.kernel.org/doc/Documentation/block/stat.txt)
		if elapsedMillis > 0 {
			ncache.mpathQue[mpath] += cmn.DivRound(rqms, elapsedMillis) + cmn.DivRound(wqms*writesPerRead, elapsedMillis)
		} else {
			ncache.mpathQue[mpath] += statsCache.mpathQue[mpath]
		}
	}

	// average and max
	var maxUtil int64
	for mpath, disks := range ctx.mpath2disks {
		numDisk := int64(len(disks))
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
	} else { // use the maximum utilization to determine expiration time
		var (
			lowWM     = cmn.MaxI64(config.Disk.DiskUtilLowWM, 1)
			highWM    = cmn.MinI64(config.Disk.DiskUtilHighWM, 100)
			delta     = int64(config.Disk.IostatTimeLong - config.Disk.IostatTimeShort)
			utilRatio = cmn.RatioPct(highWM, lowWM, maxUtil)
		)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		expireTime = config.Disk.IostatTimeShort + time.Duration(delta*(100-utilRatio)/100)
	}
	ncache.expireTime = now.Add(expireTime)
	ctx.putStatsCache(ncache)
	ctx.cacheLock.Unlock()

	return ncache
}

func (ctx *IostatContext) getStatsCache() *ioStatCache {
	cache := (*ioStatCache)(ctx.cache.Load())
	return cache
}

func (ctx *IostatContext) putStatsCache(cache *ioStatCache) {
	ctx.cache.Store(unsafe.Pointer(cache))
}
