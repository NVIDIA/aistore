// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
)

const (
	millis = int64(time.Millisecond)
	second = int64(time.Second)
)

type (
	IostatContext struct {
		mpathLock   sync.Mutex
		mpath2disks map[string]fsDisks
		disk2mpath  cmn.SimpleKVs
		sorted      []string
		disk2sysfn  cmn.SimpleKVs
		cache       atomic.Pointer
		cacheLock   *sync.Mutex
		cacheHst    [16]*ioStatCache
		cacheIdx    int
	}
	SelectedDiskStats struct {
		RBps, WBps, Util int64
	}
	ioStatCache struct {
		expireTime int64
		timestamp  int64

		diskIOms   map[string]int64
		diskUtil   map[string]int64
		diskRms    map[string]int64
		diskRBytes map[string]int64
		diskRBps   map[string]int64
		diskWms    map[string]int64
		diskWBytes map[string]int64
		diskWBps   map[string]int64

		mpathUtil map[string]int64 // average utilization of the disks, max 100
		mpathRR   map[string]*atomic.Int32
	}

	IOStater interface {
		GetMpathUtil(mpath string, nowTs int64) int64
		GetAllMpathUtils(nowTs int64) (map[string]int64, map[string]*atomic.Int32)
		AddMpath(mpath string, fs string)
		RemoveMpath(mpath string)
		LogAppend(log []string) []string
		GetSelectedDiskStats() (m map[string]*SelectedDiskStats)
	}
)

func NewIostatContext() (ctx *IostatContext) {
	ctx = &IostatContext{
		mpath2disks: make(map[string]fsDisks, 10),
		disk2mpath:  make(cmn.SimpleKVs, 10),
		sorted:      make([]string, 0, 10),
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
		diskIOms:   make(map[string]int64),
		diskUtil:   make(map[string]int64),
		diskRms:    make(map[string]int64),
		diskRBytes: make(map[string]int64),
		diskRBps:   make(map[string]int64),
		diskWms:    make(map[string]int64),
		diskWBytes: make(map[string]int64),
		diskWBps:   make(map[string]int64),
		mpathUtil:  make(map[string]int64),
		mpathRR:    make(map[string]*atomic.Int32),
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
		glog.Errorf("no disks associated with (mountpath: %q, fs: %q)", mpath, fs)
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

// For TestingEnv to avoid a case when removing a mountpath also empties
// `disk` list, so a target stops generating disk stats.
// If another mountpath containing the same disk is found, the disk2mpath map
// is updated. Otherwise, the function removes disk from the disk2map map.
func (ctx *IostatContext) removeMpathDiskTesting(mpath, disk string) {
	if _, ok := ctx.disk2mpath[disk]; !ok {
		return
	}
	for path, disks := range ctx.mpath2disks {
		if path == mpath {
			continue
		}
		for dsk := range disks {
			if dsk == disk {
				ctx.disk2mpath[disk] = path
				return
			}
		}
	}
	delete(ctx.mpath2disks, disk)
}

func (ctx *IostatContext) removeMpathDisk(mpath, disk string) {
	mp, ok := ctx.disk2mpath[disk]
	if !ok {
		return
	}
	if mp != mpath {
		cmn.Assertf(false, "(mpath %s => disk %s => mpath %s) violation", mp, disk, mpath)
	}
	delete(ctx.disk2mpath, disk)
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
		if config.TestingEnv() {
			ctx.removeMpathDiskTesting(mpath, disk)
		} else {
			ctx.removeMpathDisk(mpath, disk)
		}
	}
	delete(ctx.mpath2disks, mpath)
}

func (ctx *IostatContext) GetMpathUtil(mpath string, nowTs int64) int64 {
	cache := ctx.refreshIostatCache(nowTs)
	return cache.mpathUtil[mpath]
}
func (ctx *IostatContext) GetAllMpathUtils(nowTs int64) (map[string]int64, map[string]*atomic.Int32) {
	cache := ctx.refreshIostatCache(nowTs)
	return cache.mpathUtil, cache.mpathRR
}

func (ctx *IostatContext) GetSelectedDiskStats() (m map[string]*SelectedDiskStats) {
	var cache = ctx.refreshIostatCache()
	m = make(map[string]*SelectedDiskStats)
	for disk := range cache.diskIOms {
		m[disk] = &SelectedDiskStats{
			RBps: cache.diskRBps[disk],
			WBps: cache.diskWBps[disk],
			Util: cache.diskUtil[disk],
		}
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
func (ctx *IostatContext) refreshIostatCache(nts ...int64) *ioStatCache {
	var nowTs int64
	if len(nts) > 0 {
		nowTs = nts[0]
	} else {
		nowTs = mono.NanoTime()
	}
	statsCache := ctx.getStatsCache()
	if statsCache.expireTime > nowTs {
		return statsCache
	}

	ctx.cacheLock.Lock()
	statsCache = ctx.getStatsCache()
	if statsCache.expireTime > nowTs {
		ctx.cacheLock.Unlock()
		return statsCache
	}
	//
	// begin
	//
	ctx.mpathLock.Lock() // nested lock
	nowTs = mono.NanoTime()
	disksStats := readDiskStats(ctx.disk2mpath, ctx.disk2sysfn)

	ctx.cacheIdx++
	ctx.cacheIdx %= len(ctx.cacheHst)
	var (
		ncache         = ctx.cacheHst[ctx.cacheIdx]
		elapsed        = nowTs - statsCache.timestamp
		elapsedSeconds = cmn.DivRound(elapsed, second)
		elapsedMillis  = cmn.DivRound(elapsed, millis)
	)
	ncache.timestamp = nowTs
	for mpath := range ctx.mpath2disks {
		ncache.mpathUtil[mpath] = 0
		if rr, ok := ncache.mpathRR[mpath]; ok {
			rr.Store(0)
		} else {
			ncache.mpathRR[mpath] = atomic.NewInt32(0)
		}
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
		stat, ok := disksStats[disk]
		if !ok {
			glog.Errorf("no block stats for disk %s", disk) // TODO: remove
			continue
		}
		ncache.diskIOms[disk] = stat.IOMs()
		ncache.diskRms[disk] = stat.ReadMs()
		ncache.diskRBytes[disk] = stat.ReadBytes()
		ncache.diskWms[disk] = stat.WriteMs()
		ncache.diskWBytes[disk] = stat.WriteBytes()

		if _, ok := statsCache.diskIOms[disk]; !ok {
			missingInfo = true
			continue
		}
		// deltas
		var (
			ioms       = stat.IOMs() - statsCache.diskIOms[disk]
			readBytes  = stat.ReadBytes() - statsCache.diskRBytes[disk]
			writeBytes = stat.WriteBytes() - statsCache.diskWBytes[disk]
		)
		if elapsedMillis > 0 {
			ncache.diskUtil[disk] = cmn.DivRound(ioms*100, elapsedMillis)
		} else {
			ncache.diskUtil[disk] = statsCache.diskUtil[disk]
		}
		ncache.mpathUtil[mpath] += ncache.diskUtil[disk]
		if elapsedSeconds > 0 {
			ncache.diskRBps[disk] = cmn.DivRound(readBytes, elapsedSeconds)
			ncache.diskWBps[disk] = cmn.DivRound(writeBytes, elapsedSeconds)
		} else {
			ncache.diskRBps[disk] = statsCache.diskRBps[disk]
			ncache.diskWBps[disk] = statsCache.diskWBps[disk]
		}
	}

	// average and max
	var (
		maxUtil    int64
		expireTime int64
		config     = cmn.GCO.Get()
	)
	for mpath, disks := range ctx.mpath2disks {
		numDisk := int64(len(disks))
		ncache.mpathUtil[mpath] /= numDisk
		maxUtil = cmn.MaxI64(maxUtil, ncache.mpathUtil[mpath])
	}
	ctx.mpathLock.Unlock()

	if missingInfo {
		expireTime = int64(config.Disk.IostatTimeShort)
	} else { // use the maximum utilization to determine expiration time
		var (
			lowWM     = cmn.MaxI64(config.Disk.DiskUtilLowWM, 1)
			highWM    = cmn.MinI64(config.Disk.DiskUtilHighWM, 100)
			delta     = int64(config.Disk.IostatTimeLong - config.Disk.IostatTimeShort)
			utilRatio = cmn.RatioPct(highWM, lowWM, maxUtil)
		)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		expireTime = int64(config.Disk.IostatTimeShort) + delta*(100-utilRatio)/100
	}
	ncache.expireTime = nowTs + expireTime
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
