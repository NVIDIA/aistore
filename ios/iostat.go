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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

type (
	iostatContext struct {
		sync.RWMutex
		mpath2disks map[string]fsDisks
		disk2mpath  cos.SimpleKVs
		sorted      []string
		disk2sysfn  cos.SimpleKVs
		cache       atomic.Pointer
		cacheHst    [16]*ioStatCache
		cacheIdx    int
		busy        atomic.Bool
	}
	SelectedDiskStats struct {
		RBps, WBps, Util int64
	}

	MpathsUtils sync.Map

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

		mpathUtil   map[string]int64 // Average utilization of the disks, range [0, 100].
		mpathUtilRO MpathsUtils      // Read-only copy of `mpathUtil`.
	}

	IOStater interface {
		GetAllMpathUtils() *MpathsUtils
		GetMpathUtil(mpath string) int64
		AddMpath(mpath string, fs string)
		RemoveMpath(mpath string)
		LogAppend(log []string) []string
		GetSelectedDiskStats() (m map[string]*SelectedDiskStats)
	}
)

// interface guard
var _ IOStater = (*iostatContext)(nil)

func init() {
	// NOTE: publish disk utilizations via host:port/debug/vars (debug mode)
	debug.NewExpvar(glog.SmoduleIOS)
}

func NewIostatContext() IOStater {
	ctx := &iostatContext{
		mpath2disks: make(map[string]fsDisks, 10),
		disk2mpath:  make(cos.SimpleKVs, 10),
		sorted:      make([]string, 0, 10),
		disk2sysfn:  make(cos.SimpleKVs, 10),
	}
	for i := 0; i < len(ctx.cacheHst); i++ {
		ctx.cacheHst[i] = newIostatCache()
	}
	ctx.putStatsCache(ctx.cacheHst[0])
	ctx.cacheIdx = 0
	ctx.busy.Store(false)
	return ctx
}

func newIostatCache() *ioStatCache {
	return &ioStatCache{
		diskIOms:   make(map[string]int64, 4), // TODO: mfs.ios.num-mountpaths(...) to opt
		diskUtil:   make(map[string]int64, 4),
		diskRms:    make(map[string]int64, 4),
		diskRBytes: make(map[string]int64, 4),
		diskRBps:   make(map[string]int64, 4),
		diskWms:    make(map[string]int64, 4),
		diskWBytes: make(map[string]int64, 4),
		diskWBps:   make(map[string]int64, 4),
		mpathUtil:  make(map[string]int64, 4),
	}
}

func (x *MpathsUtils) Util(mpath string) int64 {
	if v, ok := (*sync.Map)(x).Load(mpath); ok {
		util := v.(int64)
		debug.Assert(util >= 0 && util <= 100, util)
		return util
	}
	return 100 // If hasn't been updated yet we assume the worst.
}

func (x *MpathsUtils) Store(mpath string, util int64) {
	(*sync.Map)(x).Store(mpath, util)
}

func (ctx *iostatContext) AddMpath(mpath, fs string) {
	ctx.Lock()
	defer ctx.Unlock()

	config := cmn.GCO.Get()
	fsdisks := fs2disks(fs)
	if len(fsdisks) == 0 {
		glog.Errorf("no disks associated with (mountpath: %q, fs: %q)", mpath, fs)
		return
	}
	if dd, ok := ctx.mpath2disks[mpath]; ok {
		s := fmt.Sprintf("mountpath %s already added, disks %+v %+v", mpath, dd, fsdisks)
		cos.AssertMsg(false, s)
	}
	ctx.mpath2disks[mpath] = fsdisks
	for disk := range fsdisks {
		if mp, ok := ctx.disk2mpath[disk]; ok && !config.TestingEnv() {
			s := fmt.Sprintf("disk sharing is not permitted: mp %s, add fs %s mp %s, disk %s",
				mp, fs, mpath, disk)
			cos.AssertMsg(false, s)
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
func (ctx *iostatContext) removeMpathDiskTesting(mpath, disk string) {
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

func (ctx *iostatContext) removeMpathDisk(mpath, disk string) {
	mp, ok := ctx.disk2mpath[disk]
	if !ok {
		return
	}
	debug.Assertf(mp == mpath, "(mpath %s => disk %s => mpath %s) violation", mp, disk, mpath)
	delete(ctx.disk2mpath, disk)
}

func (ctx *iostatContext) RemoveMpath(mpath string) {
	ctx.Lock()
	defer ctx.Unlock()

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

func (ctx *iostatContext) GetAllMpathUtils() *MpathsUtils {
	cache := ctx.refreshIostatCache()
	return &cache.mpathUtilRO
}

func (ctx *iostatContext) GetMpathUtil(mpath string) int64 {
	return ctx.GetAllMpathUtils().Util(mpath)
}

func (ctx *iostatContext) GetSelectedDiskStats() (m map[string]*SelectedDiskStats) {
	cache := ctx.refreshIostatCache()
	m = make(map[string]*SelectedDiskStats, len(cache.diskIOms))
	for disk := range cache.diskIOms {
		m[disk] = &SelectedDiskStats{
			RBps: cache.diskRBps[disk],
			WBps: cache.diskWBps[disk],
			Util: cache.diskUtil[disk],
		}
	}
	return
}

func (ctx *iostatContext) LogAppend(lines []string) []string {
	cache := ctx.refreshIostatCache()
	ctx.RLock()
	for _, disk := range ctx.sorted {
		if _, ok := cache.diskIOms[disk]; !ok {
			continue
		}
		util := cache.diskUtil[disk]
		if util == 0 {
			continue
		}
		rbps := cos.B2S(cache.diskRBps[disk], 0)
		wbps := cos.B2S(cache.diskWBps[disk], 0)
		line := fmt.Sprintf("%s: %s/s, %s/s, %d%%", disk, rbps, wbps, util)
		lines = append(lines, line)
	}
	ctx.RUnlock()
	return lines
}

// private methods ---

// update iostat cache
func (ctx *iostatContext) refreshIostatCache() *ioStatCache {
	var (
		expireTime int64
		nowTs      = mono.NanoTime()
		statsCache = ctx.getStatsCache()
	)
	if statsCache.expireTime > nowTs {
		return statsCache
	}
	if !ctx.busy.CAS(false, true) {
		return statsCache // never want callers to wait
	}
	defer ctx.busy.Store(false)

	// refresh under mpath lock
	ncache, maxUtil, missingInfo := ctx._refresh()

	config := cmn.GCO.Get()
	if missingInfo {
		expireTime = int64(config.Disk.IostatTimeShort)
	} else { // use the maximum utilization to determine expiration time
		var (
			lowWM     = cos.MaxI64(config.Disk.DiskUtilLowWM, 1)
			highWM    = cos.MinI64(config.Disk.DiskUtilHighWM, 100)
			delta     = int64(config.Disk.IostatTimeLong - config.Disk.IostatTimeShort)
			utilRatio = cos.RatioPct(highWM, lowWM, maxUtil)
		)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		expireTime = int64(config.Disk.IostatTimeShort) + delta*(100-utilRatio)/100
	}
	ncache.expireTime = nowTs + expireTime
	ctx.putStatsCache(ncache)

	return ncache
}

func (ctx *iostatContext) _refresh() (ncache *ioStatCache, maxUtil int64, missingInfo bool) {
	ctx.Lock()
	defer ctx.Unlock()

	ctx.cacheIdx++
	ctx.cacheIdx %= len(ctx.cacheHst)
	ncache = ctx.cacheHst[ctx.cacheIdx]

	var (
		statsCache     = ctx.getStatsCache()
		nowTs          = mono.NanoTime()
		elapsed        = nowTs - statsCache.timestamp
		elapsedSeconds = cos.DivRound(elapsed, int64(time.Second))
		elapsedMillis  = cos.DivRound(elapsed, int64(time.Millisecond))
	)

	ncache.timestamp = nowTs
	for mpath := range ctx.mpath2disks {
		ncache.mpathUtil[mpath] = 0
	}
	for disk := range ncache.diskIOms {
		if _, ok := ctx.disk2mpath[disk]; !ok {
			ncache = newIostatCache()
			ctx.cacheHst[ctx.cacheIdx] = ncache
		}
	}

	disksStats := readDiskStats(ctx.disk2mpath, ctx.disk2sysfn)
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
			ioMs       = stat.IOMs() - statsCache.diskIOms[disk]
			readBytes  = stat.ReadBytes() - statsCache.diskRBytes[disk]
			writeBytes = stat.WriteBytes() - statsCache.diskWBytes[disk]
		)
		if elapsedMillis > 0 {
			// NOTE: On macOS computation of `diskUtil` is not accurate and can
			//  sometimes exceed 100% which may cause some further inaccuracies.
			//  That is why we need to clamp the value.
			ncache.diskUtil[disk] = cos.MinI64(100, cos.DivRound(ioMs*100, elapsedMillis))
		} else {
			ncache.diskUtil[disk] = statsCache.diskUtil[disk]
		}
		ncache.mpathUtil[mpath] += ncache.diskUtil[disk]
		if elapsedSeconds > 0 {
			ncache.diskRBps[disk] = cos.DivRound(readBytes, elapsedSeconds)
			ncache.diskWBps[disk] = cos.DivRound(writeBytes, elapsedSeconds)
		} else {
			ncache.diskRBps[disk] = statsCache.diskRBps[disk]
			ncache.diskWBps[disk] = statsCache.diskWBps[disk]
		}
	}

	// average and max
	for mpath, disks := range ctx.mpath2disks {
		numDisk := int64(len(disks))
		util := ncache.mpathUtil[mpath] / numDisk

		ncache.mpathUtil[mpath] = util
		debug.SetExpvar(glog.SmoduleIOS, mpath+":util%", util)
		ncache.mpathUtilRO.Store(mpath, util)
		maxUtil = cos.MaxI64(maxUtil, util)
	}
	return
}

func (ctx *iostatContext) getStatsCache() *ioStatCache {
	cache := (*ioStatCache)(ctx.cache.Load())
	return cache
}

func (ctx *iostatContext) putStatsCache(cache *ioStatCache) {
	ctx.cache.Store(unsafe.Pointer(cache))
}
