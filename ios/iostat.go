// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

type (
	IOStater interface {
		GetAllMpathUtils() *MpathsUtils
		GetMpathUtil(mpath string) int64
		AddMpath(mpath string, fs string) (FsDisks, error)
		RemoveMpath(mpath string)
		FillDiskStats(m AllDiskStats)
	}

	FsDisks      map[string]int64 // disk name => sector size
	DiskStats    struct{ RBps, WBps, Util int64 }
	AllDiskStats map[string]DiskStats

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

	iostatContext struct {
		sync.RWMutex
		mpath2disks map[string]FsDisks
		disk2mpath  cos.SimpleKVs
		disk2sysfn  cos.SimpleKVs
		cache       atomic.Pointer
		cacheHst    [16]*ioStatCache
		cacheIdx    int
		busy        atomic.Bool
	}
)

// interface guard
var _ IOStater = (*iostatContext)(nil)

func init() {
	// NOTE: publish disk utilizations via host:port/debug/vars (debug mode)
	debug.NewExpvar(glog.SmoduleIOS)
}

/////////////////
// MpathsUtils //
/////////////////

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

///////////////////
// iostatContext //
///////////////////

func NewIostatContext() IOStater {
	ios := &iostatContext{
		mpath2disks: make(map[string]FsDisks, 10),
		disk2mpath:  make(cos.SimpleKVs, 10),
		disk2sysfn:  make(cos.SimpleKVs, 10),
	}
	for i := 0; i < len(ios.cacheHst); i++ {
		ios.cacheHst[i] = newIostatCache()
	}
	ios.putStatsCache(ios.cacheHst[0])
	ios.cacheIdx = 0
	ios.busy.Store(false)
	return ios
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

func (ios *iostatContext) AddMpath(mpath, fs string) (fsdisks FsDisks, err error) {
	config := cmn.GCO.Get()
	fsdisks = fs2disks(fs)
	if len(fsdisks) == 0 {
		if !config.TestingEnv() {
			glog.Errorf("mountpath %s has no disks (fs %q)", mpath, fs)
		}
		return
	}

	ios.Lock()
	defer ios.Unlock()

	if dd, ok := ios.mpath2disks[mpath]; ok {
		err = fmt.Errorf("mountpath %s already exists, disks %v (%v)", mpath, dd, fsdisks)
		glog.Error(err)
		return
	}
	ios.mpath2disks[mpath] = fsdisks
	for disk := range fsdisks {
		if mp, ok := ios.disk2mpath[disk]; ok && !config.TestingEnv() {
			err = fmt.Errorf("disk %s: disk sharing is not allowed: %s vs %s", disk, mpath, mp)
			glog.Error(err)
			return
		}
		ios.disk2mpath[disk] = mpath
	}
	for disk := range ios.disk2mpath {
		if _, ok := ios.disk2sysfn[disk]; !ok {
			ios.disk2sysfn[disk] = fmt.Sprintf("/sys/class/block/%v/stat", disk)
		}
	}
	if len(ios.disk2sysfn) != len(ios.disk2mpath) {
		for disk := range ios.disk2sysfn {
			if _, ok := ios.disk2mpath[disk]; !ok {
				delete(ios.disk2sysfn, disk)
			}
		}
	}
	return
}

// For TestingEnv to avoid a case when removing a mountpath also empties
// `disk` list, so a target stops generating disk stats.
// If another mountpath containing the same disk is found, the disk2mpath map
// is updated. Otherwise, the function removes disk from the disk2map map.
func (ios *iostatContext) removeMpathDiskTesting(mpath, disk string) {
	if _, ok := ios.disk2mpath[disk]; !ok {
		return
	}
	for path, disks := range ios.mpath2disks {
		if path == mpath {
			continue
		}
		for dsk := range disks {
			if dsk == disk {
				ios.disk2mpath[disk] = path
				return
			}
		}
	}
	delete(ios.mpath2disks, disk)
}

func (ios *iostatContext) removeMpathDisk(mpath, disk string) {
	mp, ok := ios.disk2mpath[disk]
	if !ok {
		return
	}
	debug.Assertf(mp == mpath, "(mpath %s => disk %s => mpath %s) violation", mp, disk, mpath)
	delete(ios.disk2mpath, disk)
}

func (ios *iostatContext) RemoveMpath(mpath string) {
	ios.Lock()
	defer ios.Unlock()

	config := cmn.GCO.Get()
	oldDisks, ok := ios.mpath2disks[mpath]
	if !ok {
		glog.Warningf("mountpath %s already removed", mpath)
		return
	}
	for disk := range oldDisks {
		if config.TestingEnv() {
			ios.removeMpathDiskTesting(mpath, disk)
		} else {
			ios.removeMpathDisk(mpath, disk)
		}
	}
	delete(ios.mpath2disks, mpath)
}

func (ios *iostatContext) GetAllMpathUtils() *MpathsUtils {
	cache := ios.refreshIostatCache()
	return &cache.mpathUtilRO
}

func (ios *iostatContext) GetMpathUtil(mpath string) int64 {
	return ios.GetAllMpathUtils().Util(mpath)
}

func (ios *iostatContext) FillDiskStats(m AllDiskStats) {
	debug.Assert(m != nil)
	cache := ios.refreshIostatCache()
	for disk := range cache.diskIOms {
		m[disk] = DiskStats{
			RBps: cache.diskRBps[disk],
			WBps: cache.diskWBps[disk],
			Util: cache.diskUtil[disk],
		}
	}
	for disk := range m {
		if _, ok := cache.diskIOms[disk]; !ok {
			delete(m, disk)
		}
	}
}

// private methods ---

// update iostat cache
func (ios *iostatContext) refreshIostatCache() *ioStatCache {
	var (
		expireTime int64
		nowTs      = mono.NanoTime()
		statsCache = ios.getStatsCache()
	)
	if statsCache.expireTime > nowTs {
		return statsCache
	}
	if !ios.busy.CAS(false, true) {
		return statsCache // never want callers to wait
	}
	defer ios.busy.Store(false)

	// refresh under mpath lock
	ncache, maxUtil, missingInfo := ios._refresh()

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
	ios.putStatsCache(ncache)

	return ncache
}

func (ios *iostatContext) _refresh() (ncache *ioStatCache, maxUtil int64, missingInfo bool) {
	ios.Lock()
	defer ios.Unlock()

	ios.cacheIdx++
	ios.cacheIdx %= len(ios.cacheHst)
	ncache = ios.cacheHst[ios.cacheIdx]

	var (
		statsCache     = ios.getStatsCache()
		nowTs          = mono.NanoTime()
		elapsed        = nowTs - statsCache.timestamp
		elapsedSeconds = cos.DivRound(elapsed, int64(time.Second))
		elapsedMillis  = cos.DivRound(elapsed, int64(time.Millisecond))
	)

	ncache.timestamp = nowTs
	for mpath := range ios.mpath2disks {
		ncache.mpathUtil[mpath] = 0
	}
	for disk := range ncache.diskIOms {
		if _, ok := ios.disk2mpath[disk]; !ok {
			ncache = newIostatCache()
			ios.cacheHst[ios.cacheIdx] = ncache
		}
	}

	disksStats := readDiskStats(ios.disk2mpath, ios.disk2sysfn)
	for disk, mpath := range ios.disk2mpath {
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
	for mpath, disks := range ios.mpath2disks {
		numDisk := int64(len(disks))
		if numDisk == 0 {
			debug.Assert(ncache.mpathUtil[mpath] == 0)
			continue
		}
		util := ncache.mpathUtil[mpath] / numDisk

		ncache.mpathUtil[mpath] = util
		debug.SetExpvar(glog.SmoduleIOS, mpath+":util%", util)
		ncache.mpathUtilRO.Store(mpath, util)
		maxUtil = cos.MaxI64(maxUtil, util)
	}
	return
}

func (ios *iostatContext) getStatsCache() *ioStatCache {
	cache := (*ioStatCache)(ios.cache.Load())
	return cache
}

func (ios *iostatContext) putStatsCache(cache *ioStatCache) {
	ios.cache.Store(unsafe.Pointer(cache))
}
