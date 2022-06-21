// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
		GetAllMpathUtils() *MpathUtil
		GetMpathUtil(mpath string) int64
		AddMpath(mpath string, fs string) (FsDisks, error)
		RemoveMpath(mpath string)
		FillDiskStats(m AllDiskStats)
	}

	FsDisks      map[string]int64 // disk name => sector size
	DiskStats    struct{ RBps, Ravg, WBps, Wavg, Util int64 }
	AllDiskStats map[string]DiskStats

	MpathUtil sync.Map

	cache struct {
		expireTime int64
		timestamp  int64

		ioms   map[string]int64 // IO millis
		util   map[string]int64 // utilization
		rms    map[string]int64 // read millis
		rbytes map[string]int64 // read bytes
		reads  map[string]int64 // completed read requests
		rbps   map[string]int64 // read B/s
		ravg   map[string]int64 // average read size
		wms    map[string]int64 // write millis
		wbytes map[string]int64 // written bytes
		writes map[string]int64 // completed write requests
		wbps   map[string]int64 // write B/s
		wavg   map[string]int64 // average write size

		mpathUtil   map[string]int64 // Average utilization of the disks, range [0, 100].
		mpathUtilRO MpathUtil        // Read-only copy of `mpathUtil`.
	}
	ios struct {
		sync.RWMutex
		mpath2disks map[string]FsDisks
		disk2mpath  cos.SimpleKVs
		disk2sysfn  cos.SimpleKVs
		cache       atomic.Pointer
		cacheHst    [16]*cache
		cacheIdx    int
		busy        atomic.Bool
	}
)

// interface guard
var _ IOStater = (*ios)(nil)

///////////////
// MpathUtil //
///////////////

func (x *MpathUtil) Get(mpath string) int64 {
	if v, ok := (*sync.Map)(x).Load(mpath); ok {
		util := v.(int64)
		return util
	}
	return 100 // assume the worst
}

func (x *MpathUtil) Set(mpath string, util int64) {
	(*sync.Map)(x).Store(mpath, util)
}

/////////
// ios //
/////////

func New() IOStater {
	ios := &ios{
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

func newIostatCache() *cache {
	return &cache{
		ioms:      make(map[string]int64, 4), // TODO: mfs.ios.num-mountpaths(...) to opt
		util:      make(map[string]int64, 4),
		rms:       make(map[string]int64, 4),
		rbytes:    make(map[string]int64, 4),
		reads:     make(map[string]int64, 4),
		rbps:      make(map[string]int64, 4),
		ravg:      make(map[string]int64, 4),
		wms:       make(map[string]int64, 4),
		wbytes:    make(map[string]int64, 4),
		writes:    make(map[string]int64, 4),
		wbps:      make(map[string]int64, 4),
		wavg:      make(map[string]int64, 4),
		mpathUtil: make(map[string]int64, 4),
	}
}

func (ios *ios) AddMpath(mpath, fs string) (fsdisks FsDisks, err error) {
	config := cmn.GCO.Get()
	fsdisks = fs2disks(fs, config.TestingEnv())
	if len(fsdisks) == 0 {
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
func (ios *ios) removeMpathDiskTesting(mpath, disk string) {
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

func (ios *ios) removeMpathDisk(mpath, disk string) {
	mp, ok := ios.disk2mpath[disk]
	if !ok {
		return
	}
	debug.Assertf(mp == mpath, "(mpath %s => disk %s => mpath %s) violation", mp, disk, mpath)
	delete(ios.disk2mpath, disk)
}

func (ios *ios) RemoveMpath(mpath string) {
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

func (ios *ios) GetAllMpathUtils() *MpathUtil {
	cache := ios.refreshIostatCache()
	return &cache.mpathUtilRO
}

func (ios *ios) GetMpathUtil(mpath string) int64 {
	return ios.GetAllMpathUtils().Get(mpath)
}

func (ios *ios) FillDiskStats(m AllDiskStats) {
	debug.Assert(m != nil)
	cache := ios.refreshIostatCache()
	for disk := range cache.ioms {
		m[disk] = DiskStats{
			RBps: cache.rbps[disk],
			Ravg: cache.ravg[disk],
			WBps: cache.wbps[disk],
			Wavg: cache.wavg[disk],
			Util: cache.util[disk],
		}
	}
	for disk := range m {
		if _, ok := cache.ioms[disk]; !ok {
			delete(m, disk)
		}
	}
}

// private methods ---

// update iostat cache
func (ios *ios) refreshIostatCache() *cache {
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
	config := cmn.GCO.Get()
	ncache, maxUtil, missingInfo := ios._refresh(config)

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

func (ios *ios) _refresh(config *cmn.Config) (ncache *cache, maxUtil int64, missingInfo bool) {
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
	for disk := range ncache.ioms {
		if _, ok := ios.disk2mpath[disk]; !ok {
			ncache = newIostatCache()
			ios.cacheHst[ios.cacheIdx] = ncache
		}
	}

	osDiskStats := readDiskStats(ios.disk2mpath, ios.disk2sysfn)
	for disk, mpath := range ios.disk2mpath {
		ncache.rbps[disk] = 0
		ncache.wbps[disk] = 0
		ncache.util[disk] = 0
		ncache.ravg[disk] = 0
		ncache.wavg[disk] = 0
		osDisk, ok := osDiskStats[disk]
		if !ok {
			glog.Errorf("no block stats for disk %s", disk) // TODO: remove
			continue
		}
		ncache.ioms[disk] = osDisk.IOMs()
		ncache.rms[disk] = osDisk.ReadMs()
		ncache.rbytes[disk] = osDisk.ReadBytes()
		ncache.reads[disk] = osDisk.Reads()
		ncache.wms[disk] = osDisk.WriteMs()
		ncache.wbytes[disk] = osDisk.WriteBytes()
		ncache.writes[disk] = osDisk.Writes()

		if _, ok := statsCache.ioms[disk]; !ok {
			missingInfo = true
			continue
		}
		// deltas
		var (
			ioMs       = ncache.ioms[disk] - statsCache.ioms[disk]
			reads      = ncache.reads[disk] - statsCache.reads[disk]
			writes     = ncache.writes[disk] - statsCache.writes[disk]
			readBytes  = ncache.rbytes[disk] - statsCache.rbytes[disk]
			writeBytes = ncache.wbytes[disk] - statsCache.wbytes[disk]
		)
		if elapsedMillis > 0 {
			// On macOS computation of `diskUtil` may sometimes exceed 100%
			// which may cause some further inaccuracies.
			if ioMs >= elapsedMillis {
				ncache.util[disk] = 100
			} else {
				ncache.util[disk] = cos.DivRound(ioMs*100, elapsedMillis)
			}
		} else {
			ncache.util[disk] = statsCache.util[disk]
		}
		if !config.TestingEnv() {
			ncache.mpathUtil[mpath] += ncache.util[disk]
		}
		if elapsedSeconds > 0 {
			ncache.rbps[disk] = cos.DivRound(readBytes, elapsedSeconds)
			ncache.wbps[disk] = cos.DivRound(writeBytes, elapsedSeconds)
		} else {
			ncache.rbps[disk] = statsCache.rbps[disk]
			ncache.wbps[disk] = statsCache.wbps[disk]
		}
		if reads > 0 {
			ncache.ravg[disk] = cos.DivRound(readBytes, reads)
		} else if elapsedSeconds == 0 {
			ncache.ravg[disk] = statsCache.ravg[disk]
		} else {
			ncache.ravg[disk] = 0
		}
		if writes > 0 {
			ncache.wavg[disk] = cos.DivRound(writeBytes, writes)
		} else if elapsedSeconds == 0 {
			ncache.wavg[disk] = statsCache.wavg[disk]
		} else {
			ncache.wavg[disk] = 0
		}
	}

	// average and max
	if config.TestingEnv() {
		for mpath, disks := range ios.mpath2disks {
			debug.Assert(len(disks) <= 1) // testing env: one (shared) disk per mpath
			var u int64
			for d := range disks {
				u = ncache.util[d]
				ncache.mpathUtil[mpath] = u
				break
			}
			ncache.mpathUtilRO.Set(mpath, u)
			maxUtil = cos.MaxI64(maxUtil, u)
		}
		return
	}

	for mpath, disks := range ios.mpath2disks {
		numDisk := int64(len(disks))
		if numDisk == 0 {
			debug.Assert(ncache.mpathUtil[mpath] == 0)
			continue
		}
		u := ncache.mpathUtil[mpath] / numDisk
		ncache.mpathUtil[mpath] = u
		ncache.mpathUtilRO.Set(mpath, u)
		maxUtil = cos.MaxI64(maxUtil, u)
	}
	return
}

func (ios *ios) getStatsCache() *cache {
	cache := (*cache)(ios.cache.Load())
	return cache
}

func (ios *ios) putStatsCache(cache *cache) {
	ios.cache.Store(unsafe.Pointer(cache))
}
