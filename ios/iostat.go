// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const statsdir = "/sys/class/block"

// public
type (
	FsDisks map[string]int64 // disk name => sector size

	IOS interface {
		Clblk()
		GetAllMpathUtils() *MpathUtil
		GetMpathUtil(mpath string) int64
		AddMpath(mpath, fs string, label Label, config *cmn.Config) (FsDisks, error)
		RefreshDisks(mpath, fs string, disks []string) RefreshDisksResult
		RemoveMpath(mpath string, testingEnv bool)
		DiskStats(m AllDiskStats)
	}

	MpathUtil sync.Map

	RefreshDisksResult struct {
		FsDisks  FsDisks
		Fatal    error
		Lost     []error
		Attached []error
	}
)

// internal
type (
	cache struct {
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

		expireTime int64
		timestamp  int64
	}
	ios struct {
		mpath2disks map[string]FsDisks
		disk2mpath  cos.StrKVs
		disk2sysfn  cos.StrKVs
		blockStats  allBlockStats
		lsblk       ratomic.Pointer[LsBlk] // used only during target startup
		cache       ratomic.Pointer[cache]
		cacheHst    [16]*cache
		cacheIdx    int
		mu          sync.Mutex
		busy        atomic.Bool
	}
)

// interface guard
var _ IOS = (*ios)(nil)

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

func New(num int) IOS {
	ios := &ios{
		mpath2disks: make(map[string]FsDisks, num),
		disk2mpath:  make(cos.StrKVs, num),
		disk2sysfn:  make(cos.StrKVs, num),
		blockStats:  make(allBlockStats, num),
	}
	for i := range len(ios.cacheHst) {
		ios.cacheHst[i] = newCache(num)
	}
	ios._put(ios.cacheHst[0])
	ios.cacheIdx = 0
	ios.busy.Store(false) // redundant on purpose

	// once (cleared via Clblk)
	res, err := lsblk("new-ios", true)
	if err != nil {
		nlog.Errorln("new IOS:", err)
	} else {
		debug.Assert(res != nil)
		ios.lsblk.Store(res)
	}

	return ios
}

func newCache(num int) *cache {
	return &cache{
		ioms:      make(map[string]int64, num),
		util:      make(map[string]int64, num),
		rms:       make(map[string]int64, num),
		rbytes:    make(map[string]int64, num),
		reads:     make(map[string]int64, num),
		rbps:      make(map[string]int64, num),
		ravg:      make(map[string]int64, num),
		wms:       make(map[string]int64, num),
		wbytes:    make(map[string]int64, num),
		writes:    make(map[string]int64, num),
		wbps:      make(map[string]int64, num),
		wavg:      make(map[string]int64, num),
		mpathUtil: make(map[string]int64, num),
	}
}

// zero-out lsblk cache (reusing it during target startup, clearing right after)
func (ios *ios) Clblk() { ios.lsblk.Store(nil) }

func (ios *ios) _get() *cache      { return ios.cache.Load() }
func (ios *ios) _put(cache *cache) { ios.cache.Store(cache) }

//
// add mountpath
//

func (ios *ios) AddMpath(mpath, fs string, label Label, config *cmn.Config) (fsdisks FsDisks, err error) {
	var (
		warn       string
		testingEnv = config.TestingEnv()
		fspaths    = config.LocalConfig.FSP.Paths
	)
	if pres := ios.lsblk.Load(); pres != nil {
		res := *pres
		fsdisks, err = fs2disks(&res, mpath, fs, label, len(fspaths), testingEnv /*no-disks is ok*/)
	} else {
		res, errInfo := lsblk(fs, testingEnv /*err is not fatal*/)
		if errInfo != nil {
			nlog.Errorln("add-mpath:", errInfo)
		} else {
			fsdisks, err = fs2disks(res, mpath, fs, label, len(fspaths), testingEnv /*no-disks is ok*/)
			if err == nil {
				ios.lsblk.Store(res)
			}
		}
	}
	if len(fsdisks) == 0 || err != nil {
		return
	}
	ios.mu.Lock()
	warn, err = ios._add(mpath, label, fsdisks, fspaths, testingEnv)
	ios.mu.Unlock()

	if err != nil {
		nlog.Errorln(err)
	}
	if warn != "" {
		nlog.Infoln(warn)
	}
	return
}

func (ios *ios) _add(mpath string, label Label, fsdisks FsDisks, fspaths cos.StrKVs, testingEnv bool) (warn string, _ error) {
	if dd, ok := ios.mpath2disks[mpath]; ok {
		return "", fmt.Errorf("duplicate mountpath %s (disks %s, %s)", mpath, dd._str(), fsdisks._str())
	}

	ios.mpath2disks[mpath] = fsdisks
	for disk := range fsdisks {
		if mp, ok := ios.disk2mpath[disk]; ok && !testingEnv && !cmn.AllowSharedDisksAndNoDisks {
			if label.IsNil() {
				return "", fmt.Errorf("disk %s is shared between mountpaths %s and %s", disk, mpath, mp)
			}
			var otherLabel Label
			if o, ok := fspaths[mp]; ok {
				otherLabel = Label(o)
			}
			warn = fmt.Sprintf("Warning: disk %s is shared between %s%s and %s%s",
				disk, mpath, label.ToLog(), mp, otherLabel.ToLog())
		}
		ios.disk2mpath[disk] = mpath
		ios.blockStats[disk] = &blockStats{}
	}

	for disk, mountpath := range ios.disk2mpath {
		if _, ok := ios.disk2sysfn[disk]; ok {
			continue
		}
		path := filepath.Join(statsdir, disk, "stat")
		ios.disk2sysfn[disk] = path

		// multipath NVMe: alternative block-stats location
		cdisk, err := icn(disk, statsdir)
		if err != nil {
			if label.IsNil() {
				return "", err
			}
			if warn != "" {
				warn += "\n"
			}
			warn += fmt.Sprint("Warning:", err)
		}
		if cdisk != "" {
			cpath := filepath.Join(statsdir, cdisk, "stat")
			if icnPath(ios.disk2sysfn[disk], cpath, mountpath) {
				if warn != "" {
					warn += "\n"
				}
				warn += fmt.Sprint("Info: alternative block-stats path:", disk, path, "=>", cdisk, cpath)
				ios.disk2sysfn[disk] = cpath
			}
		}
	}
	if len(ios.disk2sysfn) != len(ios.disk2mpath) {
		for disk := range ios.disk2sysfn {
			if _, ok := ios.disk2mpath[disk]; !ok {
				delete(ios.disk2sysfn, disk)
			}
		}
	}
	return warn, nil
}

// at runtime:
// - resolve (mpath, filesystem) => disks
// - revalidate disk(s)
// - note: part of the alerting mechanism, via filesystem health checker (FSHC)
func (ios *ios) RefreshDisks(mpath, fs string, disks []string) (out RefreshDisksResult) {
	debug.Assert(len(disks) > 0)
	res, err := lsblk(fs, true /*err is not fatal*/)
	if err != nil {
		out.Fatal = cmn.NewErrMpathNoDisks(mpath, fs, err)
		return out
	}

	out.FsDisks, err = fs2disks(res, mpath, fs, Label(""), len(disks), false /*no-disks is ok*/)
	if err != nil {
		out.Fatal = err
		return out
	}
	fsdisks := out.FsDisks
	for _, d := range disks {
		if _, ok := fsdisks[d]; !ok {
			out.Lost = append(out.Lost, cmn.NewErrMpathLostDisk(mpath, fs, d, disks, fsdisks.ToSlice()))
		}
	}
	for d := range fsdisks {
		if !cos.StringInSlice(d, disks) {
			out.Attached = append(out.Attached, cmn.NewErrMpathNewDisk(mpath, fs, disks, fsdisks.ToSlice()))

			// TODO -- FIXME: under lock: update ios.mpath2disks and related state; log
			ios._update(mpath, fsdisks, disks)
		}
	}

	// TODO -- FIXME: read/write a few bytes, and check that block stats increment

	return out
}

func (ios *ios) _update(mpath string, fsdisks FsDisks, disks []string) {
	debug.Assert(false, "not implemented yet", mpath, fsdisks, disks, ios.mpath2disks)
}

//
// remove mountpath
//

func (ios *ios) RemoveMpath(mpath string, testingEnv bool) {
	ios.mu.Lock()
	ios._del(mpath, testingEnv)
	ios.mu.Unlock()
}

func (ios *ios) _del(mpath string, testingEnv bool) {
	oldDisks, ok := ios.mpath2disks[mpath]
	if !ok {
		nlog.Warningf("mountpath %s already removed", mpath)
		return
	}
	for disk := range oldDisks {
		if testingEnv {
			ios._delDiskTesting(mpath, disk)
		} else {
			ios._delDisk(mpath, disk)
		}
	}
	delete(ios.mpath2disks, mpath)
}

// TestingEnv ("disk sharing"):
// If another mountpath containing the same disk is found, the disk2mpath map
// gets updated. Otherwise, go ahead and remove the "disk".
func (ios *ios) _delDiskTesting(mpath, disk string) {
	if _, ok := ios.disk2mpath[disk]; !ok {
		return
	}
	for path, disks := range ios.mpath2disks {
		if path == mpath {
			continue
		}
		for dsk := range disks {
			if dsk == disk {
				ios.disk2mpath[disk] = path // found - keeping
				return
			}
		}
	}
	delete(ios.mpath2disks, disk)
}

func (ios *ios) _delDisk(mpath, disk string) {
	mp, ok := ios.disk2mpath[disk]
	if !ok {
		return
	}
	debug.Assertf(mp == mpath, "(mpath %s => disk %s => mpath %s) violation", mp, disk, mpath)
	delete(ios.disk2mpath, disk)
	delete(ios.blockStats, disk)
}

//
// get utilization and stats; refresh stats periodically
//

func (ios *ios) GetAllMpathUtils() *MpathUtil {
	cache := ios.refresh()
	return &cache.mpathUtilRO
}

func (ios *ios) GetMpathUtil(mpath string) int64 {
	return ios.GetAllMpathUtils().Get(mpath)
}

func (ios *ios) DiskStats(m AllDiskStats) {
	cache := ios.refresh()
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

// update iostat cache
func (ios *ios) refresh() *cache {
	var (
		nowTs      = mono.NanoTime()
		statsCache = ios._get()
	)
	if statsCache.expireTime > nowTs {
		return statsCache
	}
	if !ios.busy.CAS(false, true) {
		return statsCache // never want callers to wait
	}

	ncache := ios.doRefresh(nowTs)
	ios.busy.Store(false)
	return ncache
}

func (ios *ios) doRefresh(nowTs int64) *cache {
	config := cmn.GCO.Get()
	ios.mu.Lock()
	ncache, maxUtil, missingInfo := ios._ref(config)
	ios.mu.Unlock()

	var expireTime int64
	if missingInfo {
		expireTime = int64(config.Disk.IostatTimeShort)
	} else { // use the maximum utilization to determine expiration time
		var (
			lowm      = max(config.Disk.DiskUtilLowWM, 1)
			hiwm      = min(config.Disk.DiskUtilHighWM, 100)
			delta     = int64(config.Disk.IostatTimeLong - config.Disk.IostatTimeShort)
			utilRatio = cos.RatioPct(hiwm, lowm, maxUtil)
		)
		utilRatio = (utilRatio + 5) / 10 * 10 // round to nearest tenth
		expireTime = int64(config.Disk.IostatTimeShort) + delta*(100-utilRatio)/100
	}
	ncache.expireTime = nowTs + expireTime
	ios._put(ncache)

	return ncache
}

func (ios *ios) _ref(config *cmn.Config) (ncache *cache, maxUtil int64, missingInfo bool) {
	ios.cacheIdx++
	ios.cacheIdx %= len(ios.cacheHst)
	ncache = ios.cacheHst[ios.cacheIdx] // from a pool

	var (
		statsCache     = ios._get()
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
			ncache = newCache(len(statsCache.ioms))
			ios.cacheHst[ios.cacheIdx] = ncache
		}
	}

	readStats(ios.disk2mpath, ios.disk2sysfn, ios.blockStats)
	for disk, mpath := range ios.disk2mpath {
		ncache.rbps[disk] = 0
		ncache.wbps[disk] = 0
		ncache.util[disk] = 0
		ncache.ravg[disk] = 0
		ncache.wavg[disk] = 0
		ds := ios.blockStats[disk]
		ncache.ioms[disk] = ds.IOMs()
		ncache.rms[disk] = ds.ReadMs()
		ncache.rbytes[disk] = ds.ReadBytes()
		ncache.reads[disk] = ds.Reads()
		ncache.wms[disk] = ds.WriteMs()
		ncache.wbytes[disk] = ds.WriteBytes()
		ncache.writes[disk] = ds.Writes()

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
			maxUtil = max(maxUtil, u)
		}
		return
	}

	for mpath, disks := range ios.mpath2disks {
		num := int64(len(disks))
		if num == 0 {
			debug.Assert(ncache.mpathUtil[mpath] == 0)
			continue
		}
		u := cos.DivRound(ncache.mpathUtil[mpath], num)
		ncache.mpathUtil[mpath] = u
		ncache.mpathUtilRO.Set(mpath, u)
		maxUtil = max(maxUtil, u)
	}
	return
}

/////////////
// FsDisks //
/////////////

func (fsdisks FsDisks) ToSlice() (disks []string) {
	disks = make([]string, len(fsdisks))
	var i int
	for d := range fsdisks {
		disks[i] = d
		i++
	}
	return disks
}

func (fsdisks FsDisks) _str() string {
	s := fmt.Sprintf("%v", fsdisks) // with sector sizes
	return strings.TrimPrefix(s, "map")
}
