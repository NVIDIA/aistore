// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/ios"
	"github.com/OneOfOne/xxhash"
)

const nodeXattrID = "user.ais.daemon_id"

// enum Mountpath.Flags
const (
	FlagBeingDisabled uint64 = 1 << iota
	FlagBeingDetached
)

const FlagWaitingDD = FlagBeingDisabled | FlagBeingDetached

// Terminology:
// - a mountpath is equivalent to (configurable) fspath - both terms are used interchangeably;
// - each mountpath is, simply, a local directory that is serviced by a local filesystem;
// - there's a 1-to-1 relationship between a mountpath and a local filesystem
//   (different mountpaths map onto different filesystems, and vise versa);
// - mountpaths of the form <filesystem-mountpoint>/a/b/c are supported.

type (
	Mountpath struct {
		lomCaches cos.MultiSyncMap // LOM caches
		info      string
		Path      string   // clean path
		cos.FS             // underlying filesystem
		Disks     []string // owned disks (ios.FsDisks map => slice)
		bpc       struct {
			m map[uint64]string
			sync.RWMutex
		}
		capacity   Capacity
		flags      uint64 // bit flags (set/get atomic)
		PathDigest uint64 // (HRW logic)
		cmu        sync.RWMutex
	}
	MPI map[string]*Mountpath

	// MountedFS holds all mountpaths for the target.
	MountedFS struct {
		// Iostats for the available mountpaths
		ios ios.IOS
		// fsIDs is set in which we store fsids of mountpaths. This allows for
		// determining if there are any duplications of file system - we allow
		// only one mountpath per file system.
		fsIDs map[cos.FsID]string
		// Available mountpaths - mountpaths which are used to store the data.
		available atomic.Pointer
		// Disabled mountpaths - mountpaths which for some reason did not pass
		// the health check and cannot be used for a moment.
		disabled atomic.Pointer

		// capacity
		cs        CapStatus
		csExpires atomic.Int64
		cmu       sync.RWMutex

		mu sync.RWMutex

		// allow disk sharing by multiple mountpaths and mountpaths with no disks whatsoever
		// (default = false)
		allowSharedDisksAndNoDisks bool
	}
	CapStatus struct {
		Err        error
		TotalUsed  uint64 // bytes
		TotalAvail uint64 // bytes
		PctAvg     int32  // used average (%)
		PctMax     int32  // max used (%)
		OOS        bool
	}
)

var mfs *MountedFS

///////////////////
// Mountpath //
///////////////////

func NewMountpath(mpath string) (mi *Mountpath, err error) {
	var (
		cleanMpath string
		fsInfo     cos.FS
	)
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	if _, err = os.Stat(cleanMpath); err != nil {
		return nil, cmn.NewErrNotFound("mountpath %q", mpath)
	}
	if fsInfo, err = makeFsInfo(cleanMpath); err != nil {
		return
	}
	mi = &Mountpath{
		Path:       cleanMpath,
		FS:         fsInfo,
		PathDigest: xxhash.ChecksumString64S(cleanMpath, cos.MLCG32),
	}
	mi.bpc.m = make(map[uint64]string, 16)
	return
}

// flags
func (mi *Mountpath) setFlags(flags uint64) (ok bool) {
	return cos.SetfAtomic(&mi.flags, flags)
}

func (mi *Mountpath) IsAnySet(flags uint64) bool {
	return cos.IsAnySetfAtomic(&mi.flags, flags)
}

func (mi *Mountpath) String() string {
	if mi.info == "" {
		switch len(mi.Disks) {
		case 0:
			mi.info = fmt.Sprintf("mp[%s, fs=%s]", mi.Path, mi.Fs)
		case 1:
			mi.info = fmt.Sprintf("mp[%s, %s]", mi.Path, mi.Disks[0])
		default:
			mi.info = fmt.Sprintf("mp[%s, %v]", mi.Path, mi.Disks)
		}
	}
	if !mi.IsAnySet(FlagWaitingDD) {
		return mi.info
	}
	l := len(mi.info)
	return mi.info[:l-1] + ", waiting-dd]"
}

func (mi *Mountpath) LomCache(idx int) *sync.Map { return mi.lomCaches.Get(idx) }

func LcacheIdx(digest uint64) int { return int(digest & (cos.MultiSyncMapCount - 1)) }

func (mi *Mountpath) EvictLomCache() {
	for idx := 0; idx < cos.MultiSyncMapCount; idx++ {
		cache := mi.LomCache(idx)
		cache.Range(func(key any, _ any) bool {
			cache.Delete(key)
			return true
		})
	}
}

func (mi *Mountpath) evictLomBucketCache(bck *cmn.Bck) {
	for idx := 0; idx < cos.MultiSyncMapCount; idx++ {
		cache := mi.LomCache(idx)
		cache.Range(func(hkey any, _ any) bool {
			uname := hkey.(string)
			b, _ := cmn.ParseUname(uname)
			if b.Equal(bck) {
				cache.Delete(hkey)
			}
			return true
		})
	}
}

func (mi *Mountpath) IsIdle(config *cmn.Config) bool {
	curr := mfs.ios.GetMpathUtil(mi.Path)
	return curr >= 0 && curr < config.Disk.DiskUtilLowWM
}

func (mi *Mountpath) CreateMissingBckDirs(bck *cmn.Bck) (err error) {
	for contentType := range CSM.m {
		dir := mi.MakePathCT(bck, contentType)
		if _, err = os.Stat(dir); err == nil {
			continue
		}
		if err = cos.CreateDir(dir); err != nil {
			return
		}
	}
	return
}

func (mi *Mountpath) backupAtmost(from, backup string, bcnt, atMost int) (newBcnt int) {
	var (
		fromPath   = filepath.Join(mi.Path, from)
		backupPath = filepath.Join(mi.Path, backup)
	)
	os.Remove(backupPath)
	newBcnt = bcnt
	if bcnt >= atMost {
		return
	}
	if _, err := os.Stat(fromPath); err != nil {
		return
	}
	if err := os.Rename(fromPath, backupPath); err != nil {
		glog.Error(err)
		os.Remove(fromPath)
	} else {
		newBcnt = bcnt + 1
	}
	return
}

func (mi *Mountpath) ClearMDs() {
	for _, mdPath := range mdFilesDirs {
		mi.Remove(mdPath)
	}
}

func (mi *Mountpath) Remove(path string) error {
	fpath := filepath.Join(mi.Path, path)
	if err := os.RemoveAll(fpath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (mi *Mountpath) SetDaemonIDXattr(tid string) error {
	cos.Assert(tid != "")
	// Validate if mountpath already has daemon ID set.
	mpathDaeID, err := _loadXattrID(mi.Path)
	if err != nil {
		return err
	}
	if mpathDaeID == tid {
		return nil
	}
	if mpathDaeID != "" && mpathDaeID != tid {
		return &ErrStorageIntegrity{
			Code: SieMpathIDMismatch,
			Msg:  fmt.Sprintf("target ID mismatch: %q vs %q(%q)", tid, mpathDaeID, mi),
		}
	}
	return SetXattr(mi.Path, nodeXattrID, []byte(tid))
}

// make-path methods

func (mi *Mountpath) makePathBuf(bck *cmn.Bck, contentType string, extra int) (buf []byte) {
	var provLen, nsLen, bckNameLen, ctLen int
	if contentType != "" {
		debug.Assert(len(contentType) == contentTypeLen)
		ctLen = 1 + 1 + contentTypeLen
		if bck.Props != nil {
			debug.Assert(bck.Props.BID != 0)
			mi.bpc.RLock()
			bdir, ok := mi.bpc.m[bck.Props.BID]
			mi.bpc.RUnlock()
			if ok {
				buf = make([]byte, 0, len(bdir)+ctLen+extra)
				buf = append(buf, bdir...)
				goto ct
			}
		}
	}
	if !bck.Ns.IsGlobal() {
		nsLen = 1
		if bck.Ns.IsRemote() {
			nsLen += 1 + len(bck.Ns.UUID)
		}
		nsLen += 1 + len(bck.Ns.Name)
	}
	if bck.Name != "" {
		bckNameLen = 1 + len(bck.Name)
	}
	provLen = 1 + 1 + len(bck.Provider)
	buf = make([]byte, 0, len(mi.Path)+provLen+nsLen+bckNameLen+ctLen+extra)
	buf = append(buf, mi.Path...)
	buf = append(buf, filepath.Separator, prefProvider)
	buf = append(buf, bck.Provider...)
	if nsLen > 0 {
		buf = append(buf, filepath.Separator)
		if bck.Ns.IsRemote() {
			buf = append(buf, prefNsUUID)
			buf = append(buf, bck.Ns.UUID...)
		}
		buf = append(buf, prefNsName)
		buf = append(buf, bck.Ns.Name...)
	}
	if bckNameLen > 0 {
		buf = append(buf, filepath.Separator)
		buf = append(buf, bck.Name...)
	}
ct:
	if ctLen > 0 {
		buf = append(buf, filepath.Separator, prefCT)
		buf = append(buf, contentType...)
	}
	return
}

func (mi *Mountpath) MakePathBck(bck *cmn.Bck) string {
	if bck.Props == nil {
		buf := mi.makePathBuf(bck, "", 0)
		return cos.UnsafeS(buf)
	}

	bid := bck.Props.BID
	debug.Assert(bid != 0)
	mi.bpc.RLock()
	dir, ok := mi.bpc.m[bid]
	mi.bpc.RUnlock()
	if ok {
		return dir
	}
	buf := mi.makePathBuf(bck, "", 0)
	dir = cos.UnsafeS(buf)

	mi.bpc.Lock()
	mi.bpc.m[bid] = dir
	mi.bpc.Unlock()
	return dir
}

func (mi *Mountpath) MakePathCT(bck *cmn.Bck, contentType string) string {
	debug.Assert(contentType != "")
	buf := mi.makePathBuf(bck, contentType, 0)
	return cos.UnsafeS(buf)
}

func (mi *Mountpath) MakePathFQN(bck *cmn.Bck, contentType, objName string) string {
	debug.Assert(contentType != "" && objName != "")
	buf := mi.makePathBuf(bck, contentType, 1+len(objName))
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return cos.UnsafeS(buf)
}

func (mi *Mountpath) makeDelPathBck(bck *cmn.Bck, bid uint64) string {
	mi.bpc.Lock()
	dir, ok := mi.bpc.m[bid]
	if ok {
		delete(mi.bpc.m, bid)
	}
	mi.bpc.Unlock()
	debug.Assert(!(ok && bid == 0))
	if !ok {
		dir = mi.MakePathBck(bck)
	}
	return dir
}

// Creates all CT directories for a given (mountpath, bck) - NOTE handling of empty dirs
func (mi *Mountpath) createBckDirs(bck *cmn.Bck, nilbmd bool) (int, error) {
	var num int
	for contentType := range CSM.m {
		dir := mi.MakePathCT(bck, contentType)
		if _, err := os.Stat(dir); err == nil {
			if nilbmd {
				// NOTE: e.g., has been decommissioned without proper cleanup, and rejoined
				glog.Errorf("bucket %s: directory %s already exists but local BMD is nil - skipping...",
					bck, dir)
				num++
				continue
			}
			names, empty, errEmpty := IsDirEmpty(dir)
			if errEmpty != nil {
				return num, errEmpty
			}
			if !empty {
				err = fmt.Errorf("bucket %s: directory %s already exists and is not empty (%v...)",
					bck, dir, names)
				if contentType != WorkfileType {
					return num, err
				}
				glog.Error(err)
			}
		} else if err := cos.CreateDir(dir); err != nil {
			return num, fmt.Errorf("bucket %s: failed to create directory %s: %w", bck, dir, err)
		}
		num++
	}
	return num, nil
}

func (mi *Mountpath) _setDisks(fsdisks ios.FsDisks) {
	mi.Disks = make([]string, len(fsdisks))
	i := 0
	for d := range fsdisks {
		mi.Disks[i] = d
		i++
	}
}

// available/used capacity

func (mi *Mountpath) getCapacity(config *cmn.Config, refresh bool) (c Capacity, err error) {
	if !refresh {
		mi.cmu.RLock()
		c = mi.capacity
		mi.cmu.RUnlock()
		return
	}

	mi.cmu.Lock()
	statfs := &syscall.Statfs_t{}
	if err = syscall.Statfs(mi.Path, statfs); err != nil {
		mi.cmu.Unlock()
		return
	}
	bused := statfs.Blocks - statfs.Bavail
	pct := bused * 100 / statfs.Blocks
	if pct >= uint64(config.Space.HighWM)-1 {
		fpct := math.Ceil(float64(bused) * 100 / float64(statfs.Blocks))
		pct = uint64(fpct)
	}
	mi.capacity.Used = bused * uint64(statfs.Bsize)
	mi.capacity.Avail = statfs.Bavail * uint64(statfs.Bsize)
	mi.capacity.PctUsed = int32(pct)
	c = mi.capacity
	mi.cmu.Unlock()
	return
}

//
// mountpath add/enable helpers - always call under mfs lock
//

func (mi *Mountpath) AddEnabled(tid string, availablePaths MPI, config *cmn.Config) (err error) {
	if err = mi._checkExists(availablePaths); err != nil {
		return
	}
	if err = mi._addEnabled(tid, availablePaths, config); err == nil {
		mfs.fsIDs[mi.FsID] = mi.Path
	}
	cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
	return
}

func (mi *Mountpath) AddDisabled(disabledPaths MPI) {
	cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
	disabledPaths[mi.Path] = mi
	mfs.fsIDs[mi.FsID] = mi.Path
}

// TODO: extend `force=true` to disregard "filesystem sharing" (see AddMpath)
func (mi *Mountpath) _checkExists(availablePaths MPI) (err error) {
	if existingMi, exists := availablePaths[mi.Path]; exists {
		err = fmt.Errorf("failed adding %s: %s already exists", mi, existingMi)
	} else if existingPath, exists := mfs.fsIDs[mi.FsID]; exists && !mfs.allowSharedDisksAndNoDisks {
		err = fmt.Errorf("FSID %v: filesystem sharing is not allowed: %s vs %q", mi.FsID, mi, existingPath)
	} else {
		l := len(mi.Path)
		for mpath := range availablePaths {
			if err = cmn.IsNestedMpath(mi.Path, l, mpath); err != nil {
				break
			}
		}
	}
	return
}

func (mi *Mountpath) _addEnabled(tid string, availablePaths MPI, config *cmn.Config) error {
	disks, err := mfs.ios.AddMpath(mi.Path, mi.Fs, config.TestingEnv())
	if err != nil {
		return err
	}
	if tid != "" && config.WritePolicy.MD != apc.WriteNever {
		if err := mi.SetDaemonIDXattr(tid); err != nil {
			return err
		}
	}
	mi._setDisks(disks)
	_ = mi.String() // assign mi.info if not yet
	availablePaths[mi.Path] = mi
	return nil
}

// under lock: clones and adds self to available
func (mi *Mountpath) _cloneAddEnabled(tid string, config *cmn.Config) (err error) {
	debug.Assert(!mi.IsAnySet(FlagWaitingDD)) // m.b. new
	availablePaths, disabledPaths := Get()
	if _, ok := disabledPaths[mi.Path]; ok {
		return fmt.Errorf("%s exists and is currently disabled (hint: did you want to enable it?)", mi)
	}

	// dd-transition
	if ddmi, ok := availablePaths[mi.Path]; ok && ddmi.IsAnySet(FlagWaitingDD) {
		availableCopy := _cloneOne(availablePaths)
		glog.Warningf("%s (%s): interrupting dd-transition - adding&enabling", mi, ddmi)
		availableCopy[mi.Path] = mi
		putAvailMPI(availableCopy)
		return
	}

	// add new mp
	if err = mi._checkExists(availablePaths); err != nil {
		return
	}
	availableCopy := _cloneOne(availablePaths)
	if err = mi.AddEnabled(tid, availableCopy, config); err == nil {
		putAvailMPI(availableCopy)
	}
	return
}

func (mi *Mountpath) CheckDisks() (err error) {
	if !mfs.allowSharedDisksAndNoDisks && len(mi.Disks) == 0 {
		err = &ErrMountpathNoDisks{Mi: mi}
	}
	return
}

func (mi *Mountpath) ClearDD() {
	cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
}

/////////////////////
// MountedFS & MPI //
/////////////////////

// create a new singleton
func New(num int, allowSharedDisksAndNoDisks bool) {
	if allowSharedDisksAndNoDisks {
		glog.Warningln("allowed disk sharing by multiple mountpaths and mountpaths with no disks")
	}
	mfs = &MountedFS{fsIDs: make(map[cos.FsID]string, 10), allowSharedDisksAndNoDisks: allowSharedDisksAndNoDisks}
	mfs.ios = ios.New(num)
}

// used only in tests
func TestNew(iostater ios.IOS) {
	const num = 10
	mfs = &MountedFS{fsIDs: make(map[cos.FsID]string, num), allowSharedDisksAndNoDisks: false}
	if iostater == nil {
		mfs.ios = ios.New(num)
	} else {
		mfs.ios = iostater
	}
	PutMPI(make(MPI, num), make(MPI, num))
}

func Decommission(mdOnly bool) {
	available, disabled := Get()
	allmpi := []MPI{available, disabled}
	for idx, mpi := range allmpi {
		if mdOnly { // NOTE: removing daemon ID below as well
			for _, mi := range mpi {
				mi.ClearMDs()
			}
		} else {
			// NOTE: the entire content including user data, MDs, and daemon ID
			for _, mi := range mpi {
				if err := os.RemoveAll(mi.Path); err != nil && !os.IsNotExist(err) && idx == 0 {
					// available is [0]
					glog.Errorf("failed to cleanup available %s: %v", mi, err)
				}
			}
		}
	}
	if mdOnly {
		RemoveDaemonIDs()
	}
}

// `ios` delegations
func GetAllMpathUtils() (utils *ios.MpathUtil) { return mfs.ios.GetAllMpathUtils() }
func GetMpathUtil(mpath string) int64          { return mfs.ios.GetMpathUtil(mpath) }
func FillDiskStats(m ios.AllDiskStats)         { mfs.ios.FillDiskStats(m) }

// TestDisableValidation disables fsid checking and allows mountpaths without disks (testing-only)
func TestDisableValidation() { mfs.allowSharedDisksAndNoDisks = true }

// Returns number of available mountpaths
func NumAvail() int {
	availablePaths := (*MPI)(mfs.available.Load())
	return len(*availablePaths)
}

func putAvailMPI(available MPI) { mfs.available.Store(unsafe.Pointer(&available)) }
func putDisabMPI(disabled MPI)  { mfs.disabled.Store(unsafe.Pointer(&disabled)) }

func PutMPI(available, disabled MPI) {
	putAvailMPI(available)
	putDisabMPI(disabled)
}

func MountpathsToLists() (mpl *apc.MountpathList) {
	availablePaths, disabledPaths := Get()
	mpl = &apc.MountpathList{
		Available: make([]string, 0, len(availablePaths)),
		WaitingDD: make([]string, 0),
		Disabled:  make([]string, 0, len(disabledPaths)),
	}
	for _, mi := range availablePaths {
		if mi.IsAnySet(FlagWaitingDD) {
			mpl.WaitingDD = append(mpl.WaitingDD, mi.Path)
		} else {
			mpl.Available = append(mpl.Available, mi.Path)
		}
	}
	for mpath := range disabledPaths {
		mpl.Disabled = append(mpl.Disabled, mpath)
	}
	sort.Strings(mpl.Available)
	sort.Strings(mpl.WaitingDD)
	sort.Strings(mpl.Disabled)
	return
}

// NOTE: must be under mfs lock
func _cloneOne(mpis MPI) (clone MPI) {
	clone = make(MPI, len(mpis))
	for mpath, mi := range mpis {
		clone[mpath] = mi
	}
	return
}

// cloneMPI returns a shallow copy of the current (available, disabled) mountpaths
func cloneMPI() (availableCopy, disabledCopy MPI) {
	availablePaths, disabledPaths := Get()
	availableCopy = _cloneOne(availablePaths)
	disabledCopy = _cloneOne(disabledPaths)
	return availableCopy, disabledCopy
}

// (used only in _unit_ tests - compare with AddMpath below)
func Add(mpath, tid string) (mi *Mountpath, err error) {
	mi, err = NewMountpath(mpath)
	if err != nil {
		return
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	mfs.mu.Unlock()
	return
}

// Add adds new mountpath to the target's `availablePaths`
// TODO: extend `force=true` to disregard "filesystem sharing"
func AddMpath(mpath, tid string, cb func(), force bool) (mi *Mountpath, err error) {
	debug.Assert(tid != "")
	mi, err = NewMountpath(mpath)
	if err != nil {
		return
	}
	config := cmn.GCO.Get()
	if config.TestingEnv() {
		if err = config.LocalConfig.TestFSP.ValidateMpath(mi.Path); err != nil {
			if !force {
				return
			}
			glog.Errorf("%v - ignoring since force=%t", err, force)
			err = nil
		}
	}
	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	if err == nil {
		err = mi.CheckDisks()
	}
	if err == nil {
		cb()
	}
	mfs.mu.Unlock()

	if mi.Path != mpath {
		glog.Warningf("%s: cleanpath(%q) => %q", mi, mpath, mi.Path)
	}
	return
}

// (used only in tests - compare with EnableMpath below)
func Enable(mpath string) (enabledMpath *Mountpath, err error) {
	var cleanMpath string
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	enabledMpath, err = enable(mpath, cleanMpath, "" /*tid*/, config)
	mfs.mu.Unlock()
	return
}

// Enable enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func EnableMpath(mpath, tid string, cb func()) (enabledMpath *Mountpath, err error) {
	var cleanMpath string
	debug.Assert(tid != "")
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	enabledMpath, err = enable(mpath, cleanMpath, tid, config)
	if err == nil {
		cb()
	}
	mfs.mu.Unlock()
	return
}

func enable(mpath, cleanMpath, tid string, config *cmn.Config) (enabledMpath *Mountpath, err error) {
	availablePaths, disabledPaths := Get()
	mi, ok := availablePaths[cleanMpath]

	// dd-transition
	if ok {
		debug.Assert(cleanMpath == mi.Path)
		if _, ok = disabledPaths[cleanMpath]; ok {
			err = fmt.Errorf("FATAL: %s vs (%s, %s)", mi, availablePaths, disabledPaths)
			glog.Error(err)
			debug.AssertNoErr(err)
			return
		}
		if mi.IsAnySet(FlagWaitingDD) {
			availableCopy := _cloneOne(availablePaths)
			mi, ok = availableCopy[cleanMpath]
			debug.Assert(ok)
			glog.Warningf("%s: re-enabling during dd-transition", mi)
			cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
			enabledMpath = mi
			putAvailMPI(availableCopy)
		} else if glog.FastV(4, glog.SmoduleFS) {
			glog.Infof("%s: %s is already available, nothing to do", tid, mi)
		}
		return
	}

	// re-enable
	mi, ok = disabledPaths[cleanMpath]
	if !ok {
		err = cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
		return
	}
	debug.Assert(cleanMpath == mi.Path)
	availableCopy, disabledCopy := cloneMPI()
	mi, ok = disabledCopy[cleanMpath]
	debug.Assert(ok)
	if err = mi.AddEnabled(tid, availableCopy, config); err != nil {
		return
	}
	enabledMpath = mi
	delete(disabledCopy, cleanMpath)
	PutMPI(availableCopy, disabledCopy)
	return
}

// Remove removes mountpaths from the target's mountpaths. It searches
// for the mountpath in `available` and, if not found, in `disabled`.
func Remove(mpath string, cb ...func()) (*Mountpath, error) {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}

	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	// Clear target ID if set
	if err := removeXattr(cleanMpath, nodeXattrID); err != nil {
		return nil, err
	}
	availablePaths, disabledPaths := Get()
	mi, exists := availablePaths[cleanMpath]
	if !exists {
		if mi, exists = disabledPaths[cleanMpath]; !exists {
			return nil, cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
		}
		debug.Assert(cleanMpath == mi.Path)
		disabledCopy := _cloneOne(disabledPaths)
		delete(disabledCopy, cleanMpath)
		delete(mfs.fsIDs, mi.FsID) // optional, benign
		putDisabMPI(disabledCopy)
		return mi, nil
	}
	debug.Assert(cleanMpath == mi.Path)

	if _, exists = disabledPaths[cleanMpath]; exists {
		err := fmt.Errorf("FATAL: %s vs (%s, %s)", mi, availablePaths, disabledPaths)
		glog.Error(err)
		debug.AssertNoErr(err)
		return nil, err
	}

	config := cmn.GCO.Get()
	availableCopy := _cloneOne(availablePaths)
	mfs.ios.RemoveMpath(cleanMpath, config.TestingEnv())
	delete(availableCopy, cleanMpath)
	delete(mfs.fsIDs, mi.FsID)

	availCnt := len(availableCopy)
	if availCnt == 0 {
		glog.Errorf("removed the last available mountpath %s", mi)
	} else {
		glog.Infof("removed mountpath %s (remain available: %d)", mi, availCnt)
	}
	moveMarkers(availableCopy, mi)
	putAvailMPI(availableCopy)
	if availCnt > 0 && len(cb) > 0 {
		cb[0]()
	}
	return mi, nil
}

// begin (disable | detach) transaction: CoW-mark the corresponding mountpath
func BeginDD(action string, flags uint64, mpath string) (mi *Mountpath, numAvail int, err error) {
	var (
		cleanMpath string
		exists     bool
	)
	debug.Assert(cos.BitFlags(flags).IsAnySet(cos.BitFlags(FlagWaitingDD)))
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	availablePaths, disabledPaths := Get()
	if mi, exists = availablePaths[cleanMpath]; !exists {
		if mi, exists = disabledPaths[cleanMpath]; !exists {
			err = cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
			return
		}
		glog.Infof("%s(%q) is already fully disabled - nothing to do", mi, action)
		mi = nil
		numAvail = len(availablePaths)
		return
	}

	availableCopy := _cloneOne(availablePaths)
	mi = availableCopy[cleanMpath]
	ok := mi.setFlags(flags)
	debug.Assert(ok, mi.String()) // NOTE: under lock
	putAvailMPI(availableCopy)
	numAvail = len(availableCopy) - 1
	return
}

// Disables a mountpath, i.e., removes it from usage but keeps in the volume
// (for possible future re-enablement). If successful, returns the disabled mountpath.
// Otherwise, returns nil (also in the case if the mountpath was already disabled).
func Disable(mpath string, cb ...func()) (disabledMpath *Mountpath, err error) {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}

	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	availablePaths, disabledPaths := Get()
	if mi, ok := availablePaths[cleanMpath]; ok {
		debug.Assert(cleanMpath == mi.Path)
		if _, ok = disabledPaths[cleanMpath]; ok {
			err = fmt.Errorf("FATAL: %s vs (%s, %s)", mi, availablePaths, disabledPaths)
			glog.Error(err)
			debug.AssertNoErr(err)
			return
		}
		availableCopy, disabledCopy := cloneMPI()
		cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
		disabledCopy[cleanMpath] = mi

		config := cmn.GCO.Get()
		mfs.ios.RemoveMpath(cleanMpath, config.TestingEnv())
		delete(availableCopy, cleanMpath)
		delete(mfs.fsIDs, mi.FsID)
		moveMarkers(availableCopy, mi)
		PutMPI(availableCopy, disabledCopy)
		if l := len(availableCopy); l == 0 {
			glog.Errorf("disabled the last available mountpath %s", mi)
		} else {
			if len(cb) > 0 {
				cb[0]()
			}
			glog.Infof("disabled mountpath %s (%d remain%s active)", mi, l, cos.Plural(l))
		}
		return mi, nil
	}

	if _, ok := disabledPaths[cleanMpath]; ok {
		return nil, nil // nothing to do
	}
	return nil, cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
}

// returns both available and disabled mountpaths (compare with GetAvail)
func Get() (MPI, MPI) {
	var (
		availablePaths = (*MPI)(mfs.available.Load())
		disabledPaths  = (*MPI)(mfs.disabled.Load())
	)
	debug.Assert(availablePaths != nil)
	debug.Assert(disabledPaths != nil)
	return *availablePaths, *disabledPaths
}

func GetAvail() MPI {
	availablePaths := (*MPI)(mfs.available.Load())
	debug.Assert(availablePaths != nil)
	return *availablePaths
}

func CreateBucket(bck *cmn.Bck, nilbmd bool) (errs []error) {
	var (
		availablePaths   = GetAvail()
		totalDirs        = len(availablePaths) * len(CSM.m)
		totalCreatedDirs int
	)
	for _, mi := range availablePaths {
		num, err := mi.createBckDirs(bck, nilbmd)
		if err != nil {
			errs = append(errs, err)
		} else {
			totalCreatedDirs += num
		}
	}
	debug.Assert(totalCreatedDirs == totalDirs || errs != nil)
	return
}

func DestroyBucket(op string, bck *cmn.Bck, bid uint64) (err error) {
	var (
		n              int
		availablePaths = GetAvail()
		count          = len(availablePaths)
	)
	for _, mi := range availablePaths {
		mi.evictLomBucketCache(bck)
		dir := mi.makeDelPathBck(bck, bid)
		if errMv := mi.MoveToDeleted(dir); errMv != nil {
			glog.Errorf("%s %q: failed to rm dir %q: %v", op, bck, dir, errMv)
			// TODO: call fshc
		} else {
			n++
		}
	}
	if n < count {
		err = fmt.Errorf("%s %q: failed to destroy %d out of %d dirs", op, bck, count-n, count)
	}
	return
}

func RenameBucketDirs(bidFrom uint64, bckFrom, bckTo *cmn.Bck) (err error) {
	availablePaths := GetAvail()
	renamed := make([]*Mountpath, 0, len(availablePaths))
	for _, mi := range availablePaths {
		fromPath := mi.makeDelPathBck(bckFrom, bidFrom)
		toPath := mi.MakePathBck(bckTo)
		// os.Rename fails when renaming to a directory which already exists.
		// We should remove destination bucket directory before rename. It's reasonable to do so
		// as all targets agreed to rename and rename was committed in BMD.
		os.RemoveAll(toPath)
		if err = os.Rename(fromPath, toPath); err != nil {
			break
		}
		renamed = append(renamed, mi)
	}

	if err == nil {
		return
	}
	for _, mi := range renamed {
		fromPath := mi.MakePathBck(bckTo)
		toPath := mi.MakePathBck(bckFrom)
		if erd := os.Rename(fromPath, toPath); erd != nil {
			glog.Error(erd)
		}
	}
	return
}

func moveMarkers(available MPI, from *Mountpath) {
	var (
		fromPath    = filepath.Join(from.Path, fname.MarkersDir)
		finfos, err = os.ReadDir(fromPath)
	)
	if err != nil {
		if !os.IsNotExist(err) {
			glog.Errorf("Failed to read markers directory %q: %v", fromPath, err)
		}
		return
	}
	if len(finfos) == 0 {
		return // no markers, nothing to do
	}

	// NOTE: `from` path must no longer be in the available mountpaths
	_, ok := available[from.Path]
	debug.Assert(!ok, from.String())
	for _, mi := range available {
		ok = true
		for _, fi := range finfos {
			debug.Assert(!fi.IsDir(), fname.MarkersDir+"/"+fi.Name()) // marker is file
			var (
				fromPath = filepath.Join(from.Path, fname.MarkersDir, fi.Name())
				toPath   = filepath.Join(mi.Path, fname.MarkersDir, fi.Name())
			)
			_, _, err := cos.CopyFile(fromPath, toPath, nil, cos.ChecksumNone)
			if err != nil && os.IsNotExist(err) {
				glog.Errorf("Failed to move marker %q to %q: %v)", fromPath, toPath, err)
				ok = false
			}
		}
		if ok {
			break
		}
	}
	from.ClearMDs()
}

// load node ID

// traverses all mountpaths to load and validate node ID
func LoadNodeID(mpaths cos.StrSet) (mDaeID string, err error) {
	for mp := range mpaths {
		daeID, err := _loadXattrID(mp)
		if err != nil {
			return "", err
		}
		if daeID == "" {
			continue
		}
		if mDaeID != "" {
			if mDaeID != daeID {
				return "", &ErrStorageIntegrity{
					Code: SieMpathIDMismatch,
					Msg:  fmt.Sprintf("target ID mismatch: %q vs %q(%q)", mDaeID, daeID, mp),
				}
			}
			continue
		}
		mDaeID = daeID
	}
	return
}

func _loadXattrID(mpath string) (daeID string, err error) {
	b, err := GetXattr(mpath, nodeXattrID)
	if err == nil {
		daeID = string(b)
		return
	}
	if cos.IsErrXattrNotFound(err) {
		err = nil
	}
	return
}

/////////
// MPI //
/////////

func (mpi MPI) String() string {
	return fmt.Sprintf("%v", mpi.toSlice())
}

func (mpi MPI) toSlice() []string {
	var (
		paths = make([]string, len(mpi))
		idx   int
	)
	for key := range mpi {
		paths[idx] = key
		idx++
	}
	return paths
}

//
// capacity management
//

func Cap() (cs CapStatus) {
	mfs.cmu.RLock()
	cs = mfs.cs
	mfs.cmu.RUnlock()
	return
}

func CapRefresh(config *cmn.Config, tcdf *TargetCDF) (cs CapStatus, err error) {
	var (
		errmsg         string
		c              Capacity
		availablePaths = GetAvail()
	)
	if len(availablePaths) == 0 {
		err = cmn.ErrNoMountpaths
		return
	}
	if config == nil {
		config = cmn.GCO.Get()
	}
	high, oos := config.Space.HighWM, config.Space.OOS
	for path, mi := range availablePaths {
		if c, err = mi.getCapacity(config, true); err != nil {
			glog.Errorf("%s: %v", mi, err)
			return
		}
		cs.TotalUsed += c.Used
		cs.TotalAvail += c.Avail
		cs.PctMax = cos.MaxI32(cs.PctMax, c.PctUsed)
		cs.PctAvg += c.PctUsed
		if tcdf == nil {
			continue
		}
		cdf := tcdf.Mountpaths[path]
		if cdf == nil {
			tcdf.Mountpaths[path] = &CDF{}
			cdf = tcdf.Mountpaths[path]
		}
		cdf.Capacity = c
		cdf.Disks = mi.Disks
		cdf.FS = mi.FS.String()
	}
	cs.PctAvg /= int32(len(availablePaths))
	cs.OOS = int64(cs.PctMax) > oos
	if cs.OOS || int64(cs.PctMax) > high {
		cs.Err = cmn.NewErrCapacityExceeded(high, cs.TotalUsed, cs.TotalAvail+cs.TotalUsed, cs.PctMax, cs.OOS)
		errmsg = cs.Err.Error()
	}
	if tcdf != nil {
		tcdf.PctMax, tcdf.PctAvg = cs.PctMax, cs.PctAvg
		tcdf.CsErr = errmsg
	}
	// cached cap state
	mfs.cmu.Lock()
	mfs.cs = cs
	mfs.cmu.Unlock()
	return
}

// NOTE: Is called only and exclusively by `stats.Trunner` providing
//
//	`config.Periodic.StatsTime` tick.
func CapPeriodic(now int64, config *cmn.Config, tcdf *TargetCDF) (cs CapStatus, updated bool, err error) {
	if now < mfs.csExpires.Load() {
		return
	}
	cs, err = CapRefresh(config, tcdf)
	updated = err == nil
	mfs.csExpires.Store(now + int64(cs._next(config)))
	return
}

func CapStatusGetWhat() (fsInfo apc.CapacityInfo) {
	cs := Cap()
	fsInfo.Used = cs.TotalUsed
	fsInfo.Total = cs.TotalUsed + cs.TotalAvail
	fsInfo.PctUsed = float64(cs.PctAvg)
	return
}

///////////////
// CapStatus //
///////////////

func (cs *CapStatus) IsNil() bool { return cs.TotalUsed == 0 && cs.TotalAvail == 0 }

func (cs *CapStatus) String() (str string) {
	var (
		totalUsed  = cos.ToSizeIEC(int64(cs.TotalUsed), 1)
		totalAvail = cos.ToSizeIEC(int64(cs.TotalAvail), 1)
	)
	str = fmt.Sprintf("cap(used=%s, avail=%s, avg-use=%d%%, max-use=%d%%", totalUsed, totalAvail, cs.PctAvg, cs.PctMax)
	if cs.Err != nil {
		if cs.OOS {
			str += ", OOS"
		} else {
			str += ", err=" + cs.Err.Error()
		}
	}
	str += ")"
	return
}

// next time to CapRefresh()
func (cs *CapStatus) _next(config *cmn.Config) time.Duration {
	var (
		util = int64(cs.PctMax)
		umin = cos.MinI64(config.Space.HighWM-10, config.Space.LowWM)
		umax = config.Space.OOS
		tmax = config.LRU.CapacityUpdTime.D()
		tmin = config.Periodic.StatsTime.D()
	)
	umin = cos.MinI64(umin, config.Space.CleanupWM)
	if util <= umin {
		return tmax
	}
	if util >= umax-1 {
		return tmin
	}
	ratio := (util - umin) * 100 / (umax - umin)
	return time.Duration(100-ratio)*(tmax-tmin)/100 + tmin
}
