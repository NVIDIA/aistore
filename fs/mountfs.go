// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ios"
	"github.com/OneOfOne/xxhash"
)

const (
	uQuantum = 10 // each GET adds a "quantum" of utilization to the mountpath

	TrashDir      = "$trash"
	daemonIDXattr = "user.ais.daemon_id"

	siePrefix = "storage integrity error: sie#"
)

const (
	siMpathIDMismatch = (1 + iota) * 10
	siTargetIDMismatch
	siMetaMismatch
	siMetaCorrupted
	siMpathMissing
)

// globals
var (
	mfs      *MountedFS
	mpathsRR sync.Map
)

// Terminology:
// - a mountpath is equivalent to (configurable) fspath - both terms are used interchangeably;
// - each mountpath is, simply, a local directory that is serviced by a local filesystem;
// - there's a 1-to-1 relationship between a mountpath and a local filesystem
//   (different mountpaths map onto different filesystems, and vise versa);
// - mountpaths of the form <filesystem-mountpoint>/a/b/c are supported.

type (
	MountpathInfo struct {
		Path     string // Cleaned OrigPath
		OrigPath string // As entered by the user, must be used for logging / returning errors
		Fsid     syscall.Fsid

		FileSystem string
		PathDigest uint64

		// LOM caches
		lomCaches cmn.MultiSyncMap

		// bucket path cache
		bpc struct {
			sync.RWMutex
			m map[uint64]string
		}
		// capacity
		cmu      sync.RWMutex
		capacity Capacity
	}
	MPI map[string]*MountpathInfo

	Capacity struct {
		Used    uint64 `json:"used,string"`  // bytes
		Avail   uint64 `json:"avail,string"` // ditto
		PctUsed int32  `json:"pct_used"`     // %% used (redundant ok)
	}
	MPCap map[string]Capacity // [mpath => Capacity]

	// MountedFS holds all mountpaths for the target.
	MountedFS struct {
		mu sync.RWMutex
		// fsIDs is set in which we store fsids of mountpaths. This allows for
		// determining if there are any duplications of file system - we allow
		// only one mountpath per file system.
		fsIDs map[syscall.Fsid]string
		// checkFsID determines if we should actually check FSID when adding new
		// mountpath. By default it is set to true.
		checkFsID bool
		// Available mountpaths - mountpaths which are used to store the data.
		available atomic.Pointer
		// Disabled mountpaths - mountpaths which for some reason did not pass
		// the health check and cannot be used for a moment.
		disabled atomic.Pointer
		// Iostats for the available mountpaths
		ios ios.IOStater

		// capacity
		cmu     sync.RWMutex
		capTime struct {
			curr, next int64
		}
		capStatus CapStatus
	}
	CapStatus struct {
		TotalUsed  uint64 // bytes
		TotalAvail uint64 // bytes
		PctAvg     int32  // used average (%)
		PctMax     int32  // max used (%)
		Err        error
		OOS        bool
	}

	// errors
	StorageIntegrityError struct {
		msg  string
		code int
	}
)

///////////////////
// MountpathInfo //
///////////////////

func newMountpath(cleanPath, origPath string, fsid syscall.Fsid, fs string) *MountpathInfo {
	mi := &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   origPath,
		Fsid:       fsid,
		FileSystem: fs,
		PathDigest: xxhash.ChecksumString64S(cleanPath, cmn.MLCG32),
	}
	mi.bpc.m = make(map[uint64]string, 16)
	return mi
}

func (mi *MountpathInfo) String() string {
	return fmt.Sprintf("mp[%s, fs=%s]", mi.Path, mi.FileSystem)
}

func (mi *MountpathInfo) LomCache(idx int) *sync.Map { return mi.lomCaches.Get(idx) }

func (mi *MountpathInfo) EvictLomCache() {
	for idx := 0; idx < cmn.MultiSyncMapCount; idx++ {
		cache := mi.LomCache(idx)
		cache.Range(func(key interface{}, _ interface{}) bool {
			cache.Delete(key)
			return true
		})
	}
}

func (mi *MountpathInfo) MakePathTrash() string { return filepath.Join(mi.Path, TrashDir) }

// MoveToTrash removes directory in steps:
// 1. Synchronously gets temporary directory name
// 2. Synchronously renames old folder to temporary directory
func (mi *MountpathInfo) MoveToTrash(dir string) error {
	// Loose assumption: removing something which doesn't exist is fine.
	if err := Access(dir); err != nil && os.IsNotExist(err) {
		return nil
	}
Retry:
	var (
		trashDir = mi.MakePathTrash()
		tmpDir   = filepath.Join(trashDir, fmt.Sprintf("$dir-%d", mono.NanoTime()))
	)
	if err := cmn.CreateDir(trashDir); err != nil {
		return err
	}
	if err := os.Rename(dir, tmpDir); err != nil {
		if os.IsExist(err) {
			// Slow path: `tmpDir` already exists so let's retry. It should
			// never happen but who knows...
			glog.Warningf("directory %q already exist in trash", tmpDir)
			goto Retry
		}
		if os.IsNotExist(err) {
			// Someone removed `dir` before `os.Rename`, nothing more to do.
			return nil
		}
		if err != nil {
			return err
		}
	}
	// TODO: remove and make it work when the space is extremely constrained (J)
	debug.Func(func() {
		go func() {
			if err := os.RemoveAll(tmpDir); err != nil {
				glog.Errorf("RemoveAll for %q failed with %v", tmpDir, err)
			}
		}()
	})
	return nil
}

func (mi *MountpathInfo) IsIdle(config *cmn.Config) bool {
	curr := mfs.ios.GetMpathUtil(mi.Path)
	return curr >= 0 && curr < config.Disk.DiskUtilLowWM
}

func (mi *MountpathInfo) CreateMissingBckDirs(bck cmn.Bck) (err error) {
	for contentType := range CSM.RegisteredContentTypes {
		dir := mi.MakePathCT(bck, contentType)
		if err = Access(dir); err == nil {
			continue
		}
		if err = cmn.CreateDir(dir); err != nil {
			return
		}
	}
	return
}

func (mi *MountpathInfo) StoreMD(path string, what interface{}, options jsp.Options) error {
	fpath := filepath.Join(mi.Path, path)
	if what == nil {
		file, err := cmn.CreateFile(fpath)
		file.Close()
		return err
	}
	return jsp.Save(fpath, what, options)
}

func (mi *MountpathInfo) ClearMDs() {
	for _, mdPath := range mdFilesDirs {
		mi.Remove(mdPath)
	}
}

func (mi *MountpathInfo) Remove(path string) error {
	fpath := filepath.Join(mi.Path, path)
	if err := os.RemoveAll(fpath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (mi *MountpathInfo) MoveMD(from, to string) bool {
	var (
		fromPath = filepath.Join(mi.Path, from)
		toPath   = filepath.Join(mi.Path, to)
	)
	err := os.Rename(fromPath, toPath)
	if err != nil && !os.IsNotExist(err) {
		glog.Error(err)
	}
	return err == nil
}

func (mi *MountpathInfo) SetDaemonIDXattr(daeID string) error {
	cmn.Assert(daeID != "")
	// Validate if mountpath already has daemon ID set.
	mpathDaeID, err := LoadDaemonIDXattr(mi.Path)
	if err != nil {
		return err
	}
	if mpathDaeID == daeID {
		return nil
	}
	if mpathDaeID != "" && mpathDaeID != daeID {
		return newMpathIDMismatchErr(daeID, mpathDaeID, mi.Path)
	}
	return SetXattr(mi.Path, daemonIDXattr, []byte(daeID))
}

// make-path methods

func (mi *MountpathInfo) makePathBuf(bck cmn.Bck, contentType string, extra int) (buf []byte) {
	var provLen, nsLen, bckNameLen, ctLen int
	if contentType != "" {
		debug.Assert(len(contentType) == contentTypeLen)
		ctLen = 1 + 1 + contentTypeLen
		if bck.Props != nil && bck.Props.BID != 0 {
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

func (mi *MountpathInfo) MakePathBck(bck cmn.Bck) string {
	if bck.Props == nil {
		buf := mi.makePathBuf(bck, "", 0)
		return *(*string)(unsafe.Pointer(&buf))
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
	dir = *(*string)(unsafe.Pointer(&buf))

	mi.bpc.Lock()
	mi.bpc.m[bid] = dir
	mi.bpc.Unlock()
	return dir
}

func (mi *MountpathInfo) MakePathCT(bck cmn.Bck, contentType string) string {
	debug.AssertFunc(bck.Valid, bck)
	debug.Assert(contentType != "")
	buf := mi.makePathBuf(bck, contentType, 0)
	return *(*string)(unsafe.Pointer(&buf))
}

func (mi *MountpathInfo) MakePathFQN(bck cmn.Bck, contentType, objName string) string {
	debug.AssertFunc(bck.Valid, bck)
	debug.Assert(contentType != "" && objName != "")
	buf := mi.makePathBuf(bck, contentType, 1+len(objName))
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return *(*string)(unsafe.Pointer(&buf))
}

func (mi *MountpathInfo) makeDelPathBck(bck cmn.Bck, bid uint64) string {
	mi.bpc.Lock()
	dir, ok := mi.bpc.m[bid]
	if ok {
		delete(mi.bpc.m, bid)
	}
	mi.bpc.Unlock()
	if !ok {
		dir = mi.MakePathBck(bck)
	}
	return dir
}

// Creates all CT directories for a given (mountpath, bck) - NOTE handling of empty dirs
func (mi *MountpathInfo) createBckDirs(bck cmn.Bck) (num int, err error) {
	for contentType := range CSM.RegisteredContentTypes {
		dir := mi.MakePathCT(bck, contentType)
		if err := Access(dir); err == nil {
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
				glog.Warning(err)
			}
		} else if err := cmn.CreateDir(dir); err != nil {
			return num, fmt.Errorf("bucket %s: failed to create directory %s: %w", bck, dir, err)
		}
		num++
	}
	return num, nil
}

// available/used capacity

func (mi *MountpathInfo) getCapacity(config *cmn.Config, refresh bool) (c Capacity, err error) {
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
	if pct >= uint64(config.LRU.HighWM)-1 {
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

///////////////
// MountedFS //
///////////////

// create a new singleton
func Init(iostater ...ios.IOStater) {
	mfs = &MountedFS{fsIDs: make(map[syscall.Fsid]string, 10), checkFsID: true}
	if len(iostater) > 0 {
		mfs.ios = iostater[0]
	} else {
		mfs.ios = ios.NewIostatContext()
	}
}

// InitMpaths prepares, validates, and adds configured mountpaths.
func InitMpaths(daeID string) error {
	var (
		config      = cmn.GCO.Get()
		configPaths = config.FSpaths.Paths
		vmd, err    = LoadVMD(configPaths)
		changed     bool
	)

	if len(configPaths) == 0 {
		// (usability) not to clutter the log with backtraces when starting up and validating config
		return fmt.Errorf("FATAL: no fspaths - see README => Configuration and/or fspaths section in the config.sh")
	}

	if err != nil {
		return err
	}

	if vmd == nil {
		for path := range configPaths {
			if _, err := Add(path, daeID); err != nil {
				return err
			}
		}
		_, err = CreateNewVMD(daeID)
		return err
	}

	if vmd.DaemonID != daeID {
		return newVMDIDMismatchErr(vmd.DaemonID, daeID)
	}

	// Validate VMD with config FS
	for path := range configPaths {
		// 1. Check if path is present in VMD paths.
		device, exists := vmd.Devices[path]
		if !exists {
			changed = true
			glog.Error(newVMDMissingMpathErr(path))
		}

		enabled := device == nil || device.Enabled
		if _, err := add(path, daeID, enabled); err != nil {
			return err
		}
	}

	if len(vmd.Devices) > len(configPaths) {
		for device := range vmd.Devices {
			if !configPaths.Contains(device) {
				changed = true
				glog.Error(newConfigMissingMpathErr(device))
			}
		}
	}

	if changed {
		_, err = CreateNewVMD(daeID)
	}
	return err
}

func LoadBalanceGET(objFQN, objMpath string, copies MPI) (fqn string) {
	fqn = objFQN
	var (
		mpathUtils = GetAllMpathUtils()
		minUtil    = mpathUtils.Util(objMpath)
		v, _       = mpathsRR.LoadOrStore(objMpath, atomic.NewInt32(0))
		minCounter = v.(*atomic.Int32)
	)
	for copyFQN, copyMPI := range copies {
		if copyFQN == objFQN {
			continue
		}
		var (
			mpathUtil    = mpathUtils.Util(copyMPI.Path)
			v, _         = mpathsRR.LoadOrStore(copyMPI.Path, atomic.NewInt32(0))
			mpathCounter = v.(*atomic.Int32)
		)
		if mpathUtil < minUtil && mpathCounter.Load() <= minCounter.Load() {
			fqn, minUtil, minCounter = copyFQN, mpathUtil, mpathCounter
			continue
		}
		// NOTE: `uQuantum` heuristics
		if mpathUtil+int64(mpathCounter.Load())*uQuantum < minUtil+int64(minCounter.Load())*uQuantum {
			fqn, minUtil, minCounter = copyFQN, mpathUtil, mpathCounter
		}
	}
	// NOTE: The counter could've been already incremented - ignoring for now
	minCounter.Inc()
	return
}

//////////////////////////////
// `ios` package delegators //
//////////////////////////////

func GetAllMpathUtils() (utils *ios.MpathsUtils) { return mfs.ios.GetAllMpathUtils() }
func GetMpathUtil(mpath string) int64            { return mfs.ios.GetMpathUtil(mpath) }
func LogAppend(lines []string) []string          { return mfs.ios.LogAppend(lines) }
func GetSelectedDiskStats() (m map[string]*ios.SelectedDiskStats) {
	return mfs.ios.GetSelectedDiskStats()
}

// DisableFsIDCheck disables fsid checking when adding new mountpath
func DisableFsIDCheck() { mfs.checkFsID = false }

// Returns number of available mountpaths
func NumAvail() int {
	availablePaths := (*MPI)(mfs.available.Load())
	return len(*availablePaths)
}

func updatePaths(available, disabled MPI) {
	mfs.available.Store(unsafe.Pointer(&available))
	mfs.disabled.Store(unsafe.Pointer(&disabled))
}

// Add adds new mountpath to the target's mountpaths.
// FIXME: unify error messages for original and clean mountpath
func Add(mpath, daeID string) (*MountpathInfo, error) {
	return add(mpath, daeID, true)
}

// helper for adding an enabled/disabled mountpath to mfs
func add(mpath, daeID string, enabled bool) (*MountpathInfo, error) {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}
	if err := Access(cleanMpath); err != nil {
		return nil, fmt.Errorf("fspath %q %s, err: %v", mpath, cmn.DoesNotExist, err)
	}
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(cleanMpath, &statfs); err != nil {
		return nil, fmt.Errorf("cannot statfs fspath %q, err: %w", mpath, err)
	}

	fs, err := fqn2fsAtStartup(cleanMpath)
	if err != nil {
		return nil, fmt.Errorf("cannot get filesystem: %v", err)
	}

	mp := newMountpath(cleanMpath, mpath, statfs.Fsid, fs)
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	availablePaths, disabledPaths := mountpathsCopy()
	if _, exists := availablePaths[mp.Path]; exists {
		return nil, fmt.Errorf("tried to add already registered mountpath: %v", mp.Path)
	}

	if existingPath, exists := mfs.fsIDs[mp.Fsid]; exists && mfs.checkFsID {
		return nil, fmt.Errorf("tried to add path %v but same fsid (%v) was already registered by %v",
			mpath, mp.Fsid, existingPath)
	}

	if enabled {
		mfs.ios.AddMpath(mp.Path, mp.FileSystem)
		if err := mp.SetDaemonIDXattr(daeID); err != nil {
			return nil, err
		}
		availablePaths[mp.Path] = mp
	} else {
		disabledPaths[mp.Path] = mp
	}

	mfs.fsIDs[mp.Fsid] = cleanMpath
	updatePaths(availablePaths, disabledPaths)
	return mp, nil
}

func SetDaemonIDXattrAllMpaths(daeID string) (err error) {
	available, disabled := Get()
	for _, mPath := range available {
		if err = mPath.SetDaemonIDXattr(daeID); err != nil {
			return
		}
	}

	for _, mPath := range disabled {
		if err = mPath.SetDaemonIDXattr(daeID); err != nil {
			if _, ok := err.(*os.SyscallError); ok {
				glog.Error(err)
				continue
			}
			return
		}
	}
	return
}

// mountpathsCopy returns a shallow copy of current mountpaths
func mountpathsCopy() (MPI, MPI) {
	availablePaths, disabledPaths := Get()
	availableCopy := make(MPI, len(availablePaths))
	disabledCopy := make(MPI, len(availablePaths))

	for mpath, mpathInfo := range availablePaths {
		availableCopy[mpath] = mpathInfo
	}
	for mpath, mpathInfo := range disabledPaths {
		disabledCopy[mpath] = mpathInfo
	}
	return availableCopy, disabledCopy
}

// Remove removes mountpaths from the target's mountpaths. It searches
// for the mountpath in `available` and, if not found, in `disabled`.
func Remove(mpath string) (*MountpathInfo, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}

	// Clear daemonID xattr if set
	RemoveXattr(mpath, daemonIDXattr)

	var (
		exists    bool
		mpathInfo *MountpathInfo

		availablePaths, disabledPaths = mountpathsCopy()
	)
	if mpathInfo, exists = availablePaths[cleanMpath]; !exists {
		if mpathInfo, exists = disabledPaths[cleanMpath]; !exists {
			return nil, fmt.Errorf("tried to remove non-existing mountpath: %v", mpath)
		}

		delete(disabledPaths, cleanMpath)
		delete(mfs.fsIDs, mpathInfo.Fsid)
		updatePaths(availablePaths, disabledPaths)
		return mpathInfo, nil
	}

	mfs.ios.RemoveMpath(cleanMpath)
	delete(availablePaths, cleanMpath)
	delete(mfs.fsIDs, mpathInfo.Fsid)

	if availCnt := len(availablePaths); availCnt == 0 {
		glog.Errorf("removed the last available mountpath %s", mpathInfo)
	} else {
		glog.Infof("removed mountpath %s (%d remain(s) active)", mpathInfo, availCnt)
	}

	moveMarkers(availablePaths, mpathInfo)
	updatePaths(availablePaths, disabledPaths)
	return mpathInfo, nil
}

// Enable enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func Enable(mpath string) (enabledMpath *MountpathInfo, err error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}
	availablePaths, disabledPaths := mountpathsCopy()
	if _, ok := availablePaths[cleanMpath]; ok {
		return nil, nil
	}
	if mp, ok := disabledPaths[cleanMpath]; ok {
		availablePaths[cleanMpath] = mp
		mfs.ios.AddMpath(cleanMpath, mp.FileSystem)
		delete(disabledPaths, cleanMpath)
		updatePaths(availablePaths, disabledPaths)
		cmn.Assert(mp != nil)
		return mp, nil
	}

	return nil, cmn.NewNoMountpathError(mpath)
}

// Disable disables an available mountpath.
// It returns disabled mountpath if it was actually disabled - moved from enabled to disabled.
// Otherwise it returns nil, even if the mountpath existed (but was already disabled).
func Disable(mpath string) (disabledMpath *MountpathInfo, err error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}

	availablePaths, disabledPaths := mountpathsCopy()
	if mpathInfo, ok := availablePaths[cleanMpath]; ok {
		disabledPaths[cleanMpath] = mpathInfo
		mfs.ios.RemoveMpath(cleanMpath)
		delete(availablePaths, cleanMpath)
		moveMarkers(availablePaths, mpathInfo)
		updatePaths(availablePaths, disabledPaths)
		if l := len(availablePaths); l == 0 {
			glog.Errorf("disabled the last available mountpath %s", mpathInfo)
		} else {
			glog.Infof("disabled mountpath %s (%d remain(s) active)", mpathInfo, l)
		}

		return mpathInfo, nil
	}
	if _, ok := disabledPaths[cleanMpath]; ok {
		return nil, nil
	}
	return nil, cmn.NewNoMountpathError(mpath)
}

// Mountpaths returns both available and disabled mountpaths.
func Get() (MPI, MPI) {
	var (
		availablePaths = (*MPI)(mfs.available.Load())
		disabledPaths  = (*MPI)(mfs.disabled.Load())
	)
	if availablePaths == nil {
		tmp := make(MPI, 10)
		availablePaths = &tmp
	}
	if disabledPaths == nil {
		tmp := make(MPI, 10)
		disabledPaths = &tmp
	}
	return *availablePaths, *disabledPaths
}

func CreateBuckets(op string, bcks ...cmn.Bck) (errs []error) {
	var (
		availablePaths, _ = Get()
		totalDirs         = len(availablePaths) * len(bcks) * len(CSM.RegisteredContentTypes)
		totalCreatedDirs  int
	)
	for _, mi := range availablePaths {
		for _, bck := range bcks {
			num, err := mi.createBckDirs(bck)
			if err != nil {
				errs = append(errs, err)
			} else {
				totalCreatedDirs += num
			}
		}
	}
	if errs == nil && totalCreatedDirs != totalDirs {
		errs = append(errs, fmt.Errorf("failed to create %d out of %d buckets' directories: %v",
			totalDirs-totalCreatedDirs, totalDirs, bcks))
	}
	if errs == nil && glog.FastV(4, glog.SmoduleFS) {
		glog.Infof("%s(create bucket dirs): %v, num=%d", op, bcks, totalDirs)
	}
	return
}

func DestroyBucket(op string, bck cmn.Bck, bid uint64) error {
	const destroyStr = "destroy-ais-bucket-dir"
	var n int
	availablePaths, _ := Get()
	for _, mi := range availablePaths {
		dir := mi.makeDelPathBck(bck, bid)
		if err := mi.MoveToTrash(dir); err != nil {
			glog.Errorf("%s: failed to %s (dir: %q, err: %v)", op, destroyStr, dir, err)
		} else {
			n++
		}
	}
	if count := len(availablePaths); n < count {
		return fmt.Errorf("bucket %s: failed to destroy %d/%d dirs", bck, count-n, count)
	}
	return nil
}

func RenameBucketDirs(bidFrom uint64, bckFrom, bckTo cmn.Bck) (err error) {
	availablePaths, _ := Get()
	renamed := make([]*MountpathInfo, 0, len(availablePaths))
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

func moveMarkers(mpaths MPI, from *MountpathInfo) {
	// We assume that the `from` path is no longer in available mountpaths
	// (it was disabled or removed).
	_, ok := mpaths[from.Path]
	debug.Assert(!ok)

	for _, mpath := range mpaths {
		var (
			fromPath = filepath.Join(from.Path, markersDirName)
			fis, err = ioutil.ReadDir(fromPath)
		)
		if err != nil && !os.IsNotExist(err) {
			glog.Errorf("Failed to read directory content (dir: %q, err: %v)", fromPath, err)
		} else {
			for _, fi := range fis {
				debug.Assert(!fi.IsDir()) // Markers should be the files.

				var (
					fromPath = filepath.Join(from.Path, markersDirName, fi.Name())
					toPath   = filepath.Join(mpath.Path, markersDirName, fi.Name())
				)
				if _, _, err := cmn.CopyFile(fromPath, toPath, nil, cmn.ChecksumNone); err != nil && !os.IsNotExist(err) {
					glog.Errorf("Failed to move marker to another mountpath (from: %q, to: %q, err: %v)", fromPath, toPath, err)
				}
			}
		}
		break
	}

	from.ClearMDs()
}

// capacity management

func GetCapStatus() (cs CapStatus) {
	mfs.cmu.RLock()
	cs = mfs.capStatus
	mfs.cmu.RUnlock()
	return
}

func RefreshCapStatus(config *cmn.Config, mpcap MPCap) (cs CapStatus, err error) {
	var (
		availablePaths, _ = Get()
		c                 Capacity
	)
	if len(availablePaths) == 0 {
		err = errors.New(cmn.NoMountpaths)
		return
	}
	if config == nil {
		config = cmn.GCO.Get()
	}
	high, oos := config.LRU.HighWM, config.LRU.OOS
	for path, mi := range availablePaths {
		if c, err = mi.getCapacity(config, true); err != nil {
			glog.Error(err) // TODO: handle
			return
		}
		cs.TotalUsed += c.Used
		cs.TotalAvail += c.Avail
		cs.PctMax = cmn.MaxI32(cs.PctMax, c.PctUsed)
		cs.PctAvg += c.PctUsed
		if mpcap != nil {
			mpcap[path] = c
		}
	}
	cs.PctAvg /= int32(len(availablePaths))
	cs.OOS = int64(cs.PctMax) > oos
	if cs.OOS || int64(cs.PctMax) > high {
		cs.Err = cmn.NewErrorCapacityExceeded(high, cs.PctMax, cs.OOS)
	}
	// cached cap state
	mfs.cmu.Lock()
	mfs.capStatus = cs
	mfs.capTime.curr = mono.NanoTime()
	mfs.capTime.next = mfs.capTime.curr + int64(nextRefresh(config))
	mfs.cmu.Unlock()
	return
}

// recompute next time to refresh cached capacity stats (mfs.capStatus)
func nextRefresh(config *cmn.Config) time.Duration {
	var (
		util = int64(mfs.capStatus.PctAvg) // NOTE: average not max
		umin = cmn.MaxI64(config.LRU.HighWM-10, config.LRU.LowWM)
		umax = config.LRU.OOS
		tmax = config.LRU.CapacityUpdTime
		tmin = config.Periodic.StatsTime
	)
	if util <= umin {
		return config.LRU.CapacityUpdTime
	}
	if util >= umax {
		return config.Periodic.StatsTime
	}
	debug.Assert(umin < umax)
	debug.Assert(tmin < tmax)
	ratio := (util - umin) * 100 / (umax - umin)
	return time.Duration(ratio)*(tmax-tmin)/100 + tmin
}

// NOTE: Is called only and exclusively by `stats.Trunner` providing
//  `config.Periodic.StatsTime` tick.
func CapPeriodic(mpcap MPCap) (cs CapStatus, updated bool, err error) {
	config := cmn.GCO.Get()
	mfs.cmu.RLock()
	mfs.capTime.curr += int64(config.Periodic.StatsTime)
	if mfs.capTime.curr < mfs.capTime.next {
		mfs.cmu.RUnlock()
		return
	}
	mfs.cmu.RUnlock()
	cs, err = RefreshCapStatus(config, mpcap)
	updated = true
	return
}

func CapStatusAux() (fsInfo cmn.CapacityInfo) {
	cs := GetCapStatus()
	fsInfo.Used = cs.TotalUsed
	fsInfo.Total = cs.TotalUsed + cs.TotalAvail
	fsInfo.PctUsed = float64(cs.PctAvg)
	return
}

////////////////
// errors     //
////////////////

func (sie *StorageIntegrityError) Error() string {
	return fmt.Sprintf("%s: %s", siError(sie.code), sie.msg)
}

func newMpathIDMismatchErr(mainDaeID, daeID, mpath string) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siMpathIDMismatch,
		msg:  fmt.Sprintf("DaemonIDs different (%q): %s vs %s", mpath, mainDaeID, daeID),
	}
}

func newVMDIDMismatchErr(vmdDaeID, daeID string) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siTargetIDMismatch,
		msg:  fmt.Sprintf("VMD and target DaemonID don't match: %s vs %s", vmdDaeID, daeID),
	}
}

func newVMDMissingMpathErr(mpath string) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siMpathMissing,
		msg:  fmt.Sprintf("mount path (%q) not in VMD", mpath),
	}
}

func newConfigMissingMpathErr(mpath string) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siMpathMissing,
		msg:  fmt.Sprintf("mountpath %q in VMD but not in config", mpath),
	}
}

func newVMDLoadErr(mpath string, err error) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siMetaCorrupted,
		msg:  fmt.Sprintf("failed to read VMD (%q), err: %v", mpath, err),
	}
}

func newVMDValidationErr(mpath string, err error) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siMetaCorrupted,
		msg:  fmt.Sprintf("failed to validate VMD (%q), err: %v", mpath, err),
	}
}

func newVMDMismatchErr(mainVMD, otherVMD *VMD, mpath string) *StorageIntegrityError {
	return &StorageIntegrityError{
		code: siMetaMismatch,
		msg:  fmt.Sprintf("VMD is different (%q): %v vs %v", mpath, mainVMD, otherVMD),
	}
}

func siError(num int) string {
	const s = "[%s%d - for details, see %s/blob/master/docs/troubleshooting.md]"
	return fmt.Sprintf(s, siePrefix, num, cmn.GithubHome)
}
