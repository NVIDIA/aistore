// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ios"
	"github.com/OneOfOne/xxhash"
)

const (
	uQuantum = 10 // each GET adds a "quantum" of utilization to the mountpath
)

// mountpath lifecycle-change enum
const (
	Add     = "add-mp"
	Remove  = "remove-mp"
	Enable  = "enable-mp"
	Disable = "disable-mp"
)

// globals
var (
	Mountpaths *MountedFS
)

// Terminology:
// - a mountpath is equivalent to (configurable) fspath - both terms are used interchangeably;
// - each mountpath is, simply, a local directory that is serviced by a local filesystem;
// - there's a 1-to-1 relationship between a mountpath and a local filesystem
//   (different mountpaths map onto different filesystems, and vise versa);
// - mountpaths of the form <filesystem-mountpoint>/a/b/c are supported.

type (
	MPI          map[string]*MountpathInfo
	PathRunGroup interface {
		Reg(r PathRunner)
		Unreg(r PathRunner)
	}
	// As a rule, running xactions are aborted and restarted on any mountpath change.
	// But for a few xactions it can be too harsh. E.g, aborting and restarting
	// `download` xaction results in waste of time and network traffic to
	// redownload objects. These xactions should subscribe to mountpath changes
	// as a `PathRunner`s to `PathRunGroup` events and adapt on the fly.
	PathRunner interface {
		cmn.Runner
		Name() string
		ReqAddMountpath(mpath string)
		ReqRemoveMountpath(mpath string)
		ReqEnableMountpath(mpath string)
		ReqDisableMountpath(mpath string)
	}
	MountpathInfo struct {
		Path       string // Cleaned OrigPath
		OrigPath   string // As entered by the user, must be used for logging / returning errors
		Fsid       syscall.Fsid
		FileSystem string
		PathDigest uint64

		// atomic, only increasing counter to prevent name conflicts
		// see: FastRemoveDir method
		removeDirCounter atomic.Uint64

		// LOM caches
		lomCaches cmn.MultiSyncMap
	}

	// MountedFS holds all mountpaths for the target.
	MountedFS struct {
		mu sync.Mutex
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
		// Cached pointer to mountpathInfo used to store BMD
		xattrMpath atomic.Pointer
		// Iostats for the available mountpaths
		ios ios.IOStater
	}
	ChangeReq struct {
		Action string // MountPath action enum (above)
		Path   string // path
	}
)

func MountpathAdd(p string) ChangeReq { return ChangeReq{Action: Add, Path: p} }
func MountpathRem(p string) ChangeReq { return ChangeReq{Action: Remove, Path: p} }
func MountpathEnb(p string) ChangeReq { return ChangeReq{Action: Enable, Path: p} }
func MountpathDis(p string) ChangeReq { return ChangeReq{Action: Disable, Path: p} }

//
// MountpathInfo
//

func newMountpath(cleanPath, origPath string, fsid syscall.Fsid, fs string) *MountpathInfo {
	mi := &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   origPath,
		Fsid:       fsid,
		FileSystem: fs,
		PathDigest: xxhash.ChecksumString64S(cleanPath, cmn.MLCG32),
	}
	mi.removeDirCounter.Store(uint64(time.Now().UnixNano()))
	return mi
}

func (mi *MountpathInfo) LomCache(idx int) *sync.Map { return mi.lomCaches.Get(idx) }

func (mi *MountpathInfo) evictLomCache() {
	for idx := 0; idx < cmn.MultiSyncMapCount; idx++ {
		cache := mi.LomCache(idx)
		cache.Range(func(key interface{}, _ interface{}) bool {
			cache.Delete(key)
			return true
		})
	}
}

// FastRemoveDir removes directory in steps:
// 1. Synchronously gets temporary directory name
// 2. Synchronously renames old folder to temporary directory
// 3. Asynchronously deletes temporary directory
func (mi *MountpathInfo) FastRemoveDir(dir string) error {
	// `dir` will be renamed to non-existing bucket. Then we will
	// try to remove it asynchronously. In case of power cycle we expect that
	// LRU will take care of removing the rest of the bucket.
	var (
		counter        = mi.removeDirCounter.Inc()
		nonExistingDir = fmt.Sprintf("$removing-%d", counter)
		tmpDir         = filepath.Join(mi.Path, nonExistingDir)
	)
	// loose assumption: removing something which doesn't exist is fine
	if err := Access(dir); err != nil && os.IsNotExist(err) {
		return nil
	}
	if err := cmn.CreateDir(filepath.Dir(tmpDir)); err != nil {
		return err
	}
	if err := os.Rename(dir, tmpDir); err != nil {
		if os.IsExist(err) {
			// Slow path - `tmpDir` (or rather `nonExistingDir`) for some reason already exists...
			//
			// Even though `nonExistingDir` should not exist we cannot fully be sure.
			// There are at least two cases when this might not be true:
			//  1. `nonExistingDir` is leftover after target crash.
			//  2. Mountpath was removed and then added again. The counter
			//     will be reset and if we will be removing dirs quickly enough
			//     the counter will catch up with counter of the previous mountpath.
			//     If the directories from the previous mountpath were not yet removed
			//     (slow disk or filesystem) we can end up with the same name.
			//     For now we try to fight this with randomizing the initial counter.

			// In background remove leftover directory.
			go func() {
				glog.Errorf("%s already exists, removing...", tmpDir)
				if err := os.RemoveAll(tmpDir); err != nil {
					glog.Errorf("removing leftover %s failed, err: %v", tmpDir, err)
				}
			}()

			// This time generate fully unique name...
			tmpDir, err = ioutil.TempDir(mi.Path, nonExistingDir)
			if err != nil {
				return err
			}

			// Retry renaming - hopefully it should succeed now.
			err = os.Rename(dir, tmpDir)
		}
		// Someone removed dir before os.Rename, nothing more to do.
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
	}

	// Schedule removing temporary directory which is our old `dir`
	go func() {
		// TODO: in the future, the actual operation must be delegated to LRU
		// that'd take of care of it while pacing itself with regards to the
		// current disk utilization and space availability.
		if err := os.RemoveAll(tmpDir); err != nil {
			glog.Errorf("RemoveAll for %q failed with %v", tmpDir, err)
		}
	}()

	return nil
}

func (mi *MountpathInfo) IsIdle(config *cmn.Config, timestamp time.Time) bool {
	if config == nil {
		config = cmn.GCO.Get()
	}
	curr := Mountpaths.ios.GetMpathUtil(mi.Path, timestamp)
	return curr >= 0 && curr < config.Disk.DiskUtilLowWM
}

func (mi *MountpathInfo) String() string {
	return fmt.Sprintf("mp[%s, fs=%s]", mi.Path, mi.FileSystem)
}

///////////////
// make-path //
///////////////

func (mi *MountpathInfo) makePathBuf(bck cmn.Bck, contentType string, extra int) (buf []byte) {
	var (
		nsLen, bckNameLen, ctLen int

		provLen = 1 + 1 + len(bck.Provider)
	)
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
	if contentType != "" {
		cmn.Assert(bckNameLen > 0)
		cmn.Assert(len(contentType) == contentTypeLen)
		ctLen = 1 + 1 + contentTypeLen
	}

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
	if ctLen > 0 {
		buf = append(buf, filepath.Separator, prefCT)
		buf = append(buf, contentType...)
	}
	return
}

func (mi *MountpathInfo) MakePathBck(bck cmn.Bck) string {
	buf := mi.makePathBuf(bck, "", 0)
	return *(*string)(unsafe.Pointer(&buf))
}

func (mi *MountpathInfo) MakePathCT(bck cmn.Bck, contentType string) string {
	debug.AssertFunc(bck.Valid, bck)
	cmn.Assert(contentType != "")
	buf := mi.makePathBuf(bck, contentType, 0)
	return *(*string)(unsafe.Pointer(&buf))
}

func (mi *MountpathInfo) MakePathFQN(bck cmn.Bck, contentType, objName string) string {
	debug.AssertFunc(bck.Valid, bck)
	cmn.Assert(contentType != "" && objName != "")
	buf := mi.makePathBuf(bck, contentType, 1+len(objName))
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return *(*string)(unsafe.Pointer(&buf))
}

//
// MountedFS
//

func (mfs *MountedFS) LoadBalanceGET(objFQN, objMpath string, copies MPI, now time.Time) (fqn string) {
	var (
		mpathUtils, mpathRRs = mfs.ios.GetAllMpathUtils(now)
		objUtil, ok          = mpathUtils[objMpath]
		rr, _                = mpathRRs[objMpath] // GET round-robin counter (zeros out every iostats refresh i-val)
		util                 = objUtil
		r                    = rr
	)
	fqn = objFQN
	if !ok {
		// Only assert when `mpathUtils` is non-empty. If it's empty it means
		// that `fs2disks` returned empty response so there is no way to get utils.
		debug.AssertMsg(len(mpathUtils) == 0, objMpath)
		return
	}
	for copyFQN, copyMPI := range copies {
		var (
			u        int64
			c, rrcnt int32
		)
		if u, ok = mpathUtils[copyMPI.Path]; !ok {
			continue
		}
		if r, ok = mpathRRs[copyMPI.Path]; !ok {
			if u < util {
				fqn, util, rr = copyFQN, u, r
			}
			continue
		}
		c = r.Load()
		if rr != nil {
			rrcnt = rr.Load()
		}
		if u < util && c <= rrcnt { // the obvious choice
			fqn, util, rr = copyFQN, u, r
			continue
		}
		if u+int64(c)*uQuantum < util+int64(rrcnt)*uQuantum { // heuristics - make uQuantum configurable?
			fqn, util, rr = copyFQN, u, r
		}
	}
	// NOTE: the counter could've been already inc-ed
	//       could keep track of the second best and use CAS to recerve-inc and compare
	//       can wait though
	if rr != nil {
		rr.Inc()
	}
	return
}

// ios delegators
func (mfs *MountedFS) GetMpathUtil(mpath string, now time.Time) int64 {
	return mfs.ios.GetMpathUtil(mpath, now)
}
func (mfs *MountedFS) GetAllMpathUtils(now time.Time) (utils map[string]int64) {
	utils, _ = mfs.ios.GetAllMpathUtils(now)
	return
}
func (mfs *MountedFS) LogAppend(lines []string) []string {
	return mfs.ios.LogAppend(lines)
}
func (mfs *MountedFS) GetSelectedDiskStats() (m map[string]*ios.SelectedDiskStats) {
	return mfs.ios.GetSelectedDiskStats()
}

// Init prepares and adds provided mountpaths. Also validates the mountpaths
// for duplication and availability.
func (mfs *MountedFS) Init(fsPaths []string) error {
	if len(fsPaths) == 0 {
		// (usability) not to clutter the log with backtraces when starting up and validating config
		return fmt.Errorf("FATAL: no fspaths - see README => Configuration and/or fspaths section in the config.sh")
	}

	for _, path := range fsPaths {
		if err := mfs.Add(path); err != nil {
			return err
		}
	}

	return nil
}

// Add adds new mountpath to the target's mountpaths.
// FIXME: unify error messages for original and clean mountpath
func (mfs *MountedFS) Add(mpath string) error {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return err
	}
	if err := Access(cleanMpath); err != nil {
		return fmt.Errorf("fspath %q %s, err: %v", mpath, cmn.DoesNotExist, err)
	}
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(cleanMpath, &statfs); err != nil {
		return fmt.Errorf("cannot statfs fspath %q, err: %w", mpath, err)
	}

	fs, err := fqn2fsAtStartup(cleanMpath)
	if err != nil {
		return fmt.Errorf("cannot get filesystem: %v", err)
	}

	mp := newMountpath(cleanMpath, mpath, statfs.Fsid, fs)

	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if _, exists := availablePaths[mp.Path]; exists {
		return fmt.Errorf("tried to add already registered mountpath: %v", mp.Path)
	}

	if existingPath, exists := mfs.fsIDs[mp.Fsid]; exists && mfs.checkFsID {
		return fmt.Errorf("tried to add path %v but same fsid (%v) was already registered by %v", mpath, mp.Fsid, existingPath)
	}

	mfs.ios.AddMpath(mp.Path, mp.FileSystem)

	availablePaths[mp.Path] = mp
	mfs.fsIDs[mp.Fsid] = cleanMpath
	mfs.updatePaths(availablePaths, disabledPaths)
	return nil
}

// Remove removes mountpaths from the target's mountpaths. It searches
// for the mountpath in available and disabled (if the mountpath is not found
// in available).
func (mfs *MountedFS) Remove(mpath string) error {
	var (
		mp     *MountpathInfo
		exists bool
	)

	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return err
	}

	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if mp, exists = availablePaths[cleanMpath]; !exists {
		if mp, exists = disabledPaths[cleanMpath]; !exists {
			return fmt.Errorf("tried to remove non-existing mountpath: %v", mpath)
		}

		delete(disabledPaths, cleanMpath)
		delete(mfs.fsIDs, mp.Fsid)
		mfs.updatePaths(availablePaths, disabledPaths)
		return nil
	}

	delete(availablePaths, cleanMpath)
	mfs.ios.RemoveMpath(cleanMpath)
	delete(mfs.fsIDs, mp.Fsid)

	go mp.evictLomCache()

	if l := len(availablePaths); l == 0 {
		glog.Errorf("removed the last available mountpath %s", mp)
	} else {
		glog.Infof("removed mountpath %s (%d remain(s) active)", mp, l)
	}

	mfs.updatePaths(availablePaths, disabledPaths)
	return nil
}

// Enable enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func (mfs *MountedFS) Enable(mpath string) (enabled bool, err error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return false, err
	}
	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if _, ok := availablePaths[cleanMpath]; ok {
		return false, nil
	}
	if mp, ok := disabledPaths[cleanMpath]; ok {
		availablePaths[cleanMpath] = mp
		mfs.ios.AddMpath(cleanMpath, mp.FileSystem)
		delete(disabledPaths, cleanMpath)
		mfs.updatePaths(availablePaths, disabledPaths)
		return true, nil
	}

	return false, cmn.NewNoMountpathError(mpath)
}

// Disable disables an available mountpath. disabled is set to true if
// mountpath has been moved from available to disabled and exists is set to
// true if such mountpath even exists.
func (mfs *MountedFS) Disable(mpath string) (disabled bool, err error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return false, err
	}

	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if mpathInfo, ok := availablePaths[cleanMpath]; ok {
		disabledPaths[cleanMpath] = mpathInfo
		mfs.ios.RemoveMpath(cleanMpath)
		delete(availablePaths, cleanMpath)
		mfs.updatePaths(availablePaths, disabledPaths)
		if l := len(availablePaths); l == 0 {
			glog.Errorf("disabled the last available mountpath %s", mpathInfo)
		} else {
			glog.Infof("disabled mountpath %s (%d remain(s) active)", mpathInfo, l)
		}
		go mpathInfo.evictLomCache()
		return true, nil
	}
	if _, ok := disabledPaths[cleanMpath]; ok {
		return false, nil
	}
	return false, cmn.NewNoMountpathError(mpath)
}

// Returns number of available mountpaths
func (mfs *MountedFS) NumAvail() int {
	availablePaths := (*MPI)(mfs.available.Load())
	return len(*availablePaths)
}

// Mountpaths returns both available and disabled mountpaths.
func (mfs *MountedFS) Get() (MPI, MPI) {
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

// DisableFsIDCheck disables fsid checking when adding new mountpath
func (mfs *MountedFS) DisableFsIDCheck() { mfs.checkFsID = false }

func (mfs *MountedFS) CreateBuckets(op string, bcks ...cmn.Bck) (errs []error) {
	var (
		availablePaths, _ = mfs.Get()
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

func (mfs *MountedFS) DestroyBuckets(op string, bcks ...cmn.Bck) error {
	const destroyStr = "destroy-ais-bucket-dir"
	var (
		availablePaths, _  = mfs.Get()
		totalDirs          = len(availablePaths) * len(bcks)
		totalDestroyedDirs = 0
	)

	for _, mpathInfo := range availablePaths {
		for _, bck := range bcks {
			dir := mpathInfo.MakePathBck(bck)
			if err := mpathInfo.FastRemoveDir(dir); err != nil {
				glog.Errorf("%s: failed to %s (dir: %q, err: %v)", op, destroyStr, dir, err)
			} else {
				totalDestroyedDirs++
			}
		}
	}

	if totalDestroyedDirs != totalDirs {
		return fmt.Errorf("failed to destroy %d out of %d buckets' directories: %v", totalDirs-totalDestroyedDirs, totalDirs, bcks)
	}

	if glog.FastV(4, glog.SmoduleFS) {
		glog.Infof("%s: %s (buckets %v, num dirs %d)", op, destroyStr, bcks, totalDirs)
	}
	return nil
}

func (mfs *MountedFS) FetchFSInfo() cmn.FSInfo {
	var (
		fsInfo            = cmn.FSInfo{}
		availablePaths, _ = mfs.Get()
		visitedFS         = make(map[syscall.Fsid]struct{})
	)
	for mpath := range availablePaths {
		statfs := &syscall.Statfs_t{}

		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		if _, ok := visitedFS[statfs.Fsid]; ok {
			continue
		}
		visitedFS[statfs.Fsid] = struct{}{}
		fsInfo.FSUsed += (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize)
		fsInfo.FSCapacity += statfs.Blocks * uint64(statfs.Bsize)
	}
	if fsInfo.FSCapacity > 0 {
		// FIXME: assuming all mountpaths have approx. the same capacity
		fsInfo.PctFSUsed = float64(fsInfo.FSUsed*100) / float64(fsInfo.FSCapacity)
	}
	return fsInfo
}

func (mfs *MountedFS) RenameBucketDirs(bckFrom, bckTo cmn.Bck) (err error) {
	availablePaths, _ := mfs.Get()
	renamed := make([]*MountpathInfo, 0, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		fromPath := mpathInfo.MakePathBck(bckFrom)
		toPath := mpathInfo.MakePathBck(bckTo)

		// os.Rename fails when renaming to a directory which already exists.
		// We should remove destination bucket directory before rename. It's reasonable to do so
		// as all targets agreed to rename and rename was committed in BMD.
		os.RemoveAll(toPath)
		if err = os.Rename(fromPath, toPath); err != nil {
			break
		}
		renamed = append(renamed, mpathInfo)
	}

	if err == nil {
		return
	}
	for _, mpathInfo := range renamed {
		fromPath := mpathInfo.MakePathBck(bckTo)
		toPath := mpathInfo.MakePathBck(bckFrom)
		if erd := os.Rename(fromPath, toPath); erd != nil {
			glog.Error(erd)
		}
	}
	return
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

//
// private methods
//

func (mfs *MountedFS) updatePaths(available, disabled MPI) {
	mfs.available.Store(unsafe.Pointer(&available))
	mfs.disabled.Store(unsafe.Pointer(&disabled))
	mfs.xattrMpath.Store(unsafe.Pointer(nil))
}

// Creates all CT directories for a given (mountpath, bck)
// NOTE handling of empty dirs
func (mi *MountpathInfo) createBckDirs(bck cmn.Bck) (num int, err error) {
	for contentType := range CSM.RegisteredContentTypes {
		dir := mi.MakePathCT(bck, contentType)
		if err := Access(dir); err == nil {
			names, empty, errEmpty := IsDirEmpty(dir)
			if errEmpty != nil {
				return num, errEmpty
			}
			if !empty {
				err = fmt.Errorf("bucket %s: directory %s already exists and is not empty (%v...)", bck, dir, names)
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

// mountpathsCopy returns a shallow copy of current mountpaths
func (mfs *MountedFS) mountpathsCopy() (MPI, MPI) {
	availablePaths, disabledPaths := mfs.Get()
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

func (mfs *MountedFS) String() string {
	availablePaths, _ := mfs.Get()
	s := "\n"
	for _, mpathInfo := range availablePaths {
		s += mpathInfo.String() + "\n"
	}
	return strings.TrimSuffix(s, "\n")
}

// Select a "random" mountpath using HRW algorithm to store/load bucket metadata
func (mfs *MountedFS) MpathForMetadata() (mpath *MountpathInfo, err error) {
	// fast path
	mp := mfs.xattrMpath.Load()
	if mp != nil {
		return (*MountpathInfo)(mp), nil
	}

	// slow path
	avail := (*MPI)(mfs.available.Load())
	if len(*avail) == 0 {
		return nil, fmt.Errorf("no mountpath available")
	}
	maxVal := uint64(0)
	for _, m := range *avail {
		if m.PathDigest > maxVal {
			maxVal = m.PathDigest
			mpath = m
		}
	}
	if mpath == nil {
		return nil, fmt.Errorf("failed to choose a mountpath")
	}
	if glog.FastV(4, glog.SmoduleFS) {
		glog.Infof("Mountpath %q selected for storing BMD in xattrs", mpath.Path)
	}
	mfs.xattrMpath.Store(unsafe.Pointer(mpath))
	return mpath, nil
}
