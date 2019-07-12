// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
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
	"github.com/NVIDIA/aistore/ios"
	"github.com/OneOfOne/xxhash"
)

const pkgName = "fs"
const uQuantum = 10 // each GET adds a "quantum" of utilization to the mountpath

// mountpath lifecycle-change enum
const (
	Add     = "add-mp"
	Remove  = "remove-mp"
	Enable  = "enable-mp"
	Disable = "disable-mp"
)

// lomcache mask & number of those caches
const (
	LomCacheMask = 0x3f
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
	PathRunner interface {
		cmn.Runner
		SetID(int64)
		ID() int64
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
		lomcaches [LomCacheMask + 1]LomCache
	}
	LomCache struct {
		M sync.Map
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
		ios *ios.IostatContext
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
	return mi
}

func (mi *MountpathInfo) LomCache(idx int) *LomCache { x := &mi.lomcaches; return &x[idx] }

// FastRemoveDir removes directory in steps:
// 1. Synchronously gets temporary directory name
// 2. Synchronously renames old folder to temporary directory
// 3. Asynchronously deletes temporary directory
func (mi *MountpathInfo) FastRemoveDir(dir string) error {
	// dir will be renamed to non-existing bucket in WorkfileType. Then we will
	// try to remove it asynchronously. In case of power cycle we expect that
	// LRU will take care of removing the rest of the bucket.
	counter := mi.removeDirCounter.Inc()
	nonExistingBucket := fmt.Sprintf("removing-%d", counter)
	tmpDir := CSM.FQN(mi, WorkfileType, true, nonExistingBucket, "")
	if err := os.Rename(dir, tmpDir); err != nil {
		return err
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

// returns fqn for a given content-type
func (mi *MountpathInfo) MakePath(contentType string, bckIsLocal bool) (fqn string) {
	if bckIsLocal {
		fqn = filepath.Join(mi.Path, contentType, cmn.LocalBs)
	} else {
		fqn = filepath.Join(mi.Path, contentType, cmn.CloudBs)
	}
	return
}

func (mi *MountpathInfo) MakePathBucket(contentType, bucket string, bckIsLocal bool) string {
	return filepath.Join(mi.MakePath(contentType, bckIsLocal), bucket)
}
func (mi *MountpathInfo) MakePathBucketObject(contentType, bucket, objname string, bckIsLocal bool) string {
	return filepath.Join(mi.MakePath(contentType, bckIsLocal), bucket, objname)
}

//
// MountedFS
//

// global init
func InitMountedFS() {
	Mountpaths = NewMountedFS()
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleFS, logLvl)
	}
}

// new instance
func NewMountedFS() *MountedFS {
	mfs := &MountedFS{fsIDs: make(map[syscall.Fsid]string, 10), checkFsID: true}
	mfs.ios = ios.NewIostatContext()
	return mfs
}

func (mfs *MountedFS) LoadBalanceGET(objfqn, objmpath string, copies MPI, now time.Time) (fqn string) {
	var (
		mpathUtils, mpathRRs = mfs.ios.GetAllMpathUtils(now)
		objutil, ok          = mpathUtils[objmpath]
		rr, _                = mpathRRs[objmpath] // GET round-robin counter (zeros out every iostats refresh i-val)
		util                 = objutil
		r                    = rr
	)
	fqn = objfqn
	if !ok {
		cmn.DassertMsg(false, objmpath, pkgName)
		return
	}
	for copyfqn, copympi := range copies {
		var (
			u        int64
			c, rrcnt int32
		)
		if u, ok = mpathUtils[copympi.Path]; !ok {
			continue
		}
		if r, ok = mpathRRs[copympi.Path]; !ok {
			if u < util {
				fqn, util, rr = copyfqn, u, r
			}
			continue
		}
		c = r.Load()
		if rr != nil {
			rrcnt = rr.Load()
		}
		if u < util && c <= rrcnt { // the obvious choice
			fqn, util, rr = copyfqn, u, r
			continue
		}
		if u+int64(c)*uQuantum < util+int64(rrcnt)*uQuantum { // heuristics - make uQuantum configurable?
			fqn, util, rr = copyfqn, u, r
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

	separator := string(filepath.Separator)
	for _, bucket := range []string{cmn.LocalBs, cmn.CloudBs} {
		invalidMpath := separator + bucket
		if strings.HasSuffix(cleanMpath, invalidMpath) {
			return fmt.Errorf("cannot add fspath %q with suffix %q", mpath, invalidMpath)
		}
		invalidMpath += separator
		if strings.Contains(cleanMpath, invalidMpath) {
			return fmt.Errorf("fspath %q cannot contain %q anywhere in its path", mpath, invalidMpath)
		}
	}

	if _, err := os.Stat(cleanMpath); err != nil {
		return fmt.Errorf("fspath %q %s, err: %v", mpath, cmn.DoesNotExist, err)
	}
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(cleanMpath, &statfs); err != nil {
		return fmt.Errorf("cannot statfs fspath %q, err: %v", mpath, err)
	}

	fs, err := Fqn2fsAtStartup(cleanMpath)
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
	mfs.ios.AddMpath(mp.Path, mp.FileSystem)

	if existingPath, exists := mfs.fsIDs[mp.Fsid]; exists && mfs.checkFsID {
		return fmt.Errorf("tried to add path %v but same fsid was already registered by %v", mpath, existingPath)
	}

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
		return true, nil
	}
	if _, ok := disabledPaths[cleanMpath]; ok {
		return false, nil
	}
	return false, cmn.NewNoMountpathError(mpath)
}

// Returns number of available mountpaths
func (mfs *MountedFS) NumAvail() int {
	available := (*MPI)(mfs.available.Load())
	return len(*available)
}

// Mountpaths returns both available and disabled mountpaths.
func (mfs *MountedFS) Get() (MPI, MPI) {
	available := (*MPI)(mfs.available.Load())
	disabled := (*MPI)(mfs.disabled.Load())
	if available == nil {
		tmp := make(MPI, 10)
		available = &tmp
	}

	if disabled == nil {
		tmp := make(MPI, 10)
		disabled = &tmp
	}

	return *available, *disabled
}

// DisableFsIDCheck disables fsid checking when adding new mountpath
func (mfs *MountedFS) DisableFsIDCheck() { mfs.checkFsID = false }

func (mfs *MountedFS) CreateDestroyLocalBuckets(op string, create bool, buckets ...string) {
	const (
		fmt1       = "%s: failed to %s"
		fmt2       = "%s: %s"
		createstr  = "create-local-bucket-dir"
		destroystr = "destroy-local-bucket-dir"
	)

	text := createstr
	if !create {
		text = destroystr
	}

	failMsg := fmt.Sprintf(fmt1, op, text)
	passMsg := fmt.Sprintf(fmt2, op, text)

	mfs.createDestroyBuckets(create, true, passMsg, failMsg, buckets...)
}

func (mfs *MountedFS) EvictCloudBucket(bucket string) {
	const (
		passMsg = "evict cloud bucket"
		failMsg = "failed: evict cloud bucket"
	)

	mfs.createDestroyBuckets(false, false, passMsg, failMsg, bucket)
}

func (mfs *MountedFS) FetchFSInfo() cmn.FSInfo {
	fsInfo := cmn.FSInfo{}

	availableMountpaths, _ := mfs.Get()

	visitedFS := make(map[syscall.Fsid]struct{})

	for mpath := range availableMountpaths {
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
		//FIXME: assuming that each mountpath has the same capacity and gets distributed the same files
		fsInfo.PctFSUsed = float64(fsInfo.FSUsed*100) / float64(fsInfo.FSCapacity)
	}

	return fsInfo
}

//
// private methods
//

func (mfs *MountedFS) updatePaths(available, disabled MPI) {
	mfs.available.Store(unsafe.Pointer(&available))
	mfs.disabled.Store(unsafe.Pointer(&disabled))
	mfs.xattrMpath.Store(unsafe.Pointer(nil))
}

func (mfs *MountedFS) createDestroyBuckets(create bool, loc bool, passMsg string, failMsg string, buckets ...string) {
	var (
		contentTypes      = CSM.RegisteredContentTypes
		availablePaths, _ = mfs.Get()
		wg                = &sync.WaitGroup{}
	)
	for _, mpathInfo := range availablePaths {
		wg.Add(1)
		go func(mi *MountpathInfo) {
			ff, num := cmn.CreateDir, 0
			if !create {
				ff = mi.FastRemoveDir
			}
			for _, bucket := range buckets {
				for contentType := range contentTypes {
					dir := mi.MakePathBucket(contentType, bucket, loc /*whether buckets are local*/)
					if !create {
						if _, err := os.Stat(dir); os.IsNotExist(err) {
							continue
						}
					}
					// TODO: on error abort and rollback
					if err := ff(dir); err != nil {
						glog.Errorf("%q (dir: %q, err: %q)", failMsg, dir, err)
					} else {
						num++
					}
				}
				if glog.FastV(4, glog.SmoduleFS) {
					glog.Infof("%q (bucket %s, num dirs %d)", passMsg, bucket, num)
				}
				num = 0
			}
			wg.Done()
		}(mpathInfo)
	}
	wg.Wait()
}

// mountpathsCopy returns shallow copy of current mountpaths
func (mfs *MountedFS) mountpathsCopy() (MPI, MPI) {
	available, disabled := mfs.Get()
	availableCopy := make(MPI, len(available))
	disabledCopy := make(MPI, len(available))

	for mpath, mpathInfo := range available {
		availableCopy[mpath] = mpathInfo
	}

	for mpath, mpathInfo := range disabled {
		disabledCopy[mpath] = mpathInfo
	}

	return availableCopy, disabledCopy
}

func (mfs *MountedFS) String() string {
	available, _ := mfs.Get()
	s := "\n"
	for _, mpathInfo := range available {
		s += mpathInfo.String() + "\n"
	}
	return strings.TrimSuffix(s, "\n")
}

// Select a "random" mountpath using HRW algorithm to store/load bucket metadata
func (mfs *MountedFS) MpathForXattr() (mpath *MountpathInfo, err error) {
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
