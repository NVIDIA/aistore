/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/OneOfOne/xxhash"
)

const MLCG32 = 1103515245

// Mountpath Change enum
const (
	Add     = "add-mp"
	Remove  = "remove-mp"
	Enable  = "enable-mp"
	Disable = "disable-mp"
)

// filesystem utilization enum (<- iostat)
const (
	StatDiskUtil = "dutil"
	StatQueueLen = "dquel"
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
		removeDirCounter uint64

		// FileSystem utilization represented as
		// utilizations and queue lengths of the underlying disks,
		// where cmn.PairU32 structs atomically store the corresponding float32 bits
		iostats map[string]*iotracker
		ioepoch map[string]int64
	}
	iotracker struct {
		prev cmn.PairU32
		curr cmn.PairU32
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
		available unsafe.Pointer
		// Disabled mountpaths - mountpaths which for some reason did not pass
		// the health check and cannot be used for a moment.
		disabled unsafe.Pointer
		// The following correspond to the values in config.sh for "cloud_buckets"
		// and "local_buckets", used for mpath validation
		localBuckets string
		cloudBuckets string
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

func newMountpath(path string, fsid syscall.Fsid, fs string) *MountpathInfo {
	cleanPath := filepath.Clean(path)
	mi := &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   path,
		Fsid:       fsid,
		FileSystem: fs,
		PathDigest: xxhash.ChecksumString64S(cleanPath, MLCG32),
		iostats:    make(map[string]*iotracker, 2),
		ioepoch:    make(map[string]int64, 2),
	}
	mi.iostats[StatDiskUtil] = &iotracker{} // FIXME: ios.Const
	mi.iostats[StatQueueLen] = &iotracker{}
	return mi
}

// FastRemoveDir removes directory in steps:
// 1. Synchronously gets temporary directory name
// 2. Synchronously renames old folder to temporary directory
// 3. Asynchronously deletes temporary directory
func (mi *MountpathInfo) FastRemoveDir(dir string) error {
	// dir will be renamed to non-existing bucket in WorkfileType. Then we will
	// try to remove it asynchronously. In case of power cycle we expect that
	// LRU will take care of removing the rest of the bucket.
	counter := atomic.AddUint64(&mi.removeDirCounter, 1)
	nonExistingBucket := fmt.Sprintf("removing-%d", counter)
	tmpDir, errStr := CSM.FQN(mi.Path, WorkfileType, true, nonExistingBucket, "")
	if errStr != "" {
		return errors.New(errStr)
	}
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

// GetIOStats returns the most recently updated previous/current (utilization, queue size)
func (mi *MountpathInfo) GetIOstats(name string) (prev, curr cmn.PairF32) {
	cmn.Assert(name == StatDiskUtil || name == StatQueueLen)
	tracker, _ := mi.iostats[name]
	p := &tracker.prev
	prev = p.U2F()
	c := &tracker.curr
	curr = c.U2F()
	return
}

func (mi *MountpathInfo) IsIdle(config *cmn.Config) bool {
	if config == nil {
		config = cmn.GCO.Get()
	}
	prev, curr := mi.GetIOstats(StatDiskUtil)
	return prev.Max >= 0 && prev.Max < float32(config.Xaction.DiskUtilLowWM) &&
		curr.Max >= 0 && curr.Max < float32(config.Xaction.DiskUtilLowWM)
}

// SetIOstats is called by the iostat runner directly to fill-in the most recently
// updated utilizations and queue lengths of the disks used by this mountpath
// (or, more precisely, the underlying local FS)
func (mi *MountpathInfo) SetIOstats(epoch int64, name string, f float32) {
	tracker, _ := mi.iostats[name]
	if mi.ioepoch[name] < epoch {
		// current => prev, f => curr, mi.epoch = epoch
		curr := &tracker.curr
		curr.CopyTo(&tracker.prev)
		curr.Init(f)
		mi.ioepoch[name] = epoch
	} else {
		// curr min/max
		curr := &tracker.curr
		fpair := curr.U2F()
		if fpair.Max < f {
			u := math.Float32bits(f)
			atomic.StoreUint32(&curr.Max, u)
		} else if fpair.Min > f {
			u := math.Float32bits(f)
			atomic.StoreUint32(&curr.Min, u)
		}
	}
	return
}

func (mi *MountpathInfo) String() string {
	_, u := mi.GetIOstats(StatDiskUtil)
	_, q := mi.GetIOstats(StatQueueLen)
	return fmt.Sprintf("mp=%s, fs=%s, util=d%s:q%s", mi.Path, mi.FileSystem, u, q)
}

//
// MountedFS aka fs.Mountpaths
//

// NewMountedFS returns initialized instance of MountedFS struct.
func NewMountedFS() *MountedFS {
	return &MountedFS{
		fsIDs:        make(map[syscall.Fsid]string, 10),
		checkFsID:    true,
		localBuckets: cmn.LocalBs,
		cloudBuckets: cmn.CloudBs,
	}
}

// Init prepares and adds provided mountpaths. Also validates the mountpaths
// for duplication and availablity.
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
func (mfs *MountedFS) Add(mpath string) error {
	seperator := string(filepath.Separator)
	for _, bucket := range []string{mfs.localBuckets, mfs.cloudBuckets} {
		invalidMpath := seperator + bucket
		if strings.HasSuffix(mpath, invalidMpath) {
			return fmt.Errorf("Cannot add fspath %q with suffix %q", mpath, invalidMpath)
		}
		invalidMpath += seperator
		if strings.Contains(mpath, invalidMpath) {
			return fmt.Errorf("Fspath %q cannot contain %q anywhere in its path", mpath, invalidMpath)
		}
	}

	if _, err := os.Stat(mpath); err != nil {
		return fmt.Errorf("fspath %q %s, err: %v", mpath, cmn.DoesNotExist, err)
	}
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(mpath, &statfs); err != nil {
		return fmt.Errorf("cannot statfs fspath %q, err: %v", mpath, err)
	}

	fs, err := Fqn2fsAtStartup(mpath)
	if err != nil {
		return fmt.Errorf("cannot get filesystem: %v", err)
	}

	mp := newMountpath(mpath, statfs.Fsid, fs)
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if _, exists := availablePaths[mp.Path]; exists {
		return fmt.Errorf("tried to add already registered mountpath: %v", mp.Path)
	}

	if existingPath, exists := mfs.fsIDs[mp.Fsid]; exists && mfs.checkFsID {
		return fmt.Errorf("tried to add path %v but same fsid was already registered by %v", mpath, existingPath)
	}

	availablePaths[mp.Path] = mp
	mfs.fsIDs[mp.Fsid] = mpath
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

	mpath = filepath.Clean(mpath)
	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if mp, exists = availablePaths[mpath]; !exists {
		if mp, exists = disabledPaths[mpath]; !exists {
			return fmt.Errorf("tried to remove nonexisting mountpath: %v", mpath)
		}

		delete(disabledPaths, mpath)
		delete(mfs.fsIDs, mp.Fsid)
		mfs.updatePaths(availablePaths, disabledPaths)
		return nil
	}

	delete(availablePaths, mpath)
	delete(mfs.fsIDs, mp.Fsid)
	if len(availablePaths) == 0 {
		glog.Errorf("removed last available mountpath: %s", mpath)
	}

	mfs.updatePaths(availablePaths, disabledPaths)
	return nil
}

// Enable enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func (mfs *MountedFS) Enable(mpath string) (enabled, exists bool) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	mpath = filepath.Clean(mpath)
	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if _, ok := availablePaths[mpath]; ok {
		return false, true
	}

	if _, ok := disabledPaths[mpath]; ok {
		availablePaths[mpath] = disabledPaths[mpath]
		delete(disabledPaths, mpath)
		mfs.updatePaths(availablePaths, disabledPaths)
		return true, true
	}

	return
}

// Disable disables an available mountpath. disabled is set to true if
// mountpath has been moved from available to disabled and exists is set to
// true if such mountpath even exists.
func (mfs *MountedFS) Disable(mpath string) (disabled, exists bool) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	mpath = filepath.Clean(mpath)
	availablePaths, disabledPaths := mfs.mountpathsCopy()
	if mpathInfo, ok := availablePaths[mpath]; ok {
		disabledPaths[mpath] = mpathInfo
		delete(availablePaths, mpath)
		mfs.updatePaths(availablePaths, disabledPaths)
		return true, true
	}

	if _, ok := disabledPaths[mpath]; ok {
		return false, true
	}

	return
}

// Mountpaths returns both available and disabled mountpaths.
func (mfs *MountedFS) Get() (map[string]*MountpathInfo, map[string]*MountpathInfo) {
	available := (*map[string]*MountpathInfo)(atomic.LoadPointer(&mfs.available))
	disabled := (*map[string]*MountpathInfo)(atomic.LoadPointer(&mfs.disabled))
	if available == nil {
		tmp := make(map[string]*MountpathInfo, 10)
		available = &tmp
	}

	if disabled == nil {
		tmp := make(map[string]*MountpathInfo, 10)
		disabled = &tmp
	}

	return *available, *disabled
}

// DisableFsIDCheck disables fsid checking when adding new mountpath
func (mfs *MountedFS) DisableFsIDCheck() { mfs.checkFsID = false }

// builds fqn of directory for local buckets from mountpath
func (mfs *MountedFS) MakePathLocal(basePath, contentType string) string {
	return filepath.Join(basePath, contentType, mfs.localBuckets)
}

// builds fqn of directory for cloud buckets from mountpath
func (mfs *MountedFS) MakePathCloud(basePath, contentType string) string {
	return filepath.Join(basePath, contentType, mfs.cloudBuckets)
}

//
// private methods
//

func (mfs *MountedFS) updatePaths(available, disabled map[string]*MountpathInfo) {
	atomic.StorePointer(&mfs.available, unsafe.Pointer(&available))
	atomic.StorePointer(&mfs.disabled, unsafe.Pointer(&disabled))
}

// mountpathsCopy returns shallow copy of current mountpaths
func (mfs *MountedFS) mountpathsCopy() (map[string]*MountpathInfo, map[string]*MountpathInfo) {
	available, disabled := mfs.Get()
	availableCopy := make(map[string]*MountpathInfo, len(available))
	disabledCopy := make(map[string]*MountpathInfo, len(available))

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
