/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/OneOfOne/xxhash"
)

const MLCG32 = 1103515245

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
	MountpathInfo struct {
		Path       string // Cleaned OrigPath
		OrigPath   string // As entered by the user, must be used for logging / returning errors
		Fsid       syscall.Fsid
		FileSystem string
		PathDigest uint64
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
)

func newMountpath(path string, fsid syscall.Fsid, fs string) *MountpathInfo {
	cleanPath := filepath.Clean(path)
	return &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   path,
		Fsid:       fsid,
		FileSystem: fs,
		PathDigest: xxhash.ChecksumString64S(cleanPath, MLCG32),
	}
}

// NewMountedFS returns initialized instance of MountedFS struct.
func NewMountedFS(localBuckets, cloudBuckets string) *MountedFS {
	return &MountedFS{
		fsIDs:        make(map[syscall.Fsid]string),
		checkFsID:    true,
		localBuckets: localBuckets,
		cloudBuckets: cloudBuckets,
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
		if err := mfs.AddMountpath(path); err != nil {
			return err
		}
	}

	return nil
}

// AddMountpath adds new mountpath to the target's mountpaths.
func (mfs *MountedFS) AddMountpath(mpath string) error {
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
		return fmt.Errorf("fspath %q does not exists, err: %v", mpath, err)
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

// RemoveMountpath removes mountpaths from the target's mountpaths. It searches
// for the mountpath in available and disabled (if the mountpath is not found
// in available).
func (mfs *MountedFS) RemoveMountpath(mpath string) error {
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

// EnableMountpath enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func (mfs *MountedFS) EnableMountpath(mpath string) (enabled, exists bool) {
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

// DisableMountpath disables an available mountpath. disabled is set to true if
// mountpath has been moved from available to disabled and exists is set to
// true if such mountpath even exists.
func (mfs *MountedFS) DisableMountpath(mpath string) (disabled, exists bool) {
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
func (mfs *MountedFS) Mountpaths() (map[string]*MountpathInfo, map[string]*MountpathInfo) {
	available := (*map[string]*MountpathInfo)(atomic.LoadPointer(&mfs.available))
	disabled := (*map[string]*MountpathInfo)(atomic.LoadPointer(&mfs.disabled))
	if available == nil {
		tmp := make(map[string]*MountpathInfo, 0)
		available = &tmp
	}

	if disabled == nil {
		tmp := make(map[string]*MountpathInfo, 0)
		disabled = &tmp
	}

	return *available, *disabled
}

// DisableFsIDCheck disables fsid checking when adding new mountpath
func (mfs *MountedFS) DisableFsIDCheck() {
	mfs.checkFsID = false
}

func (mfs *MountedFS) updatePaths(available, disabled map[string]*MountpathInfo) {
	atomic.StorePointer(&mfs.available, unsafe.Pointer(&available))
	atomic.StorePointer(&mfs.disabled, unsafe.Pointer(&disabled))
}

// mountpathsCopy returns shallow copy of current mountpaths
func (mfs *MountedFS) mountpathsCopy() (map[string]*MountpathInfo, map[string]*MountpathInfo) {
	available, disabled := mfs.Mountpaths()
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

// builds fqn of directory for local buckets from mountpath
func (mfs *MountedFS) MakePathLocal(basePath string) string {
	return filepath.Join(basePath, mfs.localBuckets)
}

// builds fqn of directory for cloud buckets from mountpath
func (mfs *MountedFS) MakePathCloud(basePath string) string {
	return filepath.Join(basePath, mfs.cloudBuckets)
}
