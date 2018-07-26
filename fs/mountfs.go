/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package fs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// Terminology:
// - a mountpath is equivalent to (configurable) fspath - both terms are used interchangeably;
// - each mountpath is, simply, a local directory that is serviced by a local filesystem;
// - there's a 1-to-1 relationship between a mountpath and a local filesystem
//   (different mountpaths map onto different filesystems, and vise versa);
// - mountpaths of the form <filesystem-mountpoint>/a/b/c are supported.

// MountedFS holds all mountpaths for the target.
type MountedFS struct {
	mu sync.RWMutex
	// Available mountpaths - mountpaths which are used to store the data.
	Available      map[string]*MountpathInfo `json:"available"`
	availableCache unsafe.Pointer            // used for COW purposes
	// Disabled mountpaths - mountpaths which for some reason did not pass
	// the health check and cannot be used for a moment.
	Disabled      map[string]*MountpathInfo `json:"disabled"`
	disabledCache unsafe.Pointer            // used for COW purposes
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

	return mfs.checkFsidDuplicates()
}

// AddMountpath adds new mountpath to the target's mountpaths.
func (mfs *MountedFS) AddMountpath(mpath string) error {
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

	mp := NewMountpath(mpath, statfs.Fsid, fs)
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if _, ok := mfs.Available[mp.Path]; ok {
		return fmt.Errorf("tried to add already registered mountpath: %v", mp.Path)
	}

	if mfs.Available == nil {
		mfs.Available = make(map[string]*MountpathInfo)
	}

	mfs.Available[mp.Path] = mp
	mfs.recalcCache()

	return nil
}

// RemoveMountpath removes mountpaths from the target's mountpaths. It searches
// for the mountpath in available and disabled (if the mountpath is not found
// in available).
func (mfs *MountedFS) RemoveMountpath(mpath string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if _, ok := mfs.Available[mpath]; !ok {
		if _, ok := mfs.Disabled[mpath]; !ok {
			return fmt.Errorf("tried to remove nonexisting mountpath: %v", mpath)
		}

		delete(mfs.Disabled, mpath)
		return nil
	}

	delete(mfs.Available, mpath)
	if len(mfs.Available) == 0 {
		glog.Errorf("removed last available mountpath: %s", mpath)
	}

	mfs.recalcCache()
	return nil
}

// EnableMountpath enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func (mfs *MountedFS) EnableMountpath(mpath string) (enabled, exists bool) {
	mpath = filepath.Clean(mpath)
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if _, ok := mfs.Available[mpath]; ok {
		return false, true
	}

	if _, ok := mfs.Disabled[mpath]; ok {
		mpathInfo := mfs.Disabled[mpath]
		if mfs.Available == nil {
			mfs.Available = make(map[string]*MountpathInfo)
		}

		mfs.Available[mpath] = mpathInfo
		delete(mfs.Disabled, mpath)
		mfs.recalcCache()
		return true, true
	}

	return
}

// DisableMountpath disables an available mountpath. disabled is set to true if
// mountpath has been moved from available to disabled and exists is set to
// true if such mountpath even exists.
func (mfs *MountedFS) DisableMountpath(mpath string) (disabled, exists bool) {
	mpath = filepath.Clean(mpath)
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mpathInfo, ok := mfs.Available[mpath]; ok {
		if mfs.Disabled == nil {
			mfs.Disabled = make(map[string]*MountpathInfo)
		}

		mfs.Disabled[mpath] = mpathInfo
		delete(mfs.Available, mpath)
		mfs.recalcCache()
		return true, true
	}

	if _, ok := mfs.Disabled[mpath]; ok {
		return false, true
	}

	return
}

// Mountpaths returns both available and disabled mountpaths.
func (mfs *MountedFS) Mountpaths() (avail []*MountpathInfo, disabled []*MountpathInfo) {
	return mfs.avail(), mfs.disabled()
}

// Pprint returns pretty printed string of current MountedFS structure.
func (mfs *MountedFS) Pprint() string {
	mfs.mu.RLock()
	s, _ := json.MarshalIndent(mfs, "", "\t")
	mfs.mu.RUnlock()
	return fmt.Sprintln(string(s))
}

func (mfs *MountedFS) avail() []*MountpathInfo {
	return *(*[]*MountpathInfo)(atomic.LoadPointer(&mfs.availableCache))
}

func (mfs *MountedFS) disabled() []*MountpathInfo {
	return *(*[]*MountpathInfo)(atomic.LoadPointer(&mfs.disabledCache))
}

// NOTE: This method should be used under mu.Lock
func (mfs *MountedFS) recalcCache() {
	l, i := len(mfs.Available), 0
	avail := make([]*MountpathInfo, l, l)
	for _, mpathInfo := range mfs.Available {
		avail[i] = mpathInfo
		i++
	}

	atomic.StorePointer(&mfs.availableCache, unsafe.Pointer(&avail))

	l, i = len(mfs.Disabled), 0
	disabled := make([]*MountpathInfo, l, l)
	for _, mpathInfo := range mfs.Disabled {
		disabled[i] = mpathInfo
		i++
	}

	atomic.StorePointer(&mfs.disabledCache, unsafe.Pointer(&disabled))
}

func (mfs *MountedFS) checkFsidDuplicates() error {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	fsmap := make(map[syscall.Fsid]string, len(mfs.Available)+len(mfs.Disabled))
	for _, mpath := range mfs.Available {
		mpathInfo, ok := fsmap[mpath.Fsid]
		if ok {
			return fmt.Errorf("duplicate FSID %v: mpath1 %q, mpath2 %q", mpath.Fsid, mpath.OrigPath, mpathInfo)
		}
		fsmap[mpath.Fsid] = mpath.Path
	}

	for _, mpath := range mfs.Disabled {
		mpathInfo, ok := fsmap[mpath.Fsid]
		if ok {
			return fmt.Errorf("duplicate FSID %v: mpath1 %q, mpath2 %q", mpath.Fsid, mpath.OrigPath, mpathInfo)
		}
		fsmap[mpath.Fsid] = mpath.Path
	}

	return nil
}
