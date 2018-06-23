// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	fsCheckInterval  = time.Second * 30
	tmpNameTemplate  = "DFC-TMP"
	tmpLargeFileSize = 10 * 1024 * 1024
	tmpFileChunkSize = 256 * 1024
)

type (
	fsKeepAliveStats struct {
		lastCheck      time.Time
		failedFilename string
	}
	fsKeepAliveMap struct {
		sync.Mutex
		kmap map[string]fsKeepAliveStats
	}

	fsKeeper struct {
		namedrunner
		checkNow   chan string
		chStop     chan struct{}
		atomic     int64
		fsMap      *fsKeepAliveMap
		fnTempName func(string) string

		// pointers to common data
		config     *fskeeperconf
		mountpaths *mountedFS
	}
)

func newFSKeeper(conf *fskeeperconf,
	mounts *mountedFS, f func(string) string) *fsKeeper {
	return &fsKeeper{
		config:     conf,
		mountpaths: mounts,
		chStop:     make(chan struct{}, 4),
		checkNow:   make(chan string, 16),
		fsMap:      &fsKeepAliveMap{kmap: make(map[string]fsKeepAliveStats, 16)},
		fnTempName: f,
	}
}

func (k *fsKeeper) onerr(filepath string) {
	k.checkNow <- filepath
}

// updates the last time when the mountpoint was checked
func (k *fsKeeper) setLastChecked(mpath string) {
	k.fsMap.Lock()
	kstats, ok := k.fsMap.kmap[mpath]
	if !ok {
		kstats = fsKeepAliveStats{}
	}
	kstats.lastCheck = time.Now()
	k.fsMap.kmap[mpath] = kstats
	k.fsMap.Unlock()
}

// returns the last time when the mountpoint was checked and if the mountpoint
// was checked before
func (k *fsKeeper) getLastChecked(mpath string) (lastCheck time.Time, ok bool) {
	k.fsMap.Lock()
	kstats, ok := k.fsMap.kmap[mpath]
	if ok {
		lastCheck = kstats.lastCheck
	}
	k.fsMap.Unlock()
	return lastCheck, ok
}

// returns mountpoint for the filename or empty string if the file does not
// belong any of available mountpoints
func (k *fsKeeper) filenameToMpath(filename string) string {
	for key := range k.mountpaths.Available {
		// add / to avoid confusion between mountpoints '/a' and '/aa' etc
		if strings.Contains(filename, key+"/") {
			return key
		}
	}

	return ""
}

// saves the filename for next reading tests
func (k *fsKeeper) setFailedFilename(mpath, filename string) {
	if mpath == "" {
		return
	}

	k.fsMap.Lock()
	kstats, ok := k.fsMap.kmap[mpath]
	if !ok {
		kstats = fsKeepAliveStats{}
	}
	kstats.failedFilename = filename
	k.fsMap.kmap[mpath] = kstats
	k.fsMap.Unlock()

	return
}

// returns the name of a file that generated an error when accessing
// mountpoint and if the mountpoint was checked before
func (k *fsKeeper) getFailedFilename(mpath string) string {
	var filename string
	k.fsMap.Lock()
	if kstats, ok := k.fsMap.kmap[mpath]; ok {
		filename = kstats.failedFilename
	}
	k.fsMap.Unlock()
	return filename
}

// returns true if the mountpoint was recently checked and should be skipped
// this run
func (k *fsKeeper) skipCheck(mpath string) bool {
	lastCheck, ok := k.getLastChecked(mpath)
	if !ok {
		return false
	}

	interval := k.config.FSCheckTime
	if _, avail := k.mountpaths.Available[mpath]; !avail {
		interval = k.config.OfflineFSCheckTime
	}

	return time.Since(lastCheck) < interval
}

func (k *fsKeeper) run() error {
	glog.Infof("Starting %s", k.name)
	ticker := time.NewTicker(fsCheckInterval)
	for {
		select {
		case <-ticker.C:
			k.checkPaths("")
		case filepath := <-k.checkNow:
			k.checkPaths(filepath)
		case <-k.chStop:
			ticker.Stop()
			return nil
		}
	}
}

func (k *fsKeeper) stop(err error) {
	glog.Infof("Stopping %s, err: %v", k.name, err)
	var v struct{}
	k.chStop <- v
	close(k.chStop)
}

func (k *fsKeeper) checkOneAlivePath(mpath string, quickCheck bool) {
	ok := k.pathTest(mpath, quickCheck)
	if !ok {
		glog.Errorf("Mountpath %s is unavailable. Disabling it...", mpath)
		k.mountpaths.Lock()
		mp := k.mountpaths.Available[mpath]
		delete(k.mountpaths.Available, mpath)
		k.mountpaths.Offline[mpath] = mp
		k.mountpaths.Unlock()
	}
	k.setLastChecked(mpath)
}

// checkAlivePaths can be called on timer or by any DFC function when file
// reading/writing error is detected. In the former case filepath is empty,
// in the latter case the filepath is path to file DFC failed to read/write.
// In case of filepath belongs to any existing and available mountpoints the
// function does a quick check: it tests only one mountpoint that contains
// the failed file
func (k *fsKeeper) checkAlivePaths(filepath string) {
	if filepath != "" {
		mpath := k.filenameToMpath(filepath)
		if mpath != "" {
			k.setFailedFilename(mpath, filepath)
			k.checkOneAlivePath(mpath, false)
			return
		}
	}

	for _, mp := range k.mountpaths.Available {
		if filepath == "" && k.skipCheck(mp.Path) {
			continue
		}

		k.checkOneAlivePath(mp.Path, filepath == "")
	}
}

// checkOfflinePaths tests only previously disabled mountpoints - just in case
// of any of them comes back. Passing non-nil error makes the function recheck
// all disabled mountpoints immediately
func (k *fsKeeper) checkOfflinePaths(filepath string) {
	for _, mp := range k.mountpaths.Offline {
		if filepath == "" && k.skipCheck(mp.Path) {
			continue
		}

		ok := k.pathTest(mp.Path, false)
		if ok {
			glog.Infof("Mountpath %s is back. Enabling it...", mp.Path)
			k.mountpaths.Lock()
			delete(k.mountpaths.Offline, mp.Path)
			k.mountpaths.Available[mp.Path] = mp
			k.mountpaths.Unlock()
			k.setFailedFilename(mp.Path, "")
		}
		k.setLastChecked(mp.Path)
	}
}

// a core function called periodically by fsKeeper daemon
func (k *fsKeeper) checkPaths(filepath string) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&k.atomic, 0, aval) {
		glog.Infof("Path check is in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&k.atomic, aval, 0)
	if filepath != "" {
		glog.Infof("Checking now path %s", filepath)
	}

	if filepath != "" || k.config.FSCheckTime != 0 {
		k.checkAlivePaths(filepath)
	}
	if k.config.OfflineFSCheckTime != 0 {
		k.checkOfflinePaths(filepath)
	}

	if len(k.mountpaths.Available) == 0 {
		glog.Fatal("All mounted filesystems are down")
	}
}

func (k *fsKeeper) isFailedFileReadable(mpath string) bool {
	filename := k.getFailedFilename(mpath)
	if filename == "" {
		return false
	}

	file, err := os.Open(filename)
	if err != nil {
		return false
	}

	buf := make([]byte, tmpFileChunkSize, tmpFileChunkSize)
	for {
		_, err := file.Read(buf)
		if err != nil {
			file.Close()
			return err == io.EOF
		}
	}
}

// A function to detect availability of a path
// Quick check:
//	- create a temporary directory inside the path
//  - create a small random file inside temporary directory
// Long check:
//	- if a file was the cause the path was disabled then try to read
//		that file once more
//	- if previous step fails then try to create a big file in temporary
//		directory inside the path
func (k *fsKeeper) pathTest(mountpath string, quickCheck bool) (ok bool) {
	// first, check if the file that causes an error can be read
	if !quickCheck && k.isFailedFileReadable(mountpath) {
		return true
	}

	// second, try to create something on a mountpoint
	tmpdir, err := ioutil.TempDir(mountpath, tmpNameTemplate)
	if err != nil {
		glog.Errorf("Failed to create temporary directory: %v", err)
		return false
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			glog.Errorf("Failed to clean up temporary directory: %v", err)
		}
	}()

	tmpfilename := k.fnTempName(path.Join(tmpdir, tmpNameTemplate))
	tmpfile, err := os.OpenFile(tmpfilename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		glog.Errorf("Failed to create temporary file: %v", err)
		return false
	}
	defer func() {
		if err := tmpfile.Close(); err != nil {
			glog.Errorf("Failed to close tempory file %s: %v", tmpfile.Name(), err)
		}
	}()

	if quickCheck {
		if _, err := tmpfile.Write([]byte("temporary file content")); err != nil {
			glog.Errorf("Failed to write to file %s: %v", tmpfile.Name(), err)
			return false
		}
	} else {
		chunk := make([]byte, tmpFileChunkSize, tmpFileChunkSize)
		writeCnt := tmpLargeFileSize / tmpFileChunkSize
		for i := 0; i < writeCnt; i++ {
			if _, err := tmpfile.Write(chunk); err != nil {
				glog.Errorf("Failed to write to file %s: %v", tmpfile.Name(), err)
				return false
			}
		}
	}

	return true
}
