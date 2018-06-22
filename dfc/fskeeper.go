// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	fsCheckInterval  = time.Second * 30
	tmpNameTemplate  = "DFC-TMP"
	tmpLargeFileSize = 10 * 1024 * 1024
	tmpFileChunkSize = 16 * 1024
)

type okmap struct {
	sync.Mutex
	okmap map[string]time.Time
}

type fskeeper struct {
	namedrunner
	checknow   chan error
	chstop     chan struct{}
	atomic     int64
	okmap      *okmap
	fnTempName func(string) string

	// pointers to common data
	config     *fskeeperconf
	mountpaths *mountedFS
}

// construction
func newFSKeeper(conf *fskeeperconf,
	mounts *mountedFS, f func(string) string) *fskeeper {
	return &fskeeper{
		config:     conf,
		mountpaths: mounts,
		chstop:     make(chan struct{}, 4),
		checknow:   make(chan error, 16),
		okmap:      &okmap{okmap: make(map[string]time.Time, 16)},
		fnTempName: f,
	}
}

//=========================================================
//
// common methods
//
//=========================================================
func (k *fskeeper) onerr(err error) {
	k.checknow <- err
}

func (k *fskeeper) timestamp(mpath string) {
	k.okmap.Lock()
	k.okmap.okmap[mpath] = time.Now()
	k.okmap.Unlock()
}

func (k *fskeeper) skipCheck(mpath string) bool {
	k.okmap.Lock()
	last, ok := k.okmap.okmap[mpath]
	k.okmap.Unlock()

	if !ok {
		return false
	}

	interval := k.config.FSCheckTime
	if _, avail := k.mountpaths.Available[mpath]; !avail {
		interval = k.config.OfflineFSCheckTime
	}

	return time.Since(last) < interval
}

func (k *fskeeper) run() error {
	glog.Infof("Starting %s", k.name)
	ticker := time.NewTicker(fsCheckInterval)
	for {
		select {
		case <-ticker.C:
			k.checkPaths(nil)
		case err := <-k.checknow:
			k.checkPaths(err)
		case <-k.chstop:
			ticker.Stop()
			return nil
		}
	}
}

func (k *fskeeper) stop(err error) {
	glog.Infof("Stopping %s, err: %v", k.name, err)
	var v struct{}
	k.chstop <- v
	close(k.chstop)
}

func (k *fskeeper) checkAlivePaths(err error) {
	for _, mp := range k.mountpaths.Available {
		if err == nil && k.skipCheck(mp.Path) {
			continue
		}

		ok := k.pathTest(mp.Path, err == nil)
		if !ok {
			glog.Errorf("Mountpath %s is unavailable. Disabling it...", mp.Path)
			k.mountpaths.Lock()
			delete(k.mountpaths.Available, mp.Path)
			k.mountpaths.Offline[mp.Path] = mp
			k.mountpaths.Unlock()
		}
		k.timestamp(mp.Path)
	}
}

func (k *fskeeper) checkOfflinePaths(err error) {
	for _, mp := range k.mountpaths.Offline {
		if err == nil && k.skipCheck(mp.Path) {
			continue
		}

		ok := k.pathTest(mp.Path, true)
		if ok {
			glog.Infof("Mountpath %s is back. Enabling it...", mp.Path)
			k.mountpaths.Lock()
			delete(k.mountpaths.Offline, mp.Path)
			k.mountpaths.Available[mp.Path] = mp
			k.mountpaths.Unlock()
		}
		k.timestamp(mp.Path)
	}
}

func (k *fskeeper) checkPaths(err error) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&k.atomic, 0, aval) {
		glog.Infof("Path check is in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&k.atomic, aval, 0)
	if err != nil {
		glog.Infof("Path check: got err %v, checking now...", err)
	}

	if err != nil || k.config.FSCheckTime != 0 {
		k.checkAlivePaths(err)
	}
	if k.config.OfflineFSCheckTime != 0 {
		k.checkOfflinePaths(err)
	}

	if len(k.mountpaths.Available) == 0 {
		glog.Fatal("All mounted filesystems are down")
	}
}

func (k *fskeeper) pathTest(mountpath string, fastCheck bool) (ok bool) {
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

	if fastCheck {
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
