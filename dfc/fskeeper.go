// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

const (
	fsCheckInterval     = time.Second * 30
	tmpDirnameTemplate  = "DFC-TEMP-DIR"
	tmpFilenameTemplate = "DFC-TEMP-FILE"
)

type fskeeper struct {
	namedrunner
	checknow chan error
	chstop   chan struct{}
	atomic   int64
	okmap    *okmap
	t        *targetrunner
}

// construction
func newfskeeper(t *targetrunner) *fskeeper {
	return &fskeeper{t: t}
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

	interval := ctx.config.FSKeeper.FSCheckTime
	if _, avail := ctx.mountpaths.available[mpath]; !avail {
		interval = ctx.config.FSKeeper.OfflineFSCheckTime
	}

	return time.Since(last) < interval
}

func (k *fskeeper) run() error {
	glog.Infof("Starting %s", k.name)
	k.chstop = make(chan struct{}, 16)
	k.checknow = make(chan error, 16)
	k.okmap = &okmap{okmap: make(map[string]time.Time, 16)}
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
	for _, mp := range ctx.mountpaths.available {
		if err == nil && k.skipCheck(mp.Path) {
			continue
		}

		ok := k.pathTest(mp.Path)
		if !ok {
			glog.Errorf("Mountpath %s is unavailable. Disabling it...", mp.Path)
			ctx.mountpaths.Lock()
			delete(ctx.mountpaths.available, mp.Path)
			ctx.mountpaths.offline[mp.Path] = mp
			ctx.mountpaths.updateOrderedList()
			ctx.mountpaths.Unlock()
		}
		k.timestamp(mp.Path)
	}
}

func (k *fskeeper) checkOfflinePaths(err error) {
	for _, mp := range ctx.mountpaths.offline {
		if err == nil && k.skipCheck(mp.Path) {
			continue
		}

		ok := k.pathTest(mp.Path)
		if ok {
			glog.Infof("Mountpath %s is back. Enabling it...", mp.Path)
			ctx.mountpaths.Lock()
			delete(ctx.mountpaths.offline, mp.Path)
			ctx.mountpaths.available[mp.Path] = mp
			ctx.mountpaths.updateOrderedList()
			ctx.mountpaths.Unlock()
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

	if err != nil || ctx.config.FSKeeper.FSCheckTime != 0 {
		k.checkAlivePaths(err)
	}
	if ctx.config.FSKeeper.OfflineFSCheckTime != 0 {
		k.checkOfflinePaths(err)
	}

	if len(ctx.mountpaths.available) == 0 && len(ctx.mountpaths.offline) != 0 {
		glog.Fatal("All mounted filesystems are down")
	}
}

func (k *fskeeper) pathTest(mountpath string) (ok bool) {
	tmpdir, err := ioutil.TempDir(mountpath, tmpDirnameTemplate)
	if err != nil {
		glog.Errorf("Failed to create temporary directory: %v", err)
		return false
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			glog.Errorf("Failed to clean up temporary directory: %v", err)
		}
	}()

	tmpfilename := k.t.fqn2workfile(path.Join(tmpdir, tmpFilenameTemplate))
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

	if _, err := tmpfile.Write([]byte("temporary file content")); err != nil {
		glog.Errorf("Failed to write to file %s: %v", tmpfile.Name(), err)
		return false
	}

	return true
}
