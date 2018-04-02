// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io/ioutil"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

const (
	fsCheckInterval = time.Second * 10
)

type diskkeeper struct {
	namedrunner
	checknow chan error
	chstop   chan struct{}
	atomic   int64
	okmap    *okmap
	t        *targetrunner
}

// construction
func newdiskkeeper(t *targetrunner) *diskkeeper {
	k := &diskkeeper{t: t}
	return k
}

//=========================================================
//
// common methods
//
//=========================================================
func (k *diskkeeper) onerr(err error) {
	k.checknow <- err
}

func (k *diskkeeper) timestamp(sid string) {
	k.okmap.Lock()
	k.okmap.okmap[sid] = time.Now()
	k.okmap.Unlock()
}

func (k *diskkeeper) skipCheck(sid string) bool {
	k.okmap.Lock()
	last, ok := k.okmap.okmap[sid]
	k.okmap.Unlock()

	_, avail := ctx.mountpaths.available[sid]
	interval := ctx.config.DiskKeeper.FSCheckTime
	if !avail {
		interval = ctx.config.DiskKeeper.OfflineFSCheckTime
	}

	return ok && time.Since(last) < interval
}

func (k *diskkeeper) run() error {
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

func (k *diskkeeper) stop(err error) {
	glog.Infof("Stopping %s, err: %v", k.name, err)
	var v struct{}
	k.chstop <- v
	close(k.chstop)
}

func (k *diskkeeper) checkAlivePaths(err error) {
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
			// FIXME TODO: just recreate the list of sorted mountpaths and use it everywhere
			for i, mpath := range ctx.mountpaths.availOrdered {
				if mpath == mp.Path {
					copy(ctx.mountpaths.availOrdered[i:], ctx.mountpaths.availOrdered[i+1:])
					ctx.mountpaths.availOrdered = ctx.mountpaths.availOrdered[:len(ctx.mountpaths.availOrdered)-1]
					break
				}
			}
			ctx.mountpaths.Unlock()
		}
		k.timestamp(mp.Path)
	}
}

func (k *diskkeeper) checkOfflinePaths(err error) {
	for _, mp := range ctx.mountpaths.offline {
		if err == nil && k.skipCheck(mp.Path) {
			continue
		}

		ok := k.pathTest(mp.Path)
		if ok {
			glog.Errorf("Mountpath %s is back. Enabling it...", mp.Path)
			ctx.mountpaths.Lock()
			delete(ctx.mountpaths.offline, mp.Path)
			ctx.mountpaths.available[mp.Path] = mp
			ctx.mountpaths.availOrdered = append(ctx.mountpaths.availOrdered, mp.Path)
			sort.Strings(ctx.mountpaths.availOrdered)
			ctx.mountpaths.Unlock()
		}
		k.timestamp(mp.Path)
	}
}

func (k *diskkeeper) checkPaths(err error) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&k.atomic, 0, aval) {
		glog.Infof("Path check is in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&k.atomic, aval, 0)
	if err != nil {
		glog.Infof("Path check: got err %v, checking now...", err)
	}

	if ctx.config.DiskKeeper.FSCheckTime != 0 {
		k.checkAlivePaths(err)
	}
	if ctx.config.DiskKeeper.OfflineFSCheckTime != 0 {
		k.checkOfflinePaths(err)
	}

	// TODO: what to do if all mounts are down?
	if len(ctx.mountpaths.available) == 0 && len(ctx.mountpaths.offline) != 0 {
		glog.Fatal("All mounted filesystems are down")
	}
}

func (k *diskkeeper) pathTest(path string) (ok bool) {
	tmpdir, err := ioutil.TempDir(path, "DFC-TMP")
	if err != nil {
		glog.Errorf("Failed to create temporary directory: %v", err)
		return false
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			glog.Errorf("Failed to clean up temporary directory: %v", err)
		}
	}()

	// FIXME TODO: generate temporary file name to be compatible with
	// isworkfile() - it must return true for temporary file
	tmpfile, err := ioutil.TempFile(tmpdir, "DFC-TMP-FILE")
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
