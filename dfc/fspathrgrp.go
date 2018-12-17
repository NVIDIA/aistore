// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

type (
	// implements fs.PathRunGroup interface
	fsprungroup struct {
		sync.Mutex
		t       *targetrunner
		runners map[int64]fs.PathRunner // subgroup of the ctx.runners rungroup
		nextid  int64
	}
)

func (g *fsprungroup) init(t *targetrunner) {
	g.t = t
	g.runners = make(map[int64]fs.PathRunner, 8)
	g.nextid = time.Now().UTC().UnixNano() & 0xfff
}

func (g *fsprungroup) UID() int64 { return atomic.AddInt64(&g.nextid, 1) }

func (g *fsprungroup) Reg(r fs.PathRunner) {
	g.Lock()
	_, ok := g.runners[r.ID()]
	cmn.Assert(!ok)
	r.SetID(g.UID())
	g.runners[r.ID()] = r
	g.Unlock()
}

func (g *fsprungroup) Unreg(r fs.PathRunner) {
	g.Lock()
	_, ok := g.runners[r.ID()]
	cmn.Assert(ok)
	delete(g.runners, r.ID())
	g.Unlock()
}

// enableMountpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) enableMountpath(mpath string) (enabled, exists bool) {
	enabled, exists = fs.Mountpaths.Enable(mpath)
	if !enabled || !exists {
		return
	}

	for _, r := range g.runners {
		r.ReqEnableMountpath(mpath)
	}
	glog.Infof("Re-enabled mountpath %s", mpath)
	go g.t.runLocalRebalance()

	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) == 1 {
		if err := g.t.enable(); err != nil {
			glog.Errorf("Failed to re-register %s (self), err: %v", g.t.si, err)
		}
	}
	return
}

// disableMountpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) disableMountpath(mpath string) (disabled, exists bool) {
	disabled, exists = fs.Mountpaths.Disable(mpath)
	if !disabled || !exists {
		return
	}

	for _, r := range g.runners {
		r.ReqDisableMountpath(mpath)
	}
	glog.Infof("Disabled mountpath %s", mpath)

	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) > 0 {
		return
	}

	glog.Warningf("The last available mountpath has been disabled: unregistering %s (self)", g.t.si)
	if err := g.t.disable(); err != nil {
		glog.Errorf("Failed to unregister %s (self), err: %v", g.t.si, err)
	}
	return
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) addMountpath(mpath string) (err error) {
	if err = fs.Mountpaths.Add(mpath); err != nil {
		return
	}

	if err = fs.Mountpaths.CreateBucketDir(fs.BucketLocalType); err != nil {
		return
	}
	if err = fs.Mountpaths.CreateBucketDir(fs.BucketCloudType); err != nil {
		return
	}

	for _, r := range g.runners {
		r.ReqAddMountpath(mpath)
	}
	go g.t.runLocalRebalance()

	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) > 1 {
		glog.Infof("Added mountpath %s", mpath)
	} else {
		glog.Infof("Added the first mountpath %s", mpath)
		if err := g.t.enable(); err != nil {
			glog.Errorf("Failed to re-register %s (self), err: %v", g.t.si, err)
		}
	}
	return
}

// removeMountpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) removeMountpath(mpath string) (err error) {
	if err = fs.Mountpaths.Remove(mpath); err != nil {
		return
	}

	for _, r := range g.runners {
		r.ReqRemoveMountpath(mpath)
	}

	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) > 0 {
		glog.Infof("Removed mountpath %s", mpath)
	} else {
		glog.Infof("Removed the last mountpath %s", mpath)
		if err := g.t.disable(); err != nil {
			glog.Errorf("Failed to unregister %s (self), err: %v", g.t.si, err)
		}
	}
	return
}
