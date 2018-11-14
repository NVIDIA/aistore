/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/fs"
)

type (
	fsprungroup struct {
		t       *targetrunner
		runners []fs.PathRunner // subgroup of the ctx.runners rungroup
	}
)

func (g *fsprungroup) init(t *targetrunner) {
	g.t = t
	g.runners = make([]fs.PathRunner, 0, 4)
}

func (g *fsprungroup) add(r fs.PathRunner) {
	g.runners = append(g.runners, r)
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
			glog.Errorf("Failed to re-register %s (self), err: %v", g.t.si.DaemonID, err)
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

	glog.Warningf("The last available mountpath has been disabled: unregistering %s (self)", g.t.si.DaemonID)
	if err := g.t.disable(); err != nil {
		glog.Errorf("Failed to unregister %s (self), err: %v", g.t.si.DaemonID, err)
	}
	return
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) addMountpath(mpath string) (err error) {
	if err = fs.Mountpaths.Add(mpath); err != nil {
		return
	}

	err = g.t.createBucketDirs("local", ctx.config.LocalBuckets, fs.Mountpaths.MakePathLocal)
	if err != nil {
		return
	}
	err = g.t.createBucketDirs("cloud", ctx.config.CloudBuckets, fs.Mountpaths.MakePathCloud)
	if err != nil {
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
			glog.Errorf("Failed to re-register %s (self), err: %v", g.t.si.DaemonID, err)
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
			glog.Errorf("Failed to unregister %s (self), err: %v", g.t.si.DaemonID, err)
		}
	}
	return
}
