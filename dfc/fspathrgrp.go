/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

type (
	fsprunner interface {
		cmn.Runner
		reqAddMountpath(mpath string)
		reqRemoveMountpath(mpath string)
		reqEnableMountpath(mpath string)
		reqDisableMountpath(mpath string)
	}

	fsprungroup struct {
		t       *targetrunner
		runners []fsprunner // subgroup of the ctx.runners rungroup
	}
)

func (g *fsprungroup) init(t *targetrunner) {
	g.t = t
	g.runners = make([]fsprunner, 0, 4)
}

func (g *fsprungroup) add(r fsprunner) {
	g.runners = append(g.runners, r)
}

// enableMountpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) enableMountpath(mpath string) (enabled, exists bool) {
	enabled, exists = fs.Mountpaths.EnableMountpath(mpath)
	if !enabled || !exists {
		return
	}

	for _, r := range g.runners {
		r.reqEnableMountpath(mpath)
	}
	glog.Infof("Re-enabled mountpath %s", mpath)
	go g.t.runLocalRebalance()

	availablePaths, _ := fs.Mountpaths.Mountpaths()
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
	disabled, exists = fs.Mountpaths.DisableMountpath(mpath)
	if !disabled || !exists {
		return
	}

	for _, r := range g.runners {
		r.reqDisableMountpath(mpath)
	}
	glog.Infof("Disabled mountpath %s", mpath)

	availablePaths, _ := fs.Mountpaths.Mountpaths()
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
	if err = fs.Mountpaths.AddMountpath(mpath); err != nil {
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
		r.reqAddMountpath(mpath)
	}
	go g.t.runLocalRebalance()

	availablePaths, _ := fs.Mountpaths.Mountpaths()
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
	if err = fs.Mountpaths.RemoveMountpath(mpath); err != nil {
		return
	}

	for _, r := range g.runners {
		r.reqRemoveMountpath(mpath)
	}

	availablePaths, _ := fs.Mountpaths.Mountpaths()
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
