// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

const (
	addMpathAct     = "Added"
	enableMpathAct  = "Enabled"
	removeMpathAct  = "Removed"
	disableMpathAct = "Disabled"
)

type (
	fsprungroup struct {
		t *targetrunner
	}
)

func (g *fsprungroup) init(t *targetrunner) {
	g.t = t
}

// enableMountpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) enableMountpath(mpath string) (enabled bool, err error) {
	var (
		gfnActive    = g.t.gfn.local.Activate()
		enabledMpath *fs.MountpathInfo
	)
	if enabledMpath, err = fs.Enable(mpath); err != nil || enabledMpath == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return false, err
	}

	g.addMpathEvent(enableMpathAct, enabledMpath)
	return true, nil
}

// disableMountpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) disableMountpath(mpath string) (disabled bool, err error) {
	var (
		gfnActive     = g.t.gfn.local.Activate()
		disabledMpath *fs.MountpathInfo
	)
	if disabledMpath, err = fs.Disable(mpath); err != nil || disabledMpath == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return false, err
	}

	g.delMpathEvent(disableMpathAct, disabledMpath)
	return true, nil
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) addMountpath(mpath string) (err error) {
	var (
		gfnActive  = g.t.gfn.local.Activate()
		addedMpath *fs.MountpathInfo
	)
	if addedMpath, err = fs.Add(mpath); err != nil || addedMpath == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g.addMpathEvent(addMpathAct, addedMpath)
	return
}

// removeMountpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) removeMountpath(mpath string) (err error) {
	var (
		gfnActive    = g.t.gfn.local.Activate()
		removedMpath *fs.MountpathInfo
	)
	if removedMpath, err = fs.Remove(mpath); err != nil || removedMpath == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g.delMpathEvent(removeMpathAct, removedMpath)
	return
}

func (g *fsprungroup) addMpathEvent(action string, mpath *fs.MountpathInfo) {
	xreg.AbortAllMountpathsXactions()
	go func() {
		g.t.runResilver("", false /*skipGlobMisplaced*/)
		xreg.RenewMakeNCopies(g.t, "add-mp")
	}()

	g.t.owner.bmd.persist()
	if err := fs.CreateVMD(g.t.si.ID()).Persist(); err != nil {
		cmn.ExitLogf("%v", err.Error())
	}

	g.checkEnable(action, mpath.Path)
}

func (g *fsprungroup) delMpathEvent(action string, mpath *fs.MountpathInfo) {
	xreg.AbortAllMountpathsXactions()

	go mpath.EvictLomCache()
	fs.ClearMDOnMpath(mpath) // clear AIS metadata

	if g.checkZeroMountpaths(action) {
		return
	}

	go func() {
		g.t.runResilver("", false /*skipGlobMisplaced*/)
		xreg.RenewMakeNCopies(g.t, "del-mp")
	}()
}

// Check for no mountpaths and unregister(disable) the target if detected.
func (g *fsprungroup) checkZeroMountpaths(action string) (disabled bool) {
	availablePaths, _ := fs.Get()
	if len(availablePaths) > 0 {
		return false
	}
	if err := g.t.disable(); err != nil {
		glog.Errorf("%s the last available mountpath, failed to unregister target %s (self), err: %v", action, g.t.si, err)
	} else {
		glog.Errorf("%s the last available mountpath and unregistered target %s (self)", action, g.t.si)
	}
	return true
}

func (g *fsprungroup) checkEnable(action, mpath string) {
	availablePaths, _ := fs.Get()
	if len(availablePaths) > 1 {
		glog.Infof("%s mountpath %s", action, mpath)
	} else {
		glog.Infof("%s the first mountpath %s", action, mpath)
		if err := g.t.enable(); err != nil {
			glog.Errorf("Failed to re-register %s (self), err: %v", g.t.si, err)
		}
	}
}
