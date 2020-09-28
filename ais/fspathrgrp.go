// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction/registry"
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
	gfnActive := g.t.gfn.local.Activate()
	if enabled, err = fs.Enable(mpath); err != nil || !enabled {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g.addMpathEvent(enableMpathAct, mpath)
	return
}

// disableMountpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) disableMountpath(mpath string) (disabled bool, err error) {
	gfnActive := g.t.gfn.local.Activate()
	if disabled, err = fs.Disable(mpath); err != nil || !disabled {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return disabled, err
	}

	g.delMpathEvent(disableMpathAct)
	return true, nil
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) addMountpath(mpath string) (err error) {
	gfnActive := g.t.gfn.local.Activate()
	if err = fs.Add(mpath); err != nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g.addMpathEvent(addMpathAct, mpath)
	return
}

// removeMountpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) removeMountpath(mpath string) (err error) {
	gfnActive := g.t.gfn.local.Activate()
	if err = fs.Remove(mpath); err != nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g.delMpathEvent(removeMpathAct)
	return
}

func (g *fsprungroup) addMpathEvent(action, mpath string) {
	registry.Registry.AbortAllMountpathsXactions()
	go func() {
		g.t.runResilver("", false /*skipGlobMisplaced*/)
		registry.Registry.MakeNCopiesOnMpathEvent(g.t, "add-mp")
	}()
	g.checkEnable(action, mpath)
}

func (g *fsprungroup) delMpathEvent(action string) {
	registry.Registry.AbortAllMountpathsXactions()
	if g.checkZeroMountpaths(action) {
		return
	}

	go func() {
		g.t.runResilver("", false /*skipGlobMisplaced*/)
		registry.Registry.MakeNCopiesOnMpathEvent(g.t, "del-mp")
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
