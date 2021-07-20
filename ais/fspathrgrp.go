// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xreg"
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
func (g *fsprungroup) enableMountpath(mpath string) (enabledMi *fs.MountpathInfo, err error) {
	gfnActive := g.t.gfn.local.Activate()
	enabledMi, err = fs.EnableMpath(mpath, g.t.si.ID(), g.redistributeMD)
	if err != nil || enabledMi == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g._postaddmi(enableMpathAct, enabledMi)
	return
}

// disableMountpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) disableMountpath(mpath string) (disabledMi *fs.MountpathInfo, err error) {
	gfnActive := g.t.gfn.local.Activate()
	disabledMi, err = fs.Disable(mpath, g.redistributeMD)
	if err != nil || disabledMi == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g._postdelmi(disableMpathAct, disabledMi)
	return
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) addMountpath(mpath string) (addedMi *fs.MountpathInfo, err error) {
	gfnActive := g.t.gfn.local.Activate()
	addedMi, err = fs.AddMpath(mpath, g.t.si.ID(), g.redistributeMD)
	if err != nil || addedMi == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g._postaddmi(addMpathAct, addedMi)
	return
}

// removeMountpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) removeMountpath(mpath string) (removedMi *fs.MountpathInfo, err error) {
	gfnActive := g.t.gfn.local.Activate()
	removedMi, err = fs.Remove(mpath, g.redistributeMD)
	if err != nil || removedMi == nil {
		if !gfnActive {
			g.t.gfn.local.Deactivate()
		}
		return
	}

	g._postdelmi(removeMpathAct, removedMi)
	return
}

func (g *fsprungroup) _postaddmi(action string, mi *fs.MountpathInfo) {
	xreg.AbortAllMountpathsXactions()
	go func() {
		if cmn.GCO.Get().Resilver.Enabled {
			g.t.runResilver("", false /*skipGlobMisplaced*/)
		}
		xreg.RenewMakeNCopies(g.t, cos.GenUUID(), "add-mp")
	}()

	g.checkEnable(action, mi.Path)

	tstats := g.t.statsT.(*stats.Trunner)
	for _, disk := range mi.Disks {
		tstats.RegDiskMetrics(disk)
	}
}

func (g *fsprungroup) _postdelmi(action string, mi *fs.MountpathInfo) {
	xreg.AbortAllMountpathsXactions()

	go mi.EvictLomCache()
	if g.checkZeroMountpaths(action) {
		return
	}

	go func() {
		if cmn.GCO.Get().Resilver.Enabled {
			g.t.runResilver("", false /*skipGlobMisplaced*/)
		}
		xreg.RenewMakeNCopies(g.t, cos.GenUUID(), "del-mp")
	}()
}

// NOTE: executes under mfs lock
func (g *fsprungroup) redistributeMD() {
	if !hasEnoughBMDCopies() {
		bo := g.t.owner.bmd
		if err := bo.persist(bo.get(), nil); err != nil {
			debug.AssertNoErr(err)
			cos.ExitLogf("%v", err)
		}
	}
	if _, err := fs.CreateNewVMD(g.t.si.ID()); err != nil {
		debug.AssertNoErr(err)
		cos.ExitLogf("%v", err)
	}
}

// Check for no mountpaths and unregister(disable) the target if detected.
func (g *fsprungroup) checkZeroMountpaths(action string) (disabled bool) {
	availablePaths, _ := fs.Get()
	if len(availablePaths) > 0 {
		return false
	}
	if err := g.t.disable(); err != nil {
		glog.Errorf("%s the last available mountpath, failed to unregister target %s (self), err: %v",
			action, g.t.si, err)
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
