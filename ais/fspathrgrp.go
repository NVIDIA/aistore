// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/volume"
	"github.com/NVIDIA/aistore/xreg"
)

type fsprungroup struct {
	t *targetrunner
}

func (g *fsprungroup) init(t *targetrunner) {
	g.t = t
}

//
// add | re-enable
//

// enableMpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) enableMpath(mpath string) (enabledMi *fs.MountpathInfo, err error) {
	enabledMi, err = fs.EnableMpath(mpath, g.t.si.ID(), g.redistributeMD)
	if err != nil || enabledMi == nil {
		return
	}
	g._postAdd(cmn.ActMountpathEnable, enabledMi)
	return
}

// attachMpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) attachMpath(mpath string, force bool) (addedMi *fs.MountpathInfo, err error) {
	addedMi, err = fs.AddMpath(mpath, g.t.si.ID(), g.redistributeMD, force)
	if err != nil || addedMi == nil {
		return
	}

	g._postAdd(cmn.ActMountpathAttach, addedMi)
	return
}

func (g *fsprungroup) _postAdd(action string, mi *fs.MountpathInfo) {
	config := cmn.GCO.Get()
	if !config.TestingEnv() { // as testing fspaths are counted, not enumerated
		fspathsSaveCommit(mi.Path, true /*add*/)
	}
	go func() {
		if cmn.GCO.Get().Resilver.Enabled {
			g.t.runResilver(res.Args{}, nil /*wg*/)
		}
		xreg.RenewMakeNCopies(g.t, cos.GenUUID(), action)
	}()

	g.checkEnable(action, mi.Path)

	tstats := g.t.statsT.(*stats.Trunner)
	for _, disk := range mi.Disks {
		tstats.RegDiskMetrics(disk)
	}
}

//
// remove | disable
//

// disableMpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) disableMpath(mpath string, dontResilver bool) (rmi *fs.MountpathInfo, err error) {
	var nothingToDo bool
	if nothingToDo, err = g._preDD(cmn.ActMountpathDisable, fs.FlagBeingDisabled, mpath, dontResilver); err != nil {
		return
	}
	if nothingToDo {
		return
	}
	rmi, err = fs.Disable(mpath, g.redistributeMD)
	if err != nil || rmi == nil {
		return
	}
	g._postDD(cmn.ActMountpathDisable, rmi)
	return
}

// detachMpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) detachMpath(mpath string, dontResilver bool) (rmi *fs.MountpathInfo, err error) {
	var nothingToDo bool
	if nothingToDo, err = g._preDD(cmn.ActMountpathDetach, fs.FlagBeingDetached, mpath, dontResilver); err != nil {
		return
	}
	if nothingToDo {
		return
	}
	rmi, err = fs.Remove(mpath, g.redistributeMD)
	if err != nil || rmi == nil {
		return
	}
	g._postDD(cmn.ActMountpathDetach, rmi)
	return
}

func (g *fsprungroup) _preDD(action string, flags uint64, mpath string, dontResilver bool) (nothingToDo bool, err error) {
	var (
		rmi      *fs.MountpathInfo
		numAvail int
	)
	if rmi, numAvail, err = fs.BeginDD(action, flags, mpath); err != nil {
		return
	}
	if rmi == nil {
		nothingToDo = true
		return
	}
	if numAvail == 0 {
		s := fmt.Sprintf("%s: lost (via %q) the last available mountpath", g.t.si, action)
		g.t.disable(s) // TODO: handle failure to remove self from Smap
		return
	}

	rmi.EvictLomCache()

	if dontResilver || !cmn.GCO.Get().Resilver.Enabled {
		glog.Infof("%s: %q %s but resilvering is globally disabled, nothing to do", g.t.si, action, rmi)
		return
	}

	if g.t.res.IsActive() {
		glog.Infof("%s: %q %s - starting to resilver", g.t.si, action, rmi)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go g.t.runResilver(res.Args{}, wg)
		wg.Wait()
		return
	}
	// otherwise, block on this single mountpath (NOTE: optimization for special case)
	glog.Infof("%s: %q - resilvering data off of the %s", g.t.si, action, rmi)
	g.t.runResilver(res.Args{Mpath: rmi.Path}, nil /*wg*/)
	return
}

func (g *fsprungroup) _postDD(action string, mi *fs.MountpathInfo) {
	config := cmn.GCO.Get()
	if !config.TestingEnv() { // testing fspaths are counted, not enumerated
		fspathsSaveCommit(mi.Path, false /*add*/)
	}
	glog.Infof("%s: %s %q done (resilver-is-running=%t)", g.t.si, mi, action, g.t.res.IsActive())
}

// store updated fspaths locally as part of the 'OverrideConfigFname'
// and commit new version of the config
func fspathsSaveCommit(mpath string, add bool) {
	config := cmn.GCO.BeginUpdate()
	localConfig := &config.LocalConfig
	if add {
		localConfig.AddPath(mpath)
	} else {
		localConfig.DelPath(mpath)
	}
	if err := localConfig.FSP.Validate(config); err != nil {
		debug.AssertNoErr(err)
		cmn.GCO.DiscardUpdate()
		glog.Error(err)
		return
	}
	toUpdate := &cmn.ConfigToUpdate{FSP: &config.LocalConfig.FSP}
	overrideConfig := cmn.GCO.SetLocalFSPaths(toUpdate)
	if err := cmn.SaveOverrideConfig(config.ConfigDir, overrideConfig); err != nil {
		debug.AssertNoErr(err)
		cmn.GCO.DiscardUpdate()
		glog.Error(err)
		return
	}
	cmn.GCO.CommitUpdate(config)
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
	if _, err := volume.NewFromMPI(g.t.si.ID()); err != nil {
		debug.AssertNoErr(err)
		cos.ExitLogf("%v", err)
	}
}

func (g *fsprungroup) checkEnable(action, mpath string) {
	availablePaths := fs.GetAvail()
	if len(availablePaths) > 1 {
		glog.Infof("%s mountpath %s", action, mpath)
	} else {
		glog.Infof("%s the first mountpath %s", action, mpath)
		if err := g.t.enable(); err != nil {
			glog.Errorf("Failed to re-join %s (self), err: %v", g.t.si, err)
		}
	}
}
