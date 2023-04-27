// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/volume"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

type fsprungroup struct {
	t      *target
	newVol bool
}

func (g *fsprungroup) init(t *target, newVol bool) {
	g.t = t
	g.newVol = newVol
}

//
// add | re-enable
//

// enableMpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) enableMpath(mpath string) (enabledMi *fs.Mountpath, err error) {
	enabledMi, err = fs.EnableMpath(mpath, g.t.SID(), g.redistributeMD)
	if err != nil || enabledMi == nil {
		return
	}
	g._postAdd(apc.ActMountpathEnable, enabledMi)
	return
}

// attachMpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) attachMpath(mpath string, force bool) (addedMi *fs.Mountpath, err error) {
	addedMi, err = fs.AddMpath(mpath, g.t.SID(), g.redistributeMD, force)
	if err != nil || addedMi == nil {
		return
	}

	g._postAdd(apc.ActMountpathAttach, addedMi)
	return
}

func (g *fsprungroup) _postAdd(action string, mi *fs.Mountpath) {
	// TODO 1: Currently, dSort doesn't handle adding/enabling mountpaths at runtime
	// TODO 2: Integrate with xreg.LimitedCoexistence
	dsort.Managers.AbortAll(fmt.Errorf("%q %s", action, mi))

	fspathsConfigAddDel(mi.Path, true /*add*/)
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
func (g *fsprungroup) disableMpath(mpath string, dontResilver bool) (*fs.Mountpath, error) {
	return g.doDD(apc.ActMountpathDisable, fs.FlagBeingDisabled, mpath, dontResilver)
}

// detachMpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) detachMpath(mpath string, dontResilver bool) (*fs.Mountpath, error) {
	return g.doDD(apc.ActMountpathDetach, fs.FlagBeingDetached, mpath, dontResilver)
}

func (g *fsprungroup) doDD(action string, flags uint64, mpath string, dontResilver bool) (rmi *fs.Mountpath, err error) {
	var numAvail int
	if rmi, numAvail, err = fs.BeginDD(action, flags, mpath); err != nil {
		return
	}
	if rmi == nil {
		return
	}

	// TODO 1: Currently, dSort doesn't handle detaching/disabling mountpaths at runtime
	// TODO 2: Integrate with xreg.LimitedCoexistence
	dsort.Managers.AbortAll(fmt.Errorf("%q %s", action, rmi))

	if numAvail == 0 {
		s := fmt.Sprintf("%s: lost (via %q) the last available mountpath %q", g.t.si, action, rmi)
		g.postDD(rmi, action, nil /*xaction*/, nil /*error*/) // go ahead to disable/detach
		g.t.disable(s)
		return
	}

	rmi.EvictLomCache()

	if dontResilver || !cmn.GCO.Get().Resilver.Enabled {
		glog.Infof("%s: %q %s but resilvering=(%t, %t)", g.t, action, rmi,
			!dontResilver, cmn.GCO.Get().Resilver.Enabled)
		g.postDD(rmi, action, nil /*xaction*/, nil /*error*/) // ditto (compare with the one below)
		return
	}

	prevActive := g.t.res.IsActive(1 /*interval-of-inactivity multiplier*/)
	if prevActive {
		glog.Infof("%s: %q %s: starting to resilver when previous (resilvering) is active", g.t, action, rmi)
	} else {
		glog.Infof("%s: %q %s: starting to resilver", g.t, action, rmi)
	}
	args := res.Args{
		Rmi:             rmi,
		Action:          action,
		PostDD:          g.postDD,    // callback when done
		SingleRmiJogger: !prevActive, // NOTE: optimization for the special/common case
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go g.t.runResilver(args, wg)
	wg.Wait()

	return
}

func (g *fsprungroup) postDD(rmi *fs.Mountpath, action string, xres *xs.Resilver, err error) {
	// 1. handle error
	if err == nil && xres != nil {
		err = xres.AbortErr()
	}
	if err != nil {
		if errCause := cmn.AsErrAborted(err); errCause != nil {
			err = errCause
		}
		if err == cmn.ErrXactUserAbort {
			glog.Errorf("[post-dd interrupted - clearing the state] %s: %q %s %s: %v",
				g.t.si, action, rmi, xres, err)
			rmi.ClearDD()
		} else {
			glog.Errorf("[post-dd interrupted - keeping the state] %s: %q %s %s: %v",
				g.t.si, action, rmi, xres, err)
		}
		return
	}

	// 2. this action
	if action == apc.ActMountpathDetach {
		_, err = fs.Remove(rmi.Path, g.redistributeMD)
	} else {
		debug.Assert(action == apc.ActMountpathDisable)
		_, err = fs.Disable(rmi.Path, g.redistributeMD)
	}
	if err != nil {
		glog.Error(err)
		return
	}
	fspathsConfigAddDel(rmi.Path, false /*add*/)
	glog.Infof("%s: %s %q %s done", g.t, rmi, action, xres)

	// 3. the case of multiple overlapping detach _or_ disable operations
	//    (ie., commit previously aborted xs.Resilver, if any)
	availablePaths := fs.GetAvail()
	for _, mi := range availablePaths {
		if !mi.IsAnySet(fs.FlagWaitingDD) {
			continue
		}
		// TODO: assumption that `action` is the same for all
		if action == apc.ActMountpathDetach {
			_, err = fs.Remove(mi.Path, g.redistributeMD)
		} else {
			debug.Assert(action == apc.ActMountpathDisable)
			_, err = fs.Disable(mi.Path, g.redistributeMD)
		}
		if err != nil {
			glog.Error(err)
			return
		}
		fspathsConfigAddDel(mi.Path, false /*add*/)
		glog.Infof("%s: %s %s %s was previously aborted and now done", g.t, action, mi, xres)
	}
}

// store updated fspaths locally as part of the 'OverrideConfigFname'
// and commit new version of the config
func fspathsConfigAddDel(mpath string, add bool) {
	config := cmn.GCO.Get()
	if config.TestingEnv() { // since testing fspaths are counted, not enumerated
		return
	}
	config = cmn.GCO.BeginUpdate()
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
	// do
	fspathsSave(config)
}

func fspathsSave(config *cmn.Config) {
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
			cos.ExitLog(err)
		}
	}

	if !hasEnoughEtlMDCopies() {
		eo := g.t.owner.etl
		if err := eo.persist(eo.get(), nil); err != nil {
			cos.ExitLog(err)
		}
	}

	if _, err := volume.NewFromMPI(g.t.SID()); err != nil {
		cos.ExitLog(err)
	}
}

func (g *fsprungroup) checkEnable(action, mpath string) {
	availablePaths := fs.GetAvail()
	if len(availablePaths) > 1 {
		glog.Infof("%s mountpath %s", action, mpath)
	} else {
		glog.Infof("%s the first mountpath %s", action, mpath)
		if err := g.t.enable(); err != nil {
			glog.Errorf("Failed to re-join %s (self), err: %v", g.t, err)
		}
	}
}
