// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
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
// change if mountpath actually was enabled.
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
func (g *fsprungroup) attachMpath(mpath string, label ios.Label) (addedMi *fs.Mountpath, err error) {
	addedMi, err = fs.AddMpath(g.t.SID(), mpath, label, g.redistributeMD)
	if err != nil || addedMi == nil {
		return
	}

	g._postAdd(apc.ActMountpathAttach, addedMi)
	return
}

func (g *fsprungroup) _postAdd(action string, mi *fs.Mountpath) {
	// NOTE:
	// - currently, dsort doesn't handle (add/enable/disable/detach mountpath) at runtime
	// - consider integrating via `xreg.LimitedCoexistence`
	// - review all xact.IsMountpath(kind) == true
	dsort.Managers.AbortAll(fmt.Errorf("%q %s", action, mi))

	fspathsConfigAddDel(mi.Path, true /*add*/)
	go func() {
		if cmn.GCO.Get().Resilver.Enabled {
			g.t.runResilver(res.Args{}, nil /*wg*/)
		}
		xreg.RenewMakeNCopies(cos.GenUUID(), action)
	}()

	g.checkEnable(action, mi)

	tstats := g.t.statsT.(*stats.Trunner)
	for _, disk := range mi.Disks {
		tstats.RegDiskMetrics(g.t.si, disk)
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

//
// rescan and fshc (advanced use)
//

func (g *fsprungroup) rescanMpath(mpath string, dontResilver bool) error {
	avail, disabled := fs.Get()
	mi, ok := avail[mpath]
	if !ok {
		what := mpath
		if mi, ok = disabled[mpath]; ok {
			what = mi.String()
		}
		return fmt.Errorf("%s: not starting rescan-disks: %s is not available", g.t, what)
	}

	if len(mi.Disks) == 0 {
		return fmt.Errorf("%s: not starting rescan-disks: %s has no disks", g.t, mi)
	}

	warn, err := mi.RescanDisks()
	if err != nil || warn == nil {
		return err
	}
	if !dontResilver && cmn.GCO.Get().Resilver.Enabled {
		go g.t.runResilver(res.Args{}, nil /*wg*/)
	}
	return warn
}

func (g *fsprungroup) doDD(action string, flags uint64, mpath string, dontResilver bool) (*fs.Mountpath, error) {
	rmi, numAvail, noResil, err := fs.BeginDD(action, flags, mpath)
	if err != nil || rmi == nil {
		return nil, err
	}

	// NOTE: above
	dsort.Managers.AbortAll(fmt.Errorf("%q %s", action, rmi))

	if numAvail == 0 {
		nlog.Errorf("%s: lost (via %q) the last available mountpath %q", g.t.si, action, rmi)
		g.postDD(rmi, action, nil /*xaction*/, nil /*error*/) // go ahead to disable/detach
		g.t.disable()
		return rmi, nil
	}

	core.UncacheMountpath(rmi)

	if noResil || dontResilver || !cmn.GCO.Get().Resilver.Enabled {
		nlog.Infof("%s: %q %s: no resilvering (%t, %t, %t)", g.t, action, rmi,
			noResil, !dontResilver, cmn.GCO.Get().Resilver.Enabled)
		g.postDD(rmi, action, nil /*xaction*/, nil /*error*/) // ditto (compare with the one below)
		return rmi, nil
	}

	prevActive := g.t.res.IsActive(1 /*interval-of-inactivity multiplier*/)
	if prevActive {
		nlog.Infof("%s: %q %s: starting to resilver when previous (resilvering) is active", g.t, action, rmi)
	} else {
		nlog.Infof("%s: %q %s: starting to resilver", g.t, action, rmi)
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

	return rmi, nil
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
			nlog.Errorf("[post-dd interrupted - clearing the state] %s: %q %s %s: %v",
				g.t.si, action, rmi, xres, err)
			rmi.ClearDD()
		} else {
			nlog.Errorf("[post-dd interrupted - keeping the state] %s: %q %s %s: %v",
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
		nlog.Errorln(err)
		return
	}
	fspathsConfigAddDel(rmi.Path, false /*add*/)
	nlog.Infof("%s: %s %q %s done", g.t, rmi, action, xres)

	// 3. the case of multiple overlapping detach _or_ disable operations
	//    (ie., commit previously aborted xs.Resilver, if any)
	avail := fs.GetAvail()
	for _, mi := range avail {
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
			nlog.Errorln(err)
			return
		}
		fspathsConfigAddDel(mi.Path, false /*add*/)
		nlog.Infof("%s: %s %s %s was previously aborted and now done", g.t, action, mi, xres)
	}
}

// store updated fspaths locally as part of the 'OverrideConfigFname'
// and commit new version of the config
func fspathsConfigAddDel(mpath string, add bool) {
	if cmn.Rom.TestingEnv() { // since testing fspaths are counted, not enumerated
		return
	}
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
		nlog.Errorln(err)
		return
	}
	// do
	fspathsSave(config)
}

func fspathsSave(config *cmn.Config) {
	toUpdate := &cmn.ConfigToSet{FSP: &config.LocalConfig.FSP}
	overrideConfig := cmn.GCO.SetLocalFSPaths(toUpdate)
	if err := cmn.SaveOverrideConfig(config.ConfigDir, overrideConfig); err != nil {
		debug.AssertNoErr(err)
		cmn.GCO.DiscardUpdate()
		nlog.Errorln(err)
		return
	}
	cmn.GCO.CommitUpdate(config)
}

// NOTE: executes under mfs lock; all errors here are FATAL
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

func (g *fsprungroup) checkEnable(action string, mi *fs.Mountpath) {
	avail := fs.GetAvail()
	if len(avail) > 1 {
		nlog.Infoln(action, mi.String())
	} else {
		nlog.Infoln(action, "the first mountpath", mi.String())
		if err := g.t.enable(); err != nil {
			nlog.Errorf("Failed to re-join %s (self): %v", g.t, err) // (FATAL, unlikely)
		}
	}
}
