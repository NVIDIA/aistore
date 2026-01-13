// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
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
func (g *fsprungroup) attachMpath(mpath string, label cos.MountpathLabel) (addedMi *fs.Mountpath, err error) {
	addedMi, err = fs.AddMpath(g.t.SID(), mpath, label, g.redistributeMD)
	if err != nil || addedMi == nil {
		return
	}

	g._postAdd(apc.ActMountpathAttach, addedMi)
	return
}

func (g *fsprungroup) _postAdd(action string, ami *fs.Mountpath) {
	fspathsConfigAddDel(ami.Path, true /*add*/)

	g.preempt(action, ami)

	go func() {
		config := cmn.GCO.Get()
		if !config.Resilver.Enabled {
			return
		}
		args := &res.Args{
			Custom: xreg.ResArgs{Config: config},
		}
		g.t.runResilver(args)
	}()

	g.checkEnable(action, ami)

	tstats := g.t.statsT.(*stats.Trunner)
	for _, disk := range ami.Disks {
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
	if !dontResilver {
		config := cmn.GCO.Get()
		if config.Resilver.Enabled {
			args := &res.Args{Custom: xreg.ResArgs{Config: config}}
			go g.t.runResilver(args)
		}
	}
	return warn
}

func (g *fsprungroup) doDD(action string, flags uint64, mpath string, dontResilver bool) (*fs.Mountpath, error) {
	t := g.t
	rmi, numAvail, alreadyDD, err := fs.BeginDD(action, flags, mpath)
	if err != nil || rmi == nil {
		return nil, err
	}
	if numAvail == 0 {
		nlog.Errorf("%s: lost (via %q) the last available mountpath %q", t.si, action, rmi)
		g.postDD(rmi, action, nil /*xaction*/, nil /*error*/) // go ahead to disable/detach

		// NOTE: disable this target (it's a brick but still can be revitalized)
		t.disable()
		return rmi, nil
	}

	core.LcacheClearMpath(rmi)

	config := cmn.GCO.Get()
	confDisabled := !config.Resilver.Enabled

	// TODO: lookup ResilverSkippedMarker and related comments

	if alreadyDD || dontResilver || confDisabled {
		nlog.Infoln(t.String(), "action", action, rmi.String(), "- not resilvering:")
		nlog.Infoln("[ already disabled or detached:", alreadyDD, "--no-resilver:", dontResilver, "disabled via config:", confDisabled, "]")
		g.postDD(rmi, action, nil /*xaction*/, nil /*error*/) // ditto (compare with the one below)
		return rmi, nil
	}

	// Always use multi-jogger mode, and in particular when:
	// - preempting an existing resilver (prev=true): ensures complete coverage across all mountpaths
	// - resuming interrupted resilver (marked.Interrupted=true): continues with same parallelism
	// - when we have chunked objects (with possibly misplaced chunks but not the main one..)
	// TODO: remove SingleRmiJogger from res.Args

	args := &res.Args{
		Rmi:             rmi,
		Action:          action,
		PostDD:          g.postDD, // callback when done
		SingleRmiJogger: false,
		Custom:          xreg.ResArgs{Config: config},
	}
	go t.runResilver(args)

	return rmi, nil
}

func (g *fsprungroup) preempt(action string, mi *fs.Mountpath) bool /*prev*/ {
	const (
		sleep = cos.PollSleepShort
	)
	res := g.t.res
	// try to abort an active resilver, if any
	if res.Abort(fmt.Errorf("%v: %q, %s", cmn.ErrXactRenewAbort, action, mi)) {
		return true
	}
	xres := res.GetXact()
	if xres == nil { // finished ok or never ran
		return false
	}
	if xres.IsDone() {
		time.Sleep(sleep) // just finished; Res atomic state
		return false
	}

	// (unlikely)
	time.Sleep(sleep)
	debug.Assert(xres.IsAborted() || xres.IsDone(), xres.String())
	return true
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
			nlog.Errorln(g.t.String(), "(self) failed to rejoin cluster:", err) // (FATAL, unlikely)
		}
	}
}
