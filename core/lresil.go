// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

// must be called under w-lock
// returns mountpath to relocate or copy this lom, or nil if none required/available
// return fixHrw = true when lom is currently misplaced
// - checks hrw location first, and
// - checks copies (if any) against the current configuration and available mountpaths;
// - does not check `fstat` in either case
// (usage: resilvering)
func (lom *LOM) ToMpath(avail fs.MPI) (*fs.Mountpath, bool /*fix HRW*/) {
	debug.Assert(lom.IsLocked() == apc.LockWrite, lom.Cname(), "expecting w-locked")

	hrwMi, _, err := avail.Hrw(cos.UnsafeB(*lom.md.uname))
	if err != nil {
		nlog.Errorln(err)
		return nil, false
	}
	debug.Assert(!hrwMi.IsAnySet(fs.FlagWaitingDD))

	if lom.mi.Path != hrwMi.Path {
		return hrwMi, true
	}
	if !lom.IsChunked() {
		return nil, false
	}
	u, err := NewUfest("", lom, true /*must-exist*/)
	if err != nil {
		nlog.Warningln(err)
		return nil, false
	}
	if err := u.LoadCompleted(lom); err != nil {
		nlog.Warningln(err)
		return nil, false
	}
	ok, err := u.IsHRW(avail)
	if err != nil {
		nlog.Warningln(err)
		return nil, false
	}
	return hrwMi, !ok
}

// deterministically pick a copy to restore any missing ones
// must be called under w-lock
// (usage: resilvering)
func (lom *LOM) IsPrimaryCopy(avail fs.MPI, hmi *fs.Mountpath, sentinel string) (isPrimary, mainExists bool) {
	debug.Assert(lom.IsCopy(), lom.Cname(), "must be a copy")
	selected := sentinel
	for fqn, mi := range lom.md.copies {
		if _, ok := avail[mi.Path]; !ok { // not skipping fs.FlagWaitingDD mountpaths
			continue
		}
		if err := cos.Stat(fqn); err != nil {
			continue
		}
		mainExists = mainExists || mi.Path == hmi.Path
		selected = min(fqn, selected)
	}
	return lom.FQN == selected, mainExists
}

// remove stale copy metadata entries (copies on unavailable or
// disabled mountpaths) and return the expected number of copies per config
// and the actual number of valid copies remaining;
// must be called under w-lock
// (usage: resilvering)
func (lom *LOM) CleanupCopies(avail fs.MPI) (exp, got int) {
	mirror := lom.MirrorConf()
	if !mirror.Enabled || mirror.Copies < 2 {
		return
	}
	exp, got = int(mirror.Copies), 0
	for fqn, mpi := range lom.md.copies {
		mpathInfo, ok := avail[mpi.Path]
		if !ok || mpathInfo.IsAnySet(fs.FlagWaitingDD) {
			lom.delCopyMd(fqn)
		} else {
			got++
		}
	}
	return
}
