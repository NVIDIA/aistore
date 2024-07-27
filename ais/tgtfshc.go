// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

func (t *target) SoftFSHC() {
	avail := fs.GetAvail()
	for _, mi := range avail {
		t.fshc.OnErr(mi, "")
	}
}

func (t *target) FSHC(err error, mi *fs.Mountpath, fqn string) {
	debug.Assert(mi != nil)

	config := cmn.GCO.Get()
	if cmn.IsErrCapExceeded(err) {
		cs := t.oos(config)
		nlog.Errorf("%s: OOS (%s) via FSHC, %s", t, cs.String(), mi)
		return
	}

	if !config.FSHC.Enabled {
		return
	}

	// NOTE: filter-out non-IO errors
	if !t.fshc.IsErr(err) {
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Warningln(err, "is not one of the error types to trigger FSHC, ignoring...")
		}
		return
	}

	if !mi.IsAvail() {
		nlog.Warningln(mi.String(), "is not available (possibly disabled or detached), skipping FSHC")
		return
	}

	nlog.Errorf("%s: waking up FSHC to check %s, err: %v", t, mi, err)

	//
	// counting I/O errors on a per mountpath
	// TODO -- FIXME: remove `NameSuffix`
	//
	t.statsT.AddMany(cos.NamedVal64{Name: stats.ErrFSHCCount, NameSuffix: mi.Path, Value: 1})
	t.fshc.OnErr(mi, fqn)
}

// implements health.disabler interface
func (t *target) DisableMpath(mi *fs.Mountpath) (err error) {
	_, err = t.fsprg.disableMpath(mi.Path, true /*dont-resilver*/)

	t.statsT.SetFlag(stats.NodeStateFlags, cos.DiskFault)
	return err
}
