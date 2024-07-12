// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

func (t *target) FSHC(err error, mi *fs.Mountpath, fqn string) {
	config := cmn.GCO.Get()

	if cmn.IsErrCapExceeded(err) {
		cs := t.oos(config)
		nlog.Errorf("%s: OOS (%s) via FSHC", t, cs.String())
		return
	}

	if !config.FSHC.Enabled {
		return
	}
	if !t.fshc.IsErr(err) {
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Warningln(err, "is not one of the error types to trigger FSHC, ignoring...")
		}
		return
	}

	s := fmt.Sprintf("waking up FSHC to check %s for [%v]", mi, err) // or maybe not (waking up)

	if mi == nil {
		mi, _, err = fs.FQN2Mpath(fqn)
		if err != nil {
			if e, ok := err.(*cmn.ErrMountpathNotFound); ok {
				if e.Disabled() {
					nlog.Errorf("%s: %s is disabled, not %s", t, e.Mpath(), s)
					return
				}
			}
			nlog.Errorf("%s: %v, %s", t, err, s)
			return
		}
		debug.Assert(mi != nil)
	} else if mi.IsDisabled() {
		nlog.Errorf("%s: %s is disabled, not %s", t, mi, s)
		return
	}

	if err := cos.Stat(mi.Path); err != nil {
		nlog.Errorf("[FATAL %s]: available %s is not: %v", t, mi, err)
	}

	// yes "waking up"
	nlog.Errorln(t.String()+":", s)

	//
	// metrics: counting I/O errors on a per mountpath (`NameSuffix` below) basis
	//
	t.statsT.AddMany(cos.NamedVal64{Name: stats.ErrIOCount, NameSuffix: mi.Path, Value: 1})
	t.fshc.OnErr(mi, fqn)
}

// implements health.disabler interface
func (t *target) DisableMpath(mi *fs.Mountpath) (err error) {
	_, err = t.fsprg.disableMpath(mi.Path, true /*dont-resilver*/)

	t.statsT.Flag(stats.NodeStateFlags, cos.DiskFault, 0)
	return err
}
