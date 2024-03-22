// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"errors"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
)

type IniCtx struct {
	UseLoopbacks  bool // using loopback dev-s
	IgnoreMissing bool // ignore missing mountpath(s)
	RandomTID     bool // generated random target ID
}

// bootstrap from local-config referenced locations
// NOTE: local plain-text config is kept in-sync with mountpath changes (see ais/fspathgrp)
// potential TODO for bare-metal deployments: store persistent labels (dmidecode style)

// - load (or initialize new) volume
// - initialize mountpaths
// - check a variety of SIE (storage integrity error) conditions; terminate and exit if detected
func Init(t core.Target, config *cmn.Config, ctx IniCtx) (created bool) {
	var (
		vmd     *VMD
		tid     = t.SID()
		fspaths = config.FSP.Paths.Keys()
	)
	// new and empty
	fs.New(len(config.FSP.Paths))

	if v, err := configLoadVMD(tid, config.FSP.Paths); err != nil {
		cos.ExitLogf("%s: %v (config-load-vmd, %v)", t, err, fspaths)
	} else {
		vmd = v
	}

	// a) the very first deployment when volume does not exist, or
	// b) when the config doesn't contain a single valid mountpath
	// (that in turn contains a copy of VMD, possibly outdated (but that's ok))
	if vmd == nil {
		if err := configInitMPI(tid, config); err != nil {
			cos.ExitLogf("%s: %v (config-init-mpi, %v, %+v)", t, err, fspaths, ctx)
		}
		nlog.Warningln(t.String()+":", "creating new VMD from", fspaths, "config")
		if v, err := NewFromMPI(tid); err != nil {
			cos.ExitLogf("%s: %v (new-from-mpi)", t, err) // unlikely
		} else {
			vmd = v
		}
		nlog.Warningln(t.String()+":", vmd.String(), "initialized")
		created = true
		return
	}

	// otherwise, use loaded VMD to find the most recently updated (the current) one and, simultaneously,
	// initialize MPI
	var persist bool
	if v, haveOld, err := initMPI(tid, config, vmd, 1 /*pass #1*/, ctx.IgnoreMissing); err != nil {
		cos.ExitLogf("%s: %v (vmd-init-mpi-p1, %+v, %s)", t, err, ctx, vmd)
	} else {
		if v != nil && v.Version > vmd.Version {
			vmd = v
			persist = true
		}
		if haveOld {
			persist = true
		}
		if v, _, err := initMPI(tid, config, vmd, 2 /*pass #2*/, ctx.IgnoreMissing); err != nil {
			cos.ExitLogf("%s: %v (vmd-init-mpi-p2, have-old=%t, %+v, %s)", t, err, haveOld, ctx, vmd)
		} else {
			debug.Assert(v == nil || v.Version == vmd.Version)
		}
		if persist {
			vmd.persist()
		}
	}

	nlog.Infoln(vmd.String())
	return
}

// MPI => VMD
func NewFromMPI(tid string) (vmd *VMD, err error) {
	var (
		curVersion          uint64
		available, disabled = fs.Get()
	)
	vmd, err = loadVMD(tid, nil)
	if err != nil {
		nlog.Warningln(err) // TODO: handle
	}
	if vmd != nil {
		curVersion = vmd.Version
	}
	vmd = newVMD(len(available))
	vmd.DaemonID = tid
	vmd.Version = curVersion + 1 // Bump the version.
	for _, mi := range available {
		vmd.addMountpath(mi, true /*enabled*/)
	}
	for _, mi := range disabled {
		vmd.addMountpath(mi, false /*enabled*/)
	}
	err = vmd.persist()
	return
}

func newVMD(expectedSize int) *VMD {
	return &VMD{Mountpaths: make(map[string]*fsMpathMD, expectedSize)}
}

// local config => fs.MPI
func configInitMPI(tid string, config *cmn.Config) (err error) {
	var (
		fspaths  = config.FSP.Paths
		avail    = make(fs.MPI, len(fspaths))
		disabled = make(fs.MPI)
	)
	for path, label := range fspaths {
		var mi *fs.Mountpath
		if mi, err = fs.NewMountpath(path, ios.Label(label)); err != nil {
			goto rerr
		}
		if err = mi.AddEnabled(tid, avail, config); err != nil {
			goto rerr
		}
	}
	if len(avail) == 0 {
		err = cmn.ErrNoMountpaths
		goto rerr
	}
	fs.PutMPI(avail, disabled)
	return

rerr:
	err = cmn.NewErrInvalidFSPathsConf(err)
	return
}

// VMD => fs.MPI in two passes
func initMPI(tid string, config *cmn.Config, vmd *VMD, pass int, ignoreMissingMi bool) (maxVer *VMD, haveOld bool, err error) {
	var (
		avail    = make(fs.MPI, len(vmd.Mountpaths))
		disabled = make(fs.MPI)
	)
	debug.Assert(vmd.DaemonID == tid)

	for mpath, fsMpathMD := range vmd.Mountpaths {
		var mi *fs.Mountpath
		mi, err = fs.NewMountpath(mpath, fsMpathMD.Label)
		if !fsMpathMD.Enabled {
			if pass == 2 {
				mi.Fs = fsMpathMD.Fs
				mi.FsType = fsMpathMD.FsType
				mi.FsID = fsMpathMD.FsID
				mi.AddDisabled(disabled)
			}
			continue
		}
		// enabled
		if err != nil {
			err = &fs.ErrStorageIntegrity{Code: fs.SieMpathNotFound, Msg: err.Error()}
			if pass == 1 || ignoreMissingMi {
				nlog.Errorf("%v (pass=%d, ignore-missing=%t)", err, pass, ignoreMissingMi)
				err = nil
				continue
			}
			return
		}
		if mi.Path != mpath {
			nlog.Warningf("%s: cleanpath(%q) => %q", mi, mpath, mi.Path)
		}

		// The (mountpath => filesystem) relationship is persistent and must _not_ change upon reboot.
		// There are associated false positives, though, namely:
		// 1. FS ID change. Reason: certain filesystems simply do not maintain persistence (of their IDs).
		// 2. `Fs` (usually, device name) change. Reason: OS block-level subsystem enumerated devices
		//    in a different order.
		// NOTE: no workaround if (1) and (2) happen simultaneously (must be extremely unlikely).
		//
		// See also: `allowSharedDisksAndNoDisks` and `startWithLostMountpath`
		if mi.FsType != fsMpathMD.FsType || mi.Fs != fsMpathMD.Fs || mi.FsID != fsMpathMD.FsID {
			if mi.FsType != fsMpathMD.FsType || (mi.Fs != fsMpathMD.Fs && mi.FsID != fsMpathMD.FsID) {
				err = &fs.ErrStorageIntegrity{
					Code: fs.SieFsDiffers,
					Msg:  fmt.Sprintf("lost or missing mountpath %q (%+v vs %+v)", mpath, mi.FS, *fsMpathMD),
				}
				if pass == 1 || ignoreMissingMi {
					nlog.Errorf("%v (pass=%d, ignore-missing=%t)", err, pass, ignoreMissingMi)
					err = nil
					continue
				}
				return
			}
			if mi.Fs == fsMpathMD.Fs && mi.FsID != fsMpathMD.FsID {
				nlog.Warningf("detected FS ID change: mp=%q, curr=%+v, prev=%+v (pass %d)",
					mpath, mi.FS, *fsMpathMD, pass)
			} else if mi.Fs != fsMpathMD.Fs && mi.FsID == fsMpathMD.FsID {
				nlog.Warningf("detected device name change for the same FS ID: mp=%q, curr=%+v, prev=%+v (pass %d)",
					mpath, mi.FS, *fsMpathMD, pass)
			}
		}

		if pass == 1 {
			if v, old, errLoad := loadOneVMD(tid, vmd, mi.Path, len(vmd.Mountpaths)); v != nil {
				debug.Assert(v.Version > vmd.Version)
				maxVer = v
			} else if old {
				debug.AssertNoErr(errLoad)
				haveOld = true
			} else if errLoad != nil {
				nlog.Warningf("%s: %v", mi, errLoad)
			}
		} else {
			if err = mi.AddEnabled(tid, avail, config); err != nil {
				return
			}
		}
	}

	if pass == 1 {
		return
	}
	if len(avail) == 0 {
		if len(disabled) == 0 {
			err = cmn.ErrNoMountpaths
			return
		}
		nlog.Errorf("Warning: %v (avail=%d, disabled=%d)", err, len(avail), len(disabled))
	}
	fs.PutMPI(avail, disabled)
	// TODO: insufficient
	if la, lc := len(avail), len(config.FSP.Paths); la != lc {
		nlog.Warningf("number of available mountpaths (%d) differs from the configured (%d)", la, lc)
		nlog.Warningln("run 'ais storage mountpath [attach|detach]', fix the config, or ignore")
	}
	return
}

// pre-loading to try to recover lost tid
func RecoverTID(generatedID string, configPaths cos.StrKVs) (tid string, recovered bool) {
	available := make(fs.MPI, len(configPaths)) // temp MPI to attempt loading
	for mpath := range configPaths {
		available[mpath] = nil
	}
	vmd := newVMD(len(available))
	for mpath := range available {
		if err := vmd.load(mpath); err != nil {
			continue
		}
		debug.Assert(vmd.DaemonID != "" && vmd.DaemonID != generatedID)
		if tid == "" {
			nlog.Warningf("recovered lost target ID %q from mpath %s", vmd.DaemonID, mpath)
			tid = vmd.DaemonID
			recovered = true
		} else if tid != vmd.DaemonID {
			cos.ExitLogf("multiple conflicting target IDs %q(%q) vs %q", vmd.DaemonID, mpath, tid) // FATAL
		}
	}
	if tid == "" {
		tid = generatedID
	}
	return tid, recovered
}

// loading

func LoadVMDTest() (*VMD, error) { return loadVMD("", nil) } // test-only

// config => (temp MPI) => VMD
func configLoadVMD(tid string, configPaths cos.StrKVs) (vmd *VMD, err error) {
	if len(configPaths) == 0 {
		err = errors.New("no fspaths - see README => Configuration and fspaths section in the config.sh")
		return
	}
	available := make(fs.MPI, len(configPaths)) // temp MPI to attempt loading
	for mpath := range configPaths {
		available[mpath] = nil
	}
	return loadVMD(tid, available)
}

// given a set of *available mountpaths* loadVMD discovers, loads, and validates
// the most recently updated VMD (which is stored in several copies for redundancy).
// - Returns nil if VMD does not exist;
// - Returns error on failure to validate or load existing VMD.
func loadVMD(tid string, available fs.MPI) (vmd *VMD, err error) {
	if available == nil {
		available = fs.GetAvail()
	}
	l := len(available)
	for mpath := range available {
		var v *VMD
		v, _, err = loadOneVMD(tid, vmd, mpath, l)
		if err != nil {
			return
		}
		if v != nil {
			vmd = v
		}
	}
	return
}

// given mountpath return a greater-version VMD if available
func loadOneVMD(tid string, vmd *VMD, mpath string, l int) (*VMD, bool /*have old*/, error) {
	var (
		v   = newVMD(l)
		err = v.load(mpath)
	)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, &fs.ErrStorageIntegrity{
			Code: fs.SieMetaCorrupted,
			Msg:  fmt.Sprintf("failed to load VMD from %q: %v", mpath, err),
		}
	}
	if vmd == nil {
		if tid != "" && v.DaemonID != tid {
			return nil, false, &fs.ErrStorageIntegrity{
				Code: fs.SieTargetIDMismatch,
				Msg:  fmt.Sprintf("%s has a different target ID: %q != %q", v, v.DaemonID, tid),
			}
		}
		return v, false, nil
	}
	//
	// validate
	//
	debug.Assert(vmd.DaemonID == tid || tid == "")
	if v.DaemonID != vmd.DaemonID {
		return nil, false, &fs.ErrStorageIntegrity{
			Code: fs.SieTargetIDMismatch,
			Msg:  fmt.Sprintf("%s has a different target ID: %q != %q", v, v.DaemonID, vmd.DaemonID),
		}
	}
	if v.Version > vmd.Version {
		if !_mpathGreaterEq(v, vmd, mpath) {
			nlog.Warningf("mpath %s stores newer VMD: %s > %s", mpath, v, vmd)
		}
		return v, false, nil
	}
	if v.Version < vmd.Version {
		if !_mpathGreaterEq(vmd, v, mpath) {
			md := vmd.Mountpaths[mpath]
			// warn of an older version only if this mpath is enabled in the newer one
			if md != nil && md.Enabled {
				nlog.Warningf("mpath %s stores older VMD: %s < %s", mpath, v, vmd)
			}
		}
		return nil, true, nil // true: outdated copy that must be updated
	}
	if !v.equal(vmd) { // same version must be identical
		err = &fs.ErrStorageIntegrity{
			Code: fs.SieNotEqVMD,
			Msg:  fmt.Sprintf("same VMD versions must be identical: %s(mpath %q) vs %s", v, mpath, vmd),
		}
	}
	return nil, false, err
}
