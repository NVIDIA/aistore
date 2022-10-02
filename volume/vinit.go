// Package volume provides the volume abstraction and methods to configLoadVMD, store with redundancy,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

// initializes mountpaths and volume; on SIE (storage integrity error) terminates and exits
func Init(t cluster.Target, config *cmn.Config,
	allowSharedDisksAndNoDisks, useLoopbackDevs, ignoreMissingMountpath bool) (created bool) {
	var (
		vmd *VMD
		tid = t.SID()
	)
	fs.New(allowSharedDisksAndNoDisks || (config.TestingEnv() && !useLoopbackDevs)) // new and empty

	// bootstrap from a local-config referenced location; two points:
	// a) local-config is kept in-sync with mountpath changes (see ais/fspathgrp)
	// b) disk label for absolute referencing - can wait (TODO)
	if v, err := configLoadVMD(tid, config.FSP.Paths); err != nil {
		cos.ExitLogf("%s: %v", t, err)
	} else {
		vmd = v
	}

	if vmd == nil {
		// a) the very first deployment when volume does not exist, or
		// b) when the config doesn't contain a single valid mountpath
		//    (that in turn contains a copy of VMD, possibly outdated (but that's ok))
		if err := configInitMPI(tid, config); err != nil {
			cos.ExitLogf("%s: %v", t, err)
		}
		glog.Warningf("%s: creating new VMD from %v config", t, config.FSP.Paths.ToSlice())
		if v, err := NewFromMPI(tid); err != nil {
			cos.ExitLogf("%s: %v", t, err)
		} else {
			vmd = v
		}
		glog.Warningf("%s: %s created", t, vmd)
		created = true
		return
	}

	// use loaded VMD to find the most recently updated (the current) one and, simultaneously,
	// initialize MPI
	var persist bool
	if v, haveOld, err := vmdInitMPI(tid, config, vmd, 1 /*pass #1*/, ignoreMissingMountpath); err != nil {
		cos.ExitLogf("%s: %v", t, err)
	} else {
		if v != nil && v.Version > vmd.Version {
			vmd = v
			persist = true
		}
		if haveOld {
			persist = true
		}
		if v, _, err := vmdInitMPI(tid, config, vmd, 2 /*pass #2*/, ignoreMissingMountpath); err != nil {
			cos.ExitLogf("%s: %v", t, err)
		} else {
			debug.Assert(v == nil || v.Version == vmd.Version)
		}
		if persist {
			vmd.persist()
		}
	}
	glog.Infoln(vmd.String())
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
		glog.Warning(err) // TODO: handle
		err = nil
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

// config => MPI
func configInitMPI(tid string, config *cmn.Config) (err error) {
	var (
		configPaths    = config.FSP.Paths
		availablePaths = make(fs.MPI, len(configPaths))
		disabledPaths  = make(fs.MPI)
	)
	for path := range configPaths {
		var mi *fs.MountpathInfo
		if mi, err = fs.NewMountpath(path); err != nil {
			goto rerr
		}
		if err = mi.AddEnabled(tid, availablePaths, config); err != nil {
			goto rerr
		}
		if err = mi.CheckDisks(); err != nil {
			return
		}
	}
	if len(availablePaths) == 0 {
		err = cmn.ErrNoMountpaths
		goto rerr
	}
	fs.PutMPI(availablePaths, disabledPaths)
	return

rerr:
	err = cmn.NewErrInvalidFSPathsConf(err)
	return
}

// VMD => MPI in two passes
func vmdInitMPI(tid string, config *cmn.Config, vmd *VMD, pass int, ignoreMissingMountpath bool) (maxVerVMD *VMD,
	haveOld bool, err error) {
	var (
		availablePaths = make(fs.MPI, len(vmd.Mountpaths))
		disabledPaths  = make(fs.MPI)
	)
	debug.Assert(vmd.DaemonID == tid)

	for mpath, fsMpathMD := range vmd.Mountpaths {
		var mi *fs.MountpathInfo
		mi, err = fs.NewMountpath(mpath)
		if !fsMpathMD.Enabled {
			if pass == 2 {
				mi.Fs = fsMpathMD.Fs
				mi.FsType = fsMpathMD.FsType
				mi.FsID = fsMpathMD.FsID
				mi.AddDisabled(disabledPaths)
			}
			continue
		}
		// enabled
		if err != nil {
			err = &fs.ErrStorageIntegrity{Code: fs.SieMpathNotFound, Msg: err.Error()}
			if pass == 1 || ignoreMissingMountpath {
				glog.Errorf("%v (pass=%d, ignore-missing=%t)", err, pass, ignoreMissingMountpath)
				err = nil
				continue
			}
			return
		}
		if mi.Path != mpath {
			glog.Warningf("%s: cleanpath(%q) => %q", mi, mpath, mi.Path)
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
					Msg:  fmt.Sprintf("lost or missing mountpath %q (%+v vs %+v)", mpath, mi.FilesystemInfo, *fsMpathMD),
				}
				if pass == 1 || ignoreMissingMountpath {
					glog.Errorf("%v (pass=%d, ignore-missing=%t)", err, pass, ignoreMissingMountpath)
					err = nil
					continue
				}
				return
			}
			if mi.Fs == fsMpathMD.Fs && mi.FsID != fsMpathMD.FsID {
				glog.Warningf("detected FS ID change: mp=%q, curr=%+v, prev=%+v (pass %d)",
					mpath, mi.FilesystemInfo, *fsMpathMD, pass)
			} else if mi.Fs != fsMpathMD.Fs && mi.FsID == fsMpathMD.FsID {
				glog.Warningf("detected device name change for the same FS ID: mp=%q, curr=%+v, prev=%+v (pass %d)",
					mpath, mi.FilesystemInfo, *fsMpathMD, pass)
			}
		}

		if pass == 1 {
			if v, old, errLoad := loadOneVMD(tid, vmd, mi.Path, len(vmd.Mountpaths)); v != nil {
				debug.Assert(v.Version > vmd.Version)
				maxVerVMD = v
			} else if old {
				debug.AssertNoErr(errLoad)
				haveOld = true
			} else if errLoad != nil {
				glog.Warningf("%s: %v", mi, errLoad)
			}
		} else {
			if err = mi.AddEnabled(tid, availablePaths, config); err != nil {
				return
			}
			if err = mi.CheckDisks(); err != nil {
				glog.Errorf("Warning: %v", err)
			}
		}
	}

	if pass == 1 {
		return
	}
	if len(availablePaths) == 0 {
		if len(disabledPaths) == 0 {
			err = cmn.ErrNoMountpaths
			return
		}
		glog.Errorf("Warning: %v (avail=%d, disabled=%d)", err, len(availablePaths), len(disabledPaths))
	}
	fs.PutMPI(availablePaths, disabledPaths)
	// TODO: insufficient
	if la, lc := len(availablePaths), len(config.FSP.Paths); la != lc {
		glog.Warningf("number of available mountpaths (%d) differs from the configured (%d)", la, lc)
		glog.Warningln("run 'ais storage mountpath [attach|detach]', fix the config, or ignore")
	}
	return
}

// pre-loading to try to recover lost tid
func RecoverTID(generatedID string, configPaths cos.StrSet) (tid string) {
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
			glog.Warningf("recovered lost target ID %q from mpath %s", vmd.DaemonID, mpath)
			tid = vmd.DaemonID
		} else if tid != vmd.DaemonID {
			cos.ExitLogf("multiple conflicting target IDs %q(%q) vs %q", vmd.DaemonID, mpath, tid)
		}
	}
	if tid == "" {
		tid = generatedID
	}
	return
}

// loading

func LoadVMDTest() (*VMD, error) { return loadVMD("", nil) } // test-only

// config => (temp MPI) => VMD
func configLoadVMD(tid string, configPaths cos.StrSet) (vmd *VMD, err error) {
	if len(configPaths) == 0 {
		err = fmt.Errorf("no fspaths - see README => Configuration and fspaths section in the config.sh")
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
			glog.Warningf("mpath %s stores newer VMD: %s > %s", mpath, v, vmd)
		}
		return v, false, nil
	}
	if v.Version < vmd.Version {
		if !_mpathGreaterEq(vmd, v, mpath) {
			md := vmd.Mountpaths[mpath]
			// warn of an older version only if this mpath is enabled in the newer one
			if md != nil && md.Enabled {
				glog.Warningf("mpath %s stores older VMD: %s < %s", mpath, v, vmd)
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
