// Package volume provides the volume abstraction and methods to configLoadVMD, store with redundancy,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
func Init(t cluster.Target, config *cmn.Config) {
	var (
		vmd *VMD
		tid = t.Snode().ID()
	)
	fs.New() // new and empty

	if config.TestingEnv() {
		glog.Warningf("Using %d mountpaths for testing with disabled filesystem ID check", config.TestFSP.Count)
		fs.DisableFsIDCheck()
	}
	// boostrap from a local-config referenced location; two points:
	// a) local-config is kept in sync with mountpath changes (see ais/fspathgrp)
	// b) disk-label based bootstrapping can be done but can wait (see comment below)
	if v, err := configLoadVMD(tid, config.FSpaths.Paths); err != nil {
		cos.ExitLogf("%s: %v", t.Snode(), err)
	} else {
		vmd = v
	}

	if vmd == nil {
		// a) VMD is nil upon the very first deployment, or
		// b) when the config doesn't contain a single valid mountpath
		//    (that in turn contains a copy of VMD, possibly outdated (but that's ok))
		if err := configInitMPI(tid, config); err != nil {
			cos.ExitLogf("%s: %v", t.Snode(), err)
		}
		glog.Warningf("%s: initializing mountpaths and creating new VMD from %v config", t.Snode(), config.FSpaths.Paths.ToSlice())
		if v, err := NewFromMPI(tid); err != nil {
			cos.ExitLogf("%s: %v", t.Snode(), err)
		} else {
			vmd = v
		}
		glog.Warningf("%s: %s created", t.Snode(), vmd)
		return
	}

	// use loaded VMD to find the most recently updated (the current) one and, simultaneously,
	// initialize MPI
	var persist bool
	if v, haveOld, err := vmdInitMPI(tid, config, vmd, 1 /*pass #1*/); err != nil {
		cos.ExitLogf("%s: %v", t.Snode(), err)
	} else {
		if v != nil && v.Version > vmd.Version {
			vmd = v
			persist = true
		}
		if haveOld {
			persist = true
		}
		if v, _, err := vmdInitMPI(tid, config, vmd, 2 /*pass #2*/); err != nil {
			cos.ExitLogf("%s: %v", t.Snode(), err)
		} else {
			debug.Assert(v == nil || v.Version == vmd.Version)
		}
		if persist {
			vmd.persist()
		}
	}
	glog.Infoln(vmd.String())
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
		configPaths    = config.FSpaths.Paths
		availablePaths = make(fs.MPI, len(configPaths))
		disabledPaths  = make(fs.MPI)
	)
	for path := range configPaths {
		var mi *fs.MountpathInfo
		if mi, err = fs.NewMountpath(path, tid); err != nil {
			return
		}
		if err = mi.AddEnabled(tid, availablePaths, config); err != nil {
			return
		}
		if len(mi.Disks) == 0 && !config.TestingEnv() {
			err = &fs.ErrMpathNoDisks{Mi: mi}
			return
		}
	}
	fs.PutMpaths(availablePaths, disabledPaths)
	return
}

// VMD => MPI in two passes
func vmdInitMPI(tid string, config *cmn.Config, vmd *VMD, pass int) (maxVerVMD *VMD, haveOld bool, err error) {
	var (
		availablePaths = make(fs.MPI, len(vmd.Mountpaths))
		disabledPaths  = make(fs.MPI)
	)
	debug.Assert(vmd.DaemonID == tid)

	for mpath, fsMpathMD := range vmd.Mountpaths {
		var mi *fs.MountpathInfo
		mi, err = fs.NewMountpath(mpath, tid)
		if fsMpathMD.Enabled {
			if err != nil {
				return
			}
			if mi.Path != mpath {
				glog.Warningf("%s: cleanpath(%q) => %q", mi, mpath, mi.Path)
			}
			if mi.Fs != fsMpathMD.Fs || mi.FsType != fsMpathMD.FsType || mi.FsID != fsMpathMD.FsID {
				err = &fs.ErrStorageIntegrity{
					Code: fs.SieFsDiffers,
					Msg: fmt.Sprintf("Warning: filesystem change detected: %+v vs %+v (is it benign?)",
						mi.FilesystemInfo, fsMpathMD),
				}
				glog.Error(err)
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
				debug.Assert(pass == 2)
				if err = mi.AddEnabled(tid, availablePaths, config); err != nil {
					return
				}
				if len(mi.Disks) == 0 && !config.TestingEnv() {
					err = &fs.ErrMpathNoDisks{Mi: mi}
					glog.Errorf("Warning: %v", err)
				}
			}
		} else if pass == 2 {
			mi.Fs = fsMpathMD.Fs
			mi.FsType = fsMpathMD.FsType
			mi.FsID = fsMpathMD.FsID
			mi.AddDisabled(disabledPaths)
		}
	}

	if pass == 1 {
		return
	}
	fs.PutMpaths(availablePaths, disabledPaths)
	// TODO: insufficient
	if la, lc := len(availablePaths), len(config.FSpaths.Paths); la != lc {
		glog.Warningf("number of available mountpaths (%d) differs from the configured (%d)", la, lc)
		glog.Warningln("run 'ais storage mountpath [attach|detach]', fix the config, or ignore")
	}
	return
}

// loading

func LoadVMDTest() (*VMD, error) { return loadVMD("", nil) } // test-only

// config => (temp MPI) => VMD
func configLoadVMD(tid string, configPaths cos.StringSet) (vmd *VMD, err error) {
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
		available, _ = fs.Get()
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
			glog.Warningf("mpath %s: VMD version mismatch: %s vs %s", mpath, v, vmd)
		}
		return v, false, nil
	}
	if v.Version < vmd.Version {
		if !_mpathGreaterEq(vmd, v, mpath) {
			glog.Warningf("mpath %s: VMD version mismatch: %s vs %s", mpath, vmd, v)
		}
		return nil, true, nil // true: outdated copy that must be updated
	}
	if !v.equal(vmd) { // same version must be identical
		err = &fs.ErrStorageIntegrity{
			Code: fs.SieNotEqVMD,
			Msg:  fmt.Sprintf("VMD differs: %s vs %s (%q)", vmd, v, mpath),
		}
	}
	return nil, false, err
}
