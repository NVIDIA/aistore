// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
)

type IniCtx struct {
	PodRef        string // namespace/pod for a pod-derived target ID
	UseLoopbacks  bool   // using loopback dev-s
	IgnoreMissing bool   // ignore missing mountpath(s)
	RandomTID     bool   // generated random target ID
}

// bootstrap from local-config referenced locations
// NOTE: local plain-text config is kept in-sync with mountpath changes (see ais/fspathgrp)
// potential TODO for bare-metal deployments: store persistent labels (dmidecode style)

// - load (or initialize new) volume
// - initialize mountpaths
// - check a variety of SIE (storage integrity error) conditions; terminate and exit if detected
func Init(t core.Target, config *cmn.Config, ctx IniCtx) bool /*created*/ {
	var (
		vmd     *VMD
		tid     = t.SID()
		fspaths = config.FSP.Paths.Keys()
	)
	if len(config.FSP.Paths) == 0 {
		cos.ExitLogf("%s: no fspaths - see README => Configuration and fspaths section in the config.sh", t)
	}
	// new and empty
	blockDevs := fs.New(t, len(config.FSP.Paths))

	// load VMD copies from the configured mountpaths
	var (
		vc      = newVcopies(len(config.FSP.Paths))
		checked = make(cos.StrSet, len(config.FSP.Paths))
	)
	for mpath := range config.FSP.Paths {
		checked.Set(filepath.Clean(mpath))
	}
	vc.load(checked.ToSlice())
	if v, err := vc.newest(tid); err != nil {
		cos.ExitLogf("%s: %v (config-load-vmd, %v)", t, ctx.ownershipError(err, tid), fspaths)
	} else {
		vmd = v
	}

	// a) the very first deployment when volume does not exist, or
	// b) when the config doesn't contain a single valid mountpath
	// (that in turn contains a copy of VMD, possibly outdated (but that's ok))
	if vmd == nil {
		if err := configInitMPI(tid, config, blockDevs); err != nil {
			cos.ExitLogf("%s: %v (config-init-mpi, %v, %+v)", t, ctx.ownershipError(err, tid), fspaths, ctx)
		}
		nlog.Warningln(t.String()+":", "creating new VMD from", fspaths, "config")
		if v, err := NewFromMPI(tid); err != nil {
			cos.ExitLogf("%s: %v (new-from-mpi)", t, err) // unlikely
		} else {
			vmd = v
		}
		nlog.Warningln(t.String(), vmd.String(), "initialized")
		return true // created
	}

	// follow the newest copy: it may list mountpaths that store even newer copies
	for {
		mpaths := vmd.unchecked(checked)
		if len(mpaths) == 0 {
			break
		}
		vc.load(mpaths)
		checked.Add(mpaths...)
		v, err := vc.newest(tid)
		if err != nil {
			cos.ExitLogf("%s: %v (vmd-discovery, %+v, %s)", t, ctx.ownershipError(err, tid), ctx, vmd)
		}
		vmd = v
	}

	if err := initMPI(tid, config, blockDevs, vmd, checked, ctx.IgnoreMissing); err != nil {
		cos.ExitLogf("%s: %v (vmd-init-mpi, %+v, %s)", t, ctx.ownershipError(err, tid), ctx, vmd)
	}

	// rewrite outdated and bad copies (persist removes all copies from available mountpaths)
	if n := vc.stale(vmd, fs.GetAvail()); n > 0 {
		nlog.Warningln(t.String(), vmd.String(), "- replacing", n, "outdated or bad copies")
		if err := vmd.persist(); err != nil {
			cos.ExitLogf("%s: %v (vmd-persist, %+v, %s)", t, err, ctx, vmd)
		}
	}

	nlog.Infoln(vmd.String())
	return false // created
}

func (ctx IniCtx) ownershipError(err error, tid string) error {
	if ctx.PodRef != "" {
		if sie, ok := errors.AsType[*fs.ErrStorageIntegrity](err); ok &&
			(sie.Code == fs.SieMpathIDMismatch || sie.Code == fs.SieTargetIDMismatch) {
			return fmt.Errorf("pod %q expects target ID %q: %w", ctx.PodRef, tid, err)
		}
	}
	return err
}

// MPI => VMD
func NewFromMPI(tid string) (vmd *VMD, err error) {
	var (
		curVersion      uint64
		avail, disabled = fs.Get()
	)
	vc := newVcopies(len(avail))
	vc.loadMPI(avail)
	if _, err := vc.newest(tid); err != nil {
		nlog.Warningln(err) // TODO: handle
	}
	curVersion = vc.maxVersion()
	vmd = newVMD(len(avail))
	vmd.DaemonID = tid
	vmd.Version = curVersion + 1 // Bump the version.
	for _, mi := range avail {
		vmd.addMountpath(mi, true /*enabled*/)
	}
	for _, mi := range disabled {
		vmd.addMountpath(mi, false /*enabled*/)
	}
	err = vmd.persist()
	return vmd, err
}

func newVMD(expectedSize int) *VMD {
	return &VMD{Mountpaths: make(map[string]*fsMpathMD, expectedSize)}
}

// local config => fs.MPI
func configInitMPI(tid string, config *cmn.Config, blockDevs ios.BlockDevs) error {
	var (
		fspaths  = config.FSP.Paths
		avail    = make(fs.MPI, len(fspaths))
		disabled = make(fs.MPI)
	)
	for path, label := range fspaths {
		mi, err := fs.NewMountpath(path, cos.MountpathLabel(label))
		if err != nil {
			return cmn.NewErrInvalidFSPathsConf(err)
		}
		if err := mi.AddEnabled(tid, avail, disabled, config, blockDevs); err != nil {
			return cmn.NewErrInvalidFSPathsConf(err)
		}
	}
	if len(avail) == 0 {
		return cmn.NewErrInvalidFSPathsConf(cmn.ErrNoMountpaths)
	}
	fs.PutMPI(avail, disabled)
	return nil
}

// VMD => fs.MPI
func initMPI(tid string, config *cmn.Config, devs ios.BlockDevs, vmd *VMD, checked cos.StrSet, ignore bool) error {
	var (
		avail    = make(fs.MPI, len(vmd.Mountpaths))
		disabled = make(fs.MPI)
	)
	debug.Assert(vmd.DaemonID == tid)

	for mpath, fsMpathMD := range vmd.Mountpaths {
		mi, err := fs.NewMountpath(mpath, fsMpathMD.Label)
		if !fsMpathMD.Enabled {
			mi.Fs = fsMpathMD.Fs
			mi.FsType = fsMpathMD.FsType
			mi.FsID = fsMpathMD.FsID
			mi.AddDisabled(disabled)
			continue
		}
		// enabled
		if err != nil {
			err = &fs.ErrStorageIntegrity{Code: fs.SieMpathNotFound, Msg: err.Error()}
			if ignore {
				nlog.Errorf("%v (ignore-missing-mountpath)", err)
				continue
			}
			return err
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
			if lostFS(mi, fsMpathMD) {
				err := &fs.ErrStorageIntegrity{
					Code: fs.SieFsDiffers,
					Msg:  fmt.Sprintf("lost or missing mountpath %q (%+v vs %+v)", mpath, mi.FS, *fsMpathMD),
				}
				if ignore {
					nlog.Errorf("%v (ignore-missing-mountpath)", err)
					continue
				}
				return err
			}
			if mi.Fs == fsMpathMD.Fs && mi.FsID != fsMpathMD.FsID {
				nlog.Warningf("detected FS ID change: mp=%q, curr=%+v, prev=%+v", mpath, mi.FS, *fsMpathMD)
			} else if mi.Fs != fsMpathMD.Fs && mi.FsID == fsMpathMD.FsID {
				nlog.Warningf("detected device name change for the same FS ID: mp=%q, curr=%+v, prev=%+v",
					mpath, mi.FS, *fsMpathMD)
			}
		}

		if !checked.Contains(mi.Path) {
			return fmt.Errorf("mountpath %q became available during VMD discovery; retry startup", mi.Path)
		}
		if err := mi.AddEnabled(tid, avail, disabled, config, devs); err != nil {
			return err
		}
	}

	if len(avail) == 0 {
		if len(disabled) == 0 {
			return cmn.ErrNoMountpaths
		}
		nlog.Errorf("Warning: %v (avail=%d, disabled=%d)", cmn.ErrNoMountpaths, len(avail), len(disabled))
	}
	fs.PutMPI(avail, disabled)

	// TODO: insufficient
	if la, lc := len(avail), len(config.FSP.Paths); la != lc {
		nlog.Warningf("number of available mountpaths (%d) differs from the configured (%d)", la, lc)
		nlog.Warningln("run 'ais storage mountpath [attach|detach]', fix the config, or use ignore-missing-mountpath")
	}
	return nil
}

// enabled mountpaths that are present, have the recorded filesystem, and are not yet checked
func (vmd *VMD) unchecked(checked cos.StrSet) (mpaths []string) {
	for mpath, md := range vmd.Mountpaths {
		if !md.Enabled {
			continue
		}
		mi, err := fs.NewMountpath(mpath, md.Label)
		if err != nil || lostFS(mi, md) || checked.Contains(mi.Path) {
			continue
		}
		mpaths = append(mpaths, mi.Path)
	}
	return mpaths
}

// filesystem differs from the one recorded in VMD
func lostFS(mi *fs.Mountpath, md *fsMpathMD) bool {
	return mi.FsType != md.FsType || (mi.Fs != md.Fs && mi.FsID != md.FsID)
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

// test-only
func LoadVMDTest(tid string, mpaths ...string) (*VMD, error) {
	if len(mpaths) == 0 {
		return loadVMD(tid, nil)
	}
	available := make(fs.MPI, len(mpaths))
	for _, mpath := range mpaths {
		available[mpath] = nil
	}
	return loadVMD(tid, available)
}

// test-only
func SaveVMDTest(dir, tid string, version uint64, mpaths ...string) error {
	vmd := newVMD(len(mpaths))
	vmd.DaemonID, vmd.Version = tid, version
	for _, mpath := range mpaths {
		vmd.Mountpaths[mpath] = &fsMpathMD{Path: mpath, Enabled: true}
	}
	return jsp.SaveMeta(filepath.Join(dir, fname.Vmd), vmd, nil)
}

// load VMD copies from the given (or available) mountpaths and select the newest
// - returns nil if VMD does not exist
func loadVMD(tid string, available fs.MPI) (*VMD, error) {
	if available == nil {
		available = fs.GetAvail()
	}
	vc := newVcopies(len(available))
	vc.loadMPI(available)
	return vc.newest(tid)
}

//
// vcopies: VMD copies found on mountpaths
//

type vcopies struct {
	valid map[string]*VMD // mountpath => copy
	bad   cos.StrSet      // mountpaths with unreadable or corrupted copies
}

func newVcopies(l int) *vcopies {
	return &vcopies{valid: make(map[string]*VMD, l), bad: make(cos.StrSet, 2)}
}

// load copies; skip missing, record bad
func (vc *vcopies) load(mpaths []string) {
	for _, mpath := range mpaths {
		v := newVMD(0)
		err := v.load(mpath)
		switch {
		case err == nil:
			vc.valid[mpath] = v
		case cos.IsNotExist(err):
		default:
			nlog.Warningf("ignoring bad VMD copy on %q: %v", mpath, err)
			vc.bad.Set(mpath)
		}
	}
}

func (vc *vcopies) loadMPI(mpi fs.MPI) {
	mpaths := make([]string, 0, len(mpi))
	for mpath := range mpi {
		mpaths = append(mpaths, mpath)
	}
	vc.load(mpaths)
}

// select the newest copy; independent of the order of loading
// - fail on a copy of a different target (any version)
// - fail on different copies of the newest version
// - fail when all found copies are bad
func (vc *vcopies) newest(tid string) (*VMD, error) {
	if len(vc.valid) == 0 {
		if len(vc.bad) > 0 {
			bad := vc.bad.ToSlice()
			slices.Sort(bad)
			return nil, &fs.ErrStorageIntegrity{
				Code: fs.SieMetaCorrupted,
				Msg:  fmt.Sprintf("no valid VMD, bad copies on %v", bad),
			}
		}
		return nil, nil
	}
	var (
		mpaths = make([]string, 0, len(vc.valid))
		newest *VMD
		at     string
	)
	for mpath := range vc.valid {
		mpaths = append(mpaths, mpath)
	}
	slices.Sort(mpaths)
	for _, mpath := range mpaths {
		if v := vc.valid[mpath]; newest == nil || v.Version > newest.Version {
			newest, at = v, mpath
		}
	}
	if tid == "" {
		tid = newest.DaemonID
	}
	for _, mpath := range mpaths {
		v := vc.valid[mpath]
		if v.DaemonID != tid {
			return nil, &fs.ErrStorageIntegrity{
				Code: fs.SieTargetIDMismatch,
				Msg:  fmt.Sprintf("VMD on %q has target ID %q, expected %q", mpath, v.DaemonID, tid),
			}
		}
		if v.Version == newest.Version && !v.equal(newest) {
			return nil, &fs.ErrStorageIntegrity{
				Code: fs.SieNotEqVMD,
				Msg:  fmt.Sprintf("same VMD versions must be identical: %s(mpath %q) vs %s(mpath %q)", v, mpath, newest, at),
			}
		}
	}
	return newest, nil
}

func (vc *vcopies) maxVersion() (ver uint64) {
	for _, v := range vc.valid {
		ver = max(ver, v.Version)
	}
	return ver
}

// number of outdated and bad copies on the given mountpaths
func (vc *vcopies) stale(vmd *VMD, avail fs.MPI) (n int) {
	for mpath := range avail {
		if v, ok := vc.valid[mpath]; ok && v.Version < vmd.Version {
			n++
		} else if vc.bad.Contains(mpath) {
			n++
		}
	}
	return n
}
