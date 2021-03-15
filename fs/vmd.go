// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const vmdCopies = 3

type (
	fsMpathMD struct {
		Mountpath string `json:"mountpath"`
		Enabled   bool   `json:"enabled"`

		Fs     string   `json:"fs"`
		FsType string   `json:"fs_type"`
		FsID   cos.FsID `json:"fs_id"`

		Ext interface{} `json:"ext,omitempty"` // Reserved for future extension.
	}

	// Short for VolumeMetaData.
	VMD struct {
		Version    uint64                `json:"version,string"` // Version which tracks any updates happened to Mountpaths.
		Mountpaths map[string]*fsMpathMD `json:"mountpaths"`     // mountpath => metadata
		DaemonID   string                `json:"daemon_id"`      // ID of the daemon to which the mountpaths belong(ed).
		cksum      *cos.Cksum            // Checksum of loaded VMD.
	}
)

// interface guard
var _ jsp.Opts = (*VMD)(nil)

func (*VMD) JspOpts() jsp.Options { return jsp.CCSign(cmn.MetaverVMD) }

func newVMD(expectedSize int) *VMD {
	return &VMD{
		Mountpaths: make(map[string]*fsMpathMD, expectedSize),
	}
}

func (vmd *VMD) load(mpath string) (err error) {
	fpath := filepath.Join(mpath, cmn.VmdFname)
	if vmd.cksum, err = jsp.LoadMeta(fpath, vmd); err != nil {
		return err
	}
	if vmd.DaemonID == "" {
		debug.Assert(false) // Cannot happen in normal environment.
		return fmt.Errorf("daemon id is empty for vmd on %q", mpath)
	}
	return nil
}

func (vmd *VMD) persist() (err error) {
	cnt, availCnt := PersistOnMpaths(cmn.VmdFname, "", vmd, vmdCopies, vmd.JspOpts())
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		glog.Errorf("cannot store VMD: %v", ErrNoMountpaths)
		return
	}
	return fmt.Errorf("failed to store VMD on any of the mountpaths (%d)", availCnt)
}

func (vmd *VMD) equal(other *VMD) bool {
	debug.Assert(vmd.cksum != nil)
	debug.Assert(other.cksum != nil)
	return vmd.DaemonID == other.DaemonID &&
		vmd.Version == other.Version &&
		vmd.cksum.Equal(other.cksum)
}

func (vmd *VMD) String() string { return string(cos.MustMarshal(vmd)) }

func CreateNewVMD(daemonID string) (vmd *VMD, err error) {
	available, disabled := Get()

	// Try to load the currently stored vmd to determine the version.
	var curVersion uint64
	vmd, _ = LoadVMD(available.ToStringSet())
	if vmd != nil {
		curVersion = vmd.Version
	}

	vmd = newVMD(len(available))
	vmd.DaemonID = daemonID
	vmd.Version = curVersion + 1 // Bump the version.

	addMountpath := func(mpath *MountpathInfo, enabled bool) {
		vmd.Mountpaths[mpath.Path] = &fsMpathMD{
			Mountpath: mpath.Path,
			Enabled:   enabled,

			Fs:     mpath.Fs,
			FsType: mpath.FsType,
			FsID:   mpath.FsID,
		}
	}

	for _, mpath := range available {
		addMountpath(mpath, true /*enabled*/)
	}
	for _, mpath := range disabled {
		addMountpath(mpath, false /*enabled*/)
	}
	err = vmd.persist()
	return
}

// LoadVMD loads VMD from given paths:
// - Returns error in case of validation errors or failed to load existing VMD
// - Returns nil if VMD not present on any path
func LoadVMD(mpaths cos.StringSet) (mainVMD *VMD, err error) {
	for path := range mpaths {
		vmd := newVMD(len(mpaths))
		err := vmd.load(path)
		if err != nil && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			err = newVMDLoadErr(path, err)
			return nil, err
		}
		if mainVMD == nil {
			mainVMD = vmd
		} else {
			if vmd.DaemonID != mainVMD.DaemonID {
				err = newVMDMismatchErr(mainVMD, vmd, path)
				return nil, err
			}

			if vmd.Version > mainVMD.Version {
				// If the `vmd` version is greater than current `mainVMD` we should
				// just take the newer one that we trust.
				mainVMD = vmd
				glog.Warningf("Found vmd (on %q) which has greater version (%s vs %s)", path, mainVMD, vmd)
				continue
			} else if vmd.Version < mainVMD.Version {
				// If the `vmd` version is lesser than current `mainVMD` we should
				// just ignore it.
				glog.Warningf("Found vmd (on %q) which has lesser version (%s vs %s)", path, vmd, mainVMD)
				continue
			}

			// If the versions match check if the VMDs are equal (they should be).
			if !vmd.equal(mainVMD) {
				err = newVMDMismatchErr(mainVMD, vmd, path)
				return nil, err
			}
		}
	}
	if mainVMD == nil {
		glog.Warningf("VMD not found on any of %d mountpaths", len(mpaths))
	}
	return mainVMD, nil
}

// LoadDaemonID loads the daemon ID present as xattr on given mount paths.
func LoadDaemonID(mpaths cos.StringSet) (mDaeID string, err error) {
	for mp := range mpaths {
		daeID, err := loadDaemonIDXattr(mp)
		if err != nil {
			return "", err
		}
		if daeID == "" {
			continue
		}
		if mDaeID != "" {
			if mDaeID != daeID {
				return "", newMpathIDMismatchErr(mDaeID, daeID, mp)
			}
			continue
		}
		mDaeID = daeID
	}
	return
}

func loadDaemonIDXattr(mpath string) (daeID string, err error) {
	b, err := GetXattr(mpath, daemonIDXattr)
	if err == nil {
		daeID = string(b)
		return
	}
	if cmn.IsErrXattrNotFound(err) {
		err = nil
	}
	return
}
