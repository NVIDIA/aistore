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
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const (
	vmdInitialVersion = 1
	vmdCopies         = 3
)

type (
	fsDeviceMD struct {
		MountPath string `json:"mpath"`
		FsType    string `json:"fs_type"`
		Enabled   bool   `json:"enabled"`
	}

	// Short for VolumeMetaData.
	VMD struct {
		Devices  map[string]*fsDeviceMD `json:"devices"` // Mpath => MD
		DaemonID string                 `json:"daemon_id"`
		Version  uint                   `json:"version"` // formatting version for backward compatibility
		cksum    *cmn.Cksum             // checksum of VMD
	}
)

func newVMD(expectedSize int) *VMD {
	return &VMD{
		Devices: make(map[string]*fsDeviceMD, expectedSize),
		Version: vmdInitialVersion,
	}
}

func CreateNewVMD(daemonID string) (*VMD, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	var (
		available, disabled = Get()
		vmd                 = newVMD(len(available))
	)

	vmd.DaemonID = daemonID

	for _, mPath := range available {
		vmd.Devices[mPath.Path] = &fsDeviceMD{
			MountPath: mPath.Path,
			FsType:    mPath.FileSystem,
			Enabled:   true,
		}
	}

	for _, mPath := range disabled {
		vmd.Devices[mPath.Path] = &fsDeviceMD{
			MountPath: mPath.Path,
			FsType:    mPath.FileSystem,
			Enabled:   false,
		}
	}
	return vmd, vmd.persist()
}

func ReadVMD() (*VMD, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()
	var (
		available, _ = Get()
		mainVMD      *VMD
		err          error
	)
	for _, mpi := range available {
		fpath := filepath.Join(mpi.Path, VmdPersistedFileName)

		vmd := newVMD(len(available))
		vmd.cksum, err = jsp.Load(fpath, vmd, jsp.CCSign())
		if err != nil && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			err = fmt.Errorf("failed to read vmd (%q), err: %v", fpath, err)
			glog.InfoDepth(1)
			return nil, err
		}

		if err = vmd.Validate(); err != nil {
			err = fmt.Errorf("failed to validate vmd (%q), err: %v", fpath, err)
			glog.InfoDepth(1)
			return nil, err
		}

		if mainVMD != nil {
			if !mainVMD.cksum.Equal(vmd.cksum) {
				cmn.ExitLogf("VMD is different (%q): %v vs %v", fpath, mainVMD, vmd)
			}
			continue
		}
		mainVMD = vmd
	}

	if mainVMD == nil {
		glog.Infof("VMD not found on any of %d mountpaths", len(available))
	}

	return mainVMD, nil
}

func (vmd VMD) persist() error {
	// Checksum, compress and sign, as a VMD might be quite large.
	if cnt, availMpaths := PersistOnMpaths(VmdPersistedFileName, "", vmd, vmdCopies, jsp.CCSign()); availMpaths == 0 {
		glog.Errorf("failed to persist VMD no available mountpaths")
	} else if cnt == 0 {
		return fmt.Errorf("failed to persist vmd on any of mountpaths (%d)", availMpaths)
	}
	return nil
}

func (vmd VMD) Validate() error {
	// TODO: Add versions handling.
	if vmd.Version != vmdInitialVersion {
		return fmt.Errorf("invalid vmd version %q", vmd.Version)
	}
	cmn.Assert(vmd.cksum != nil)
	cmn.Assert(vmd.DaemonID != "")
	return nil
}

// LoadDaemonID loads the daemon ID present as xattr on given mount paths.
func LoadDaemonID(mpaths cmn.StringSet) (mDaeID string, err error) {
	for mp := range mpaths {
		b, err := GetXattr(mp, daemonIDXattr)
		if err != nil {
			if cmn.IsErrXattrNotFound(err) {
				continue
			}
			return "", err
		}
		daeID := string(b)
		if mDaeID != "" {
			if daeID != "" && mDaeID != daeID {
				err = fmt.Errorf("daemonID different (%q): %s vs %s", mp, mDaeID, daeID)
				return "", err
			}
			continue
		}
		mDaeID = daeID
	}
	return
}
