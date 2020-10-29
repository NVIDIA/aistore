// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
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
		MpathID   string `json:"mpath_id"`
		MountPath string `json:"mpath"`
		FsType    string `json:"fs_type"`
	}

	// Short for VolumeMetaData.
	VMD struct {
		Devices  map[string]*fsDeviceMD `json:"devices"` // MpathID => MD
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

func CreateVMD(daemonID string) *VMD {
	var (
		available, _ = Get()
		vmd          = newVMD(len(available))
	)

	vmd.DaemonID = daemonID

	for _, mPath := range available {
		vmd.Devices[mPath.ID] = &fsDeviceMD{
			MpathID:   mPath.ID,
			MountPath: mPath.Path,
			FsType:    mPath.FileSystem,
		}
	}
	return vmd
}

func ReadVMD() (err error, mainVMD *VMD) {
	available, _ := Get()
	// NOTE: iterating only over mpaths which have VmdPersistedFileName.
	for _, mpi := range available {
		fpath := filepath.Join(mpi.Path, VmdPersistedFileName)
		if err := Access(fpath); err != nil {
			continue
		}

		vmd := newVMD(len(available))
		vmd.cksum, err = jsp.Load(fpath, vmd, jsp.CCSign())
		if err != nil {
			err = fmt.Errorf("failed to read vmd (%q), err: %v", fpath, err)
			glog.InfoDepth(1)
			return
		}

		if err = vmd.Validate(); err != nil {
			err = fmt.Errorf("failed to validate vmd (%q), err: %v", fpath, err)
			glog.InfoDepth(1)
			return
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

	return
}

func (vmd VMD) Persist() error {
	// Checksum, compress and sign, as a VMD might be quite large.
	if cnt, availMpaths := PersistOnMpaths(VmdPersistedFileName, "", vmd, vmdCopies, jsp.CCSign()); cnt == 0 {
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
