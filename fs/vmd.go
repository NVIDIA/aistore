// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const (
	vmdInitialVersion = 1
	vmdCopies         = 3

	vmdPersistedFileNameOld = VmdPersistedFileName + ".old"
)

type (
	fsDeviceMD struct {
		MpathID   string `json:"mpath_id"`
		MountPath string `json:"mpath"`
		FsType    string `json:"fs_type"`
	}

	// Short for VolumeMetaData.
	VMD struct {
		Devices  []*fsDeviceMD `json:"devices"`
		DaemonID string        `json:"daemon_id"`
		Version  uint          `json:"version"` // formatting version for backward compatibility
	}
)

func newVMD(expectedSize int) *VMD {
	return &VMD{
		Devices: make([]*fsDeviceMD, 0, expectedSize),
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
		vmd.Devices = append(vmd.Devices, &fsDeviceMD{
			MpathID:   mPath.ID,
			MountPath: mPath.Path,
			FsType:    mPath.FileSystem,
		})
	}
	return vmd
}

func ReadVMD() *VMD {
	var (
		mpaths       = FindPersisted(VmdPersistedFileName)
		available, _ = Get()

		mainVMD *VMD
	)
	if len(mpaths) == 0 {
		glog.Infof("VMD not found on any of %d mountpaths", len(available))
		return nil
	}

	if len(mpaths) != len(available) {
		glog.Warningf("Some mountpaths are missing VMD (found %d/%d)", len(mpaths), len(available))
	}

	// NOTE: iterating only over mpaths which have VmdPersistedFileName.
	for _, mpi := range mpaths {
		fpath := filepath.Join(mpi.Path, VmdPersistedFileName)

		vmd := newVMD(len(available))
		if err := jsp.Load(fpath, vmd, jsp.CCSign()); err != nil {
			glog.Fatalf("Failed to read vmd (%q), err: %v", fpath, err)
		}
		if err := vmd.Validate(); err != nil {
			glog.Fatalf("Failed to validate vmd (%q), err: %v", fpath, err)
		}

		if mainVMD != nil {
			if !reflect.DeepEqual(mainVMD, vmd) {
				glog.Fatalf("VMD is different (%q): %v vs %v", fpath, mainVMD, vmd)
			}
			continue
		}

		mainVMD = vmd
	}

	// At least one mpath with VMD and no errors loading/comparing/validating.
	cmn.Assert(mainVMD != nil)
	return mainVMD
}

func (vmd VMD) Persist() error {
	MovePersisted(VmdPersistedFileName, vmdPersistedFileNameOld)

	// Checksum, compress and sign, as a VMD might be quite large.
	if cnt, availMpaths := PersistOnMpaths(VmdPersistedFileName, vmd, vmdCopies, jsp.CCSign()); cnt == 0 {
		return fmt.Errorf("failed to persist vmd on any of mountpaths (%d)", availMpaths)
	}

	RemovePersisted(vmdPersistedFileNameOld)
	return nil
}

func (vmd VMD) Validate() error {
	// TODO: Add versions handling.
	if vmd.Version != vmdInitialVersion {
		return fmt.Errorf("invalid vmd version %q", vmd.Version)
	}

	cmn.Assert(vmd.DaemonID != "")
	return nil
}
