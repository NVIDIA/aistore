// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/lufia/iostat"
)

// TODO: NIY

type (
	DiskInfo struct {
		Size  uint32
		Flags uint32
	}
	blockDev  struct{}
	BlockDevs []*blockDev
)

func _lsblk(string, *blockDev) (BlockDevs, error) {
	return nil, nil
}

func fs2disks(string, string, cos.MountpathLabel, BlockDevs, int, bool) (FsDisks, error) {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil || len(driveStats) == 0 {
		return nil, err
	}
	drive := driveStats[0]
	info := DiskInfo{
		Size: uint32(drive.BlockSize),
	}
	return FsDisks{drive.Name: info}, nil
}
