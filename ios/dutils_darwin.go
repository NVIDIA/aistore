// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"github.com/lufia/iostat"
)

// fs2disks is used when a mountpath is added to
// retrieve the disk(s) associated with a filesystem.
// This returns multiple disks only if the filesystem is RAID.
// TODO: implementation
func fs2disks(_ string) (disks FsDisks) {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil || len(driveStats) == 0 {
		return
	}
	drive := driveStats[0]
	return FsDisks{
		drive.Name: drive.BlockSize,
	}
}
