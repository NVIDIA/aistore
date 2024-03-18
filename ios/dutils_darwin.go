// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"github.com/lufia/iostat"
)

// TODO: NIY

type LsBlk struct{}

func lsblk(string, bool) *LsBlk { return nil }

func fs2disks(*LsBlk, string, string, int, bool) (disks FsDisks) {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil || len(driveStats) == 0 {
		return
	}
	drive := driveStats[0]
	return FsDisks{
		drive.Name: drive.BlockSize,
	}
}
