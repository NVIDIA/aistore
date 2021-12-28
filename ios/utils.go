// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import "os"

func FinfoAtime(fqn string) (finfo os.FileInfo, atime int64, err error) {
	if finfo, err = os.Stat(fqn); err != nil {
		return
	}
	atime = GetATime(finfo).UnixNano()
	return
}

func GetFSUsedPercentage(path string) (usedPercentage int64, ok bool) {
	totalBlocks, blocksAvailable, _, err := GetFSStats(path)
	if err != nil {
		return
	}
	usedBlocks := totalBlocks - blocksAvailable
	return int64(usedBlocks * 100 / totalBlocks), true
}
