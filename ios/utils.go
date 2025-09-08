// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ios

func GetFSUsedPercentage(path string) (int64, error) {
	totalBlocks, blocksAvailable, _, err := GetFSStats(path)
	if err != nil {
		return 0, err
	}
	usedBlocks := totalBlocks - blocksAvailable
	return int64(usedBlocks * 100 / totalBlocks), nil
}
