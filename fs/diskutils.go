// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import "github.com/NVIDIA/aistore/ios"

func GetTotalDisksSize() (uint64, error) {
	var (
		totalSize      uint64
		availablePaths = GetAvail()
	)
	for mpath := range availablePaths {
		numBlocks, _, blockSize, err := ios.GetFSStats(mpath)
		if err != nil {
			return 0, err
		}
		totalSize += numBlocks * uint64(blockSize)
	}
	return totalSize, nil
}
