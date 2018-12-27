// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"os"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

func GetFSStats(path string) (blocks uint64, bavail uint64, bsize int64, err error) {
	fsStats := syscall.Statfs_t{}
	if err = syscall.Statfs(path, &fsStats); err != nil {
		glog.Errorf("Failed to statfs %q, err: %v", path, err)
		return
	}
	return fsStats.Blocks, fsStats.Bavail, fsStats.Bsize, nil
}

func GetFSUsedPercentage(path string) (usedPercentage int64, ok bool) {
	totalBlocks, blocksAvailable, _, err := GetFSStats(path)
	if err != nil {
		return
	}
	usedBlocks := totalBlocks - blocksAvailable
	return int64(usedBlocks * 100 / totalBlocks), true
}

func GetAmTimes(osfi os.FileInfo) (time.Time, time.Time, *syscall.Stat_t) {
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
	// NOTE: see https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	mtime := osfi.ModTime()
	return atime, mtime, stat
}
