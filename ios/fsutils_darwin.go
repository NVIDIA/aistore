// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func DirSizeOnDisk(dirPath string, withNonDirPrefix bool) (uint64, error) {
	// BSD implementation of du uses -A option for apparent size and -c to show a total
	cmd := exec.Command("du", "-Ac", dirPath)
	// Output block size with -A option will be 512
	return executeDU(cmd, dirPath, withNonDirPrefix, 512)
}

func GetFSStats(path string) (blocks, bavail uint64, bsize int64, err error) {
	var fsStats unix.Statfs_t
	fsStats, err = getFSStats(path)
	if err != nil {
		return
	}
	return fsStats.Blocks, fsStats.Bavail, int64(fsStats.Bsize), nil
}

func GetATime(osfi os.FileInfo) time.Time {
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec)
	// NOTE: see https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	return atime
}
