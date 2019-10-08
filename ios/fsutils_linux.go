// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
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

func GetATime(osfi os.FileInfo) time.Time {
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
	// NOTE: see https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	return atime
}

func GetDirSize(dirPath string) (uint64, error) {
	// NOTE: we ignore the error since the `du` will exit with status 1 code
	// in case there was a file that could not be accessed (not enough permissions).
	outputBytes, _ := exec.Command("du", "-sh", dirPath, "2>/dev/null").Output()
	out := string(outputBytes)
	if out == "" {
		return 0, fmt.Errorf("failed to get the total size of the directory %q", dirPath)
	}
	idx := strings.Index(out, "\t")
	if idx == -1 {
		return 0, fmt.Errorf("invalid output format from 'du' command")
	}
	out = out[:idx]
	size, err := cmn.S2B(out)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid output format from 'du' command, err: %v", err)
	}
	return uint64(size), nil
}

func GetFileCount(dirPath string) (int, error) {
	outputBytes, err := exec.Command("bash", "-c", fmt.Sprintf("ls -1 %s | wc -l", dirPath)).Output()
	out := string(outputBytes)
	if err != nil || out == "" {
		return 0, fmt.Errorf("failed to get the number of files in the directory %q, err: %v", dirPath, err)
	}
	out = strings.TrimRight(out, "\n")
	return strconv.Atoi(out)
}
