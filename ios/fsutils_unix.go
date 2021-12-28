// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"golang.org/x/sys/unix"
)

func getFSStats(path string) (fsStats unix.Statfs_t, err error) {
	if err = unix.Statfs(path, &fsStats); err != nil {
		glog.Errorf("failed to statfs %q, err: %v", path, err)
	}
	return
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
	// `du` can return ',' as float separator what cannot be parsed properly.
	out = strings.ReplaceAll(out, ",", ".")
	size, err := cos.S2B(out)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid output format from 'du' command, err: %v", err)
	}
	return uint64(size), nil
}

func GetFileCount(dirPath string) (int, error) {
	outputBytes, err := exec.Command("bash", "-c", fmt.Sprintf("find %s -type f | wc -l", dirPath)).Output()
	out := string(outputBytes)
	if err != nil || out == "" {
		return 0, fmt.Errorf("failed to get the number of files in the directory %q, err: %v", dirPath, err)
	}
	out = strings.TrimSpace(out)
	return strconv.Atoi(out)
}
