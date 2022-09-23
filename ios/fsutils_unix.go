// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"golang.org/x/sys/unix"
)

func getFSStats(path string) (fsStats unix.Statfs_t, err error) {
	if err = unix.Statfs(path, &fsStats); err != nil {
		glog.Errorf("failed to statfs %q, err: %v", path, err)
	}
	return
}

// - on-disk size is sometimes referred to as "apparent size"
// - ignore errors since `du` exits with status 1 if it encounters a file that couldn't be accessed (permissions)
func DirSizeOnDisk(dirPath string) (uint64, error) {
	outputBytes, _ := exec.Command("du", "-sh", dirPath, "2>/dev/null").Output()
	out := string(outputBytes)
	if out == "" {
		return 0, fmt.Errorf("failed to get on-disk size of %q", dirPath)
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
		return 0, fmt.Errorf("invalid output format from 'du': %v [%s]", err, out)
	}
	return uint64(size), nil
}

func DirFileCount(dirPath string) (int, error) {
	cmd := fmt.Sprintf("find %s -type f | wc -l", dirPath)
	outputBytes, err := exec.Command("/bin/sh", "-c", cmd).Output()
	out := string(outputBytes)
	if err != nil || out == "" {
		return 0, fmt.Errorf("failed to count the number of files in %q: %v", dirPath, err)
	}
	out = strings.TrimSpace(out)
	return strconv.Atoi(out)
}

func DirSumFileSizes(dirPath string) (uint64, error) {
	cmd := fmt.Sprintf("find %s -type f | xargs wc -c | tail -1", dirPath)
	outputBytes, err := exec.Command("/bin/sh", "-c", cmd).Output()
	out := string(outputBytes)
	if err != nil || out == "" {
		return 0, fmt.Errorf("failed to correctly sum file sizes in %q: %v", dirPath, err)
	}
	i := strings.IndexByte(out, ' ')
	if i < 0 {
		debug.Assertf(out[0] == '0', "failed to sum file sizes in %q: [%s]", dirPath, out)
		return 0, nil
	}
	return strconv.ParseUint(out[:i], 10, 0)
}
