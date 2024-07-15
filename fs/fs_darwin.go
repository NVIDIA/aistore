// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"syscall"
)

func (mi *Mountpath) ResolveFS() error {
	var fsStats syscall.Statfs_t
	if err := syscall.Statfs(mi.Path, &fsStats); err != nil {
		return fmt.Errorf("cannot statfs fspath %q, err: %w", mi.Path, err)
	}

	charsToString := func(x []int8) string {
		s := ""
		for _, c := range x {
			if c == 0 {
				break
			}
			s += string(uint8(c))
		}
		return s
	}

	mi.Fs = charsToString(fsStats.Fstypename[:])
	mi.FsType = charsToString(fsStats.Mntfromname[:])
	mi.FsID = fsStats.Fsid.Val
	return nil
}

// DirectOpen opens a file with direct disk access (with OS caching disabled).
func DirectOpen(path string, flag int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return file, err
	}

	// Non-zero F_NOCACHE = caching off
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, file.Fd(), syscall.F_NOCACHE, 1)
	if errno != 0 {
		file.Close()
		return nil, fmt.Errorf("failed to set F_NOCACHE: %s", errno)
	}

	return file, nil
}
