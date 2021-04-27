// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"syscall"
)

func makeFsInfo(mpath string) (fsInfo FilesystemInfo, err error) {
	var fsStats syscall.Statfs_t
	if err := syscall.Statfs(mpath, &fsStats); err != nil {
		return fsInfo, fmt.Errorf("cannot statfs fspath %q, err: %w", mpath, err)
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

	return FilesystemInfo{
		Fs:     charsToString(fsStats.Fstypename[:]),
		FsType: charsToString(fsStats.Mntfromname[:]),
		FsID:   fsStats.Fsid.Val,
	}, nil
}

// DirectOpen opens a file with direct disk access(with OS caching disabled)
func DirectOpen(path string, flag int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return file, err
	}

	// Non-zero F_NOCACHE = caching off
	_, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(file.Fd()), syscall.F_NOCACHE, 1)
	if e1 != 0 {
		file.Close()
		return nil, fmt.Errorf("Failed to set F_NOCACHE: %s", e1)
	}

	return file, nil
}
