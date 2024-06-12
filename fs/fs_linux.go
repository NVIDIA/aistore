// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func (mi *Mountpath) resolveFS() error {
	var fsStats syscall.Statfs_t
	if err := syscall.Statfs(mi.Path, &fsStats); err != nil {
		return fmt.Errorf("cannot statfs fspath %q, err: %w", mi.Path, err)
	}
	fs, fsType, err := fqn2FsInfo(mi.Path)
	if err != nil {
		return err
	}

	mi.Fs = fs
	mi.FsType = fsType
	mi.FsID = fsStats.Fsid.X__val
	return nil
}

// fqn2FsInfo is used only at startup to store file systems for each mountpath.
func fqn2FsInfo(path string) (fs, fsType string, err error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	var (
		bestMatch string
		scanner   = bufio.NewScanner(file)
	)

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		mountPoint := fields[1]
		rel, err := filepath.Rel(path, mountPoint)
		if err != nil {
			continue
		}
		if bestMatch == "" || len(rel) < len(bestMatch) {
			bestMatch = rel
			fs = fields[0]
			fsType = fields[2]
		}
	}

	if err := scanner.Err(); err != nil {
		return "", "", err
	}

	// NOTE: `filepath.Rel` returns `.` (not an empty string) when there is an exact match.
	if bestMatch == "" {
		return "", "", fmt.Errorf("mount point not found for path: %q", path)
	}

	return
}

// DirectOpen opens a file with direct disk access (with OS caching disabled).
func DirectOpen(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, syscall.O_DIRECT|flag, perm)
}
