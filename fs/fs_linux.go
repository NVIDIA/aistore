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

const procmounts = "/proc/mounts"

func (mi *Mountpath) resolveFS() error {
	var fsStats syscall.Statfs_t
	if err := syscall.Statfs(mi.Path, &fsStats); err != nil {
		return fmt.Errorf("cannot statfs mountpath %q, err: %w", mi.Path, err)
	}

	fh, e := os.Open(procmounts)
	if e != nil {
		return fmt.Errorf("FATAL: failed to open Linux %q, err: %w", procmounts, e)
	}

	fs, fsType, err := _resolve(mi.Path, fh)
	fh.Close()
	if err != nil {
		return err
	}

	mi.Fs = fs
	mi.FsType = fsType
	mi.FsID = fsStats.Fsid.X__val
	return nil
}

// NOTE: filepath.Rel() returns '.' not an empty string when there is an exact match.
// TODO: consider excepting only exact match (rel == '.') as a match

func _resolve(path string, fh *os.File) (fs, fsType string, _ error) {
	var (
		bestMatch string
		scanner   = bufio.NewScanner(fh)
	)
outer:
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
		if rel == "." {
			// when ais mountpath EQ mount point
			return fields[0], fields[2], nil
		}
		for i := range len(rel) {
			if rel[i] != '.' && rel[i] != filepath.Separator {
				// mountpath can be a direct descendant (of a mount point),
				// with `rel` containing one or more "../" snippets
				continue outer
			}
		}
		if bestMatch == "" || len(rel) < len(bestMatch) /*minimizing `rel`*/ {
			bestMatch = rel
			fs = fields[0]
			fsType = fields[2]
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("FATAL: failed reading Linux %q, err: %w", procmounts, err)
	}
	if bestMatch == "" {
		return "", "", fmt.Errorf("failed to resolve mountpath %q: mount point not found", path)
	}
	return fs, fsType, nil
}

// DirectOpen opens a file with direct disk access (with OS caching disabled).
func DirectOpen(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, syscall.O_DIRECT|flag, perm)
}
