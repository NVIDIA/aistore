// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/NVIDIA/aistore/cmn/cos"
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

// mountpath must be either a direct descendant of a mount point, or the mount point itself
// (when `rel` == ".")
// NOTE: assuming ASCII, not unescaping from util-linux escapes: \040 (space), \011 (tab), \012 (nl), \134 (backslash))
func _resolve(mountpath string, fh *os.File) (fs, fsType string, _ error) {
	var (
		bestMatch string
		scanner   = bufio.NewScanner(fh)
	)
outer:
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) < 3 {
			continue
		}

		mountPoint := string(fields[1])

		// intentionally: relative path from `mountpath` to `mountPoint`
		// (shorter = better)
		rel, err := filepath.Rel(mountpath, mountPoint)
		if err != nil {
			continue
		}
		if rel == "." {
			// exact match: mountpath _is_ the mount point
			return string(fields[0]), string(fields[2]), nil
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
			fs = string(fields[0])     // device/source
			fsType = string(fields[2]) // fs type
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("FATAL: failed reading Linux %q, err: %w", procmounts, err)
	}
	if bestMatch == "" {
		return "", "", cos.NewErrNotFoundFmt(nil, "mount point for mountpath %q", mountpath)
	}
	return fs, fsType, nil
}

// DirectOpen opens a file with direct disk access (with OS caching disabled).
func DirectOpen(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, syscall.O_DIRECT|flag, perm)
}
