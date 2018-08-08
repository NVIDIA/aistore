/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package fs

import (
	"path/filepath"
	"syscall"
)

type MountpathInfo struct {
	Path       string // Cleaned OrigPath
	OrigPath   string // As entered by the user, must be used for logging / returning errors
	Fsid       syscall.Fsid
	FileSystem string
}

func newMountpath(path string, fsid syscall.Fsid, fs string) *MountpathInfo {
	cleanPath := filepath.Clean(path)
	return &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   path,
		Fsid:       fsid,
		FileSystem: fs,
	}
}
