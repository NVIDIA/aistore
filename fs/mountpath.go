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
	Path       string       `json:"path"` // clean
	OrigPath   string       `json:"-"`    // As entered by the user, must be used for logging / returning errors
	Fsid       syscall.Fsid `json:"fsid"`
	FileSystem string       `json:"fileSystem"`
}

func NewMountpath(path string, fsid syscall.Fsid, fs string) *MountpathInfo {
	cleanPath := filepath.Clean(path)
	return &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   path,
		Fsid:       fsid,
		FileSystem: fs,
	}
}
