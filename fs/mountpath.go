/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package fs

import (
	"path/filepath"
	"syscall"

	"github.com/NVIDIA/dfcpub/constants"
	"github.com/OneOfOne/xxhash"
)

type MountpathInfo struct {
	Path       string // Cleaned OrigPath
	OrigPath   string // As entered by the user, must be used for logging / returning errors
	Fsid       syscall.Fsid
	FileSystem string
	PathDigest uint64
}

func newMountpath(path string, fsid syscall.Fsid, fs string) *MountpathInfo {
	cleanPath := filepath.Clean(path)
	return &MountpathInfo{
		Path:       cleanPath,
		OrigPath:   path,
		Fsid:       fsid,
		FileSystem: fs,
		PathDigest: xxhash.ChecksumString64S(cleanPath, constants.MLCG32),
	}
}
