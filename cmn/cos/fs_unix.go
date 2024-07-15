// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "syscall"

type FS struct {
	Fs     string
	FsType string
	FsID   FsID
}

func (fs *FS) String() string { return fs.Fs + "(" + fs.FsType + ")" }

func (fs *FS) Equal(otherFs FS) bool {
	if fs.Fs == "" || otherFs.Fs == "" || fs.FsType == "" || otherFs.FsType == "" {
		return false
	}
	return fs.FsType == otherFs.FsType && fs.FsID == otherFs.FsID
}

// syscall to check that path exists (see bench/lstat)
func Stat(path string) error {
	var sys syscall.Stat_t
	return syscall.Stat(path, &sys)
}
