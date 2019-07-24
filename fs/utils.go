// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
)

// MvFile moves file from src to dst location. It should be used when object
// is moved across different disks/mountpaths.
func MvFile(srcFQN, dstFQN string) error {
	fsrc, err := os.Open(srcFQN)
	if err != nil {
		return err
	}
	defer fsrc.Close()
	fi, err := fsrc.Stat()
	if err != nil {
		return err
	}
	if err := cmn.CreateDir(filepath.Dir(dstFQN)); err != nil {
		return err
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	fdst, err := os.OpenFile(dstFQN, flag, fi.Mode()&os.ModePerm)
	if err != nil {
		return err
	}
	if _, err = io.Copy(fdst, fsrc); err != nil {
		fdst.Close()
		os.Remove(dstFQN)
		return err
	}
	if err := fdst.Close(); err != nil {
		return err
	}
	if err := MoveXattr(srcFQN, dstFQN); err != nil {
		return err
	}
	return os.Remove(srcFQN)
}

func MoveXattr(srcFQN, dstFQN string) error {
	if err := CopyXattr(srcFQN, dstFQN, cmn.XattrLOM); err != nil {
		return err
	}
	if err := CopyXattr(srcFQN, dstFQN, cmn.XattrBMD); err != nil {
		return err
	}
	return nil
}
