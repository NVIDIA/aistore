// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"golang.org/x/sys/unix"
)

type FilesystemInfo struct {
	Fs     string
	FsType string
	FsID   cos.FsID
}

////////////
// xattrs //
////////////

// GetXattr gets xattr by name - see also the buffered version below
func GetXattr(fqn, attrName string) ([]byte, error) {
	const maxAttrSize = 4096
	buf := make([]byte, maxAttrSize)
	return GetXattrBuf(fqn, attrName, buf)
}

// GetXattr gets xattr by name via provided buffer
func GetXattrBuf(fqn, attrName string, buf []byte) (b []byte, err error) {
	var n int
	n, err = unix.Getxattr(fqn, attrName, buf)
	if err == nil { // returns ERANGE if len(buf) is not enough
		b = buf[:n]
	}
	return
}

// SetXattr sets xattr name = value
func SetXattr(fqn, attrName string, data []byte) (err error) {
	return unix.Setxattr(fqn, attrName, data, 0)
}

// removeXattr removes xattr
func removeXattr(fqn, attrName string) error {
	err := unix.Removexattr(fqn, attrName)
	if err != nil && !cos.IsErrXattrNotFound(err) {
		glog.Errorf("failed to remove %q from %s: %v", attrName, fqn, err)
		return err
	}
	return nil
}

//////////
// misc //
//////////

func Access(path string) error { return syscall.Access(path, syscall.F_OK) }
