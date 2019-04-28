// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"syscall"

	"github.com/NVIDIA/aistore/cmn"
)

const maxAttrSize = 4096

// GetXattr gets xattr by name (NOTE: the size is limited by maxAttrSize - see the buffered version below)
func GetXattr(fqn, attrname string) ([]byte, error) {
	data := make([]byte, maxAttrSize)
	read, err := syscall.Getxattr(fqn, attrname, data)
	cmn.Assert(read < maxAttrSize)
	if err != nil {
		return nil, err
	}
	if read > 0 {
		return data[:read], nil
	}
	return nil, nil
}

// GetXattr gets xattr by name via provided buffer
func GetXattrBuf(fqn, attrname string, data []byte) (int, error) {
	cmn.Assert(data != nil)
	read, err := syscall.Getxattr(fqn, attrname, data)
	cmn.Assert(read < len(data))
	if err != nil {
		return 0, err
	}
	return read, nil
}

// SetXattr sets xattr name = value
func SetXattr(fqn, attrname string, data []byte) (err error) {
	return syscall.Setxattr(fqn, attrname, data, 0)
}
