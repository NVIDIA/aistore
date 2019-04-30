// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"syscall"
)

const maxAttrSize = 4096

// GetXattr gets xattr by name - see also the buffered version below
func GetXattr(fqn, attrname string) ([]byte, error) {
	buf := make([]byte, maxAttrSize)
	return GetXattrBuf(fqn, attrname, buf)
}

// GetXattr gets xattr by name via provided buffer
func GetXattrBuf(fqn, attrname string, buf []byte) ([]byte, error) {
	n, err := syscall.Getxattr(fqn, attrname, buf)
	if err != nil { // returns ERANGE if len(data) is not enough
		return nil, err
	}
	if n < 0 {
		return nil, syscall.ENOENT
	}
	return buf[:n], nil
}

// SetXattr sets xattr name = value
func SetXattr(fqn, attrname string, data []byte) (err error) {
	return syscall.Setxattr(fqn, attrname, data, 0)
}
