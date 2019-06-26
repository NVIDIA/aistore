// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"syscall"
)

// GetXattr gets xattr by name - see also the buffered version below
func GetXattr(fqn, attrname string) ([]byte, error) {
	const maxAttrSize = 4096
	buf := make([]byte, maxAttrSize)
	return GetXattrBuf(fqn, attrname, buf)
}

// GetXattr gets xattr by name via provided buffer
func GetXattrBuf(fqn, attrname string, buf []byte) (b []byte, err error) {
	var n int
	n, err = syscall.Getxattr(fqn, attrname, buf)
	if err == nil { // returns ERANGE if len(buf) is not enough
		b = buf[:n]
	}
	return
}

// SetXattr sets xattr name = value
func SetXattr(fqn, attrname string, data []byte) (err error) {
	return syscall.Setxattr(fqn, attrname, data, 0)
}
