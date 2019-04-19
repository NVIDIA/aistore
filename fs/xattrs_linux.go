// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"syscall"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	maxAttrSize = 1024
)

// GetXattr gets xattr by name - xattr size is limited(maxAttrSize)
func GetXattr(fqn, attrname string) ([]byte, string) {
	data := make([]byte, maxAttrSize)
	read, err := syscall.Getxattr(fqn, attrname, data)
	cmn.Assert(read < maxAttrSize)
	if err != nil && err != syscall.ENODATA {
		return nil, fmt.Sprintf("Failed to get xattr %s: %s, err [%v]", attrname, fqn, err)
	}
	if read > 0 {
		return data[:read], ""
	}
	return nil, ""
}

// GetXattr gets xattr by name - with external buffer to load big chunks
func GetXattrBuf(fqn, attrname string, data []byte) (int, string) {
	cmn.AssertMsg(data != nil, "GetXattrBuf - buffer is nil")
	read, err := syscall.Getxattr(fqn, attrname, data)
	cmn.Assert(read < len(data))
	if err != nil && err != syscall.ENODATA {
		return 0, fmt.Sprintf("Failed to get xattr %s: %s, err [%v]", attrname, fqn, err)
	}
	return read, ""
}

// SetXattr sets xattr name = value
func SetXattr(fqn, attrname string, data []byte) (errstr string) {
	err := syscall.Setxattr(fqn, attrname, data, 0)
	if err != nil {
		errstr = fmt.Sprintf("Failed to set xattr %s: %s, err [%v]", attrname, fqn, err)
	}
	return
}
