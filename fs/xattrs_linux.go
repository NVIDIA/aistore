/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"syscall"

	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	maxAttrSize = 1024
)

// GetXattr gets xattr by name
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

// SetXattr sets xattr name = value
func SetXattr(fqn, attrname string, data []byte) (errstr string) {
	cmn.Assert(len(data) < maxAttrSize)
	err := syscall.Setxattr(fqn, attrname, data, 0)
	if err != nil {
		errstr = fmt.Sprintf("Failed to set xattr %s: %s, err [%v]", attrname, fqn, err)
	}
	return
}
