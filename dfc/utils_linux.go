// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"fmt"
	"syscall"

	"github.com/NVIDIA/dfcpub/cmn"
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

// DeleteXattr deletes specific named attribute for specific fqn.
func DeleteXattr(fqn string, attrname string) (errstr string) {
	err := syscall.Removexattr(fqn, attrname)
	if err != nil {
		errstr = fmt.Sprintf("Failed to remove xattr %s: %s, err [%v]", attrname, fqn, err)
	}
	return
}

func TotalMemory() (mb uint64, err error) {
	sysinfo := &syscall.Sysinfo_t{}
	if err = syscall.Sysinfo(sysinfo); err != nil {
		return
	}
	mb = sysinfo.Totalram * uint64(sysinfo.Unit) / cmn.MiB
	return
}
