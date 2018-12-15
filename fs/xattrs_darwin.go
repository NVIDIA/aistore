/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"fmt"
	"syscall"
	"unsafe"

	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	maxAttrSize = 1024
)

// GetXattr returns specific attribute for specified fqn.
func GetXattr(fqn string, attrname string) ([]byte, string) {
	buf := make([]byte, maxAttrSize)
	// Read into buffer of that size.
	readstr, _, err := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(fqn))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(unsafe.Pointer(&buf[0])), uintptr(maxAttrSize), uintptr(0), uintptr(0))
	cmn.Assert(int(readstr) < maxAttrSize)
	if err != syscall.Errno(0) && err != syscall.ENODATA {
		errstr := fmt.Sprintf("Failed to get extended attr for fqn %s attr %s, err: %v",
			fqn, attrname, err)
		return nil, errstr
	}
	if int(readstr) > 0 {
		return buf[:int(readstr)], ""
	}

	return nil, ""
}

// SetXattr sets specific named attribute for specific fqn.
func SetXattr(fqn string, attrname string, data []byte) (errstr string) {
	datalen := len(data)
	cmn.Assert(datalen < maxAttrSize)
	_, _, err := syscall.Syscall6(syscall.SYS_SETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(fqn))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(datalen), uintptr(0), uintptr(0))

	if err != syscall.Errno(0) {
		errstr = fmt.Sprintf("Failed to set extended attr for fqn %s attr %s, err: %v",
			fqn, attrname, err)
	}
	return
}
