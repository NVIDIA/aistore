// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	maxAttrSize = 1024
)

// Getxattr returns specific attribute for specified fqn.
func Getxattr(fqn string, attrname string) ([]byte, string) {
	buf := make([]byte, maxAttrSize)
	// Read into buffer of that size.
	readstr, _, err := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(fqn))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(unsafe.Pointer(&buf[0])), uintptr(maxAttrSize), uintptr(0), uintptr(0))
	assert(int(readstr) < maxAttrSize)
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

// Setxattr sets specific named attribute for specific fqn.
func Setxattr(fqn string, attrname string, data []byte) (errstr string) {
	datalen := len(data)
	assert(datalen < maxAttrSize)
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

// Deletexattr deletes specific named attribute for specific fqn.
func Deletexattr(fqn string, attrname string) (errstr string) {
	_, _, err := syscall.Syscall(syscall.SYS_REMOVEXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(fqn))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(0))
	if err != syscall.Errno(0) {
		errstr = fmt.Sprintf("Failed to remove extended attr for fqn %s attr %s, err: %v",
			fqn, attrname, err)
	}
	return
}
