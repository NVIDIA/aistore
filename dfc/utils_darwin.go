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

// Get specific attribute for specified path.
func Getxattr(path string, attrname string) ([]byte, string) {
	buf := make([]byte, MAXATTRSIZE)
	// Read into buffer of that size.
	readstr, _, err := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(path))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(unsafe.Pointer(&buf[0])), uintptr(MAXATTRSIZE), uintptr(0), uintptr(0))
	assert(int(readstr) < MAXATTRSIZE)
	if err != syscall.Errno(0) {
		errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return nil, errstr
	}
	return buf[:int(readstr)], ""
}

// Set specific named attribute for specific path.
func Setxattr(path string, attrname string, data []byte) (errstr string) {
	datalen := len(data)
	assert(datalen < MAXATTRSIZE)
	_, _, err := syscall.Syscall6(syscall.SYS_SETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(path))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(datalen), uintptr(0), uintptr(0))

	if err != syscall.Errno(0) {
		errstr = fmt.Sprintf("Failed to set extended attr for path %s attr %s, err: %v",
			path, attrname, err)
	}
	return
}

// Delete specific named attribute for specific path.
func Deletexattr(path string, attrname string) (errstr string) {
	_, _, err := syscall.Syscall(syscall.SYS_REMOVEXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(path))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(0))
	if err != syscall.Errno(0) {
		errstr = fmt.Sprintf("Failed to remove extended attr for path %s attr %s, err: %v",
			path, attrname, err)
	}
	return
}
