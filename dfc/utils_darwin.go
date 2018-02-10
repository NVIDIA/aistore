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
	// find size.
	sizestr, _, err := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(path))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(0), uintptr(0), uintptr(0), uintptr(0))
	if err != syscall.Errno(0) {
		errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return nil, errstr
	}
	size := int(sizestr)
	if size > 0 {
		buf := make([]byte, size)
		// Read into buffer of that size.
		readstr, _, err := syscall.Syscall6(syscall.SYS_GETXATTR,
			uintptr(unsafe.Pointer(syscall.StringBytePtr(path))),
			uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
			uintptr(unsafe.Pointer(&buf[0])), uintptr(size), uintptr(0), uintptr(0))
		if err != syscall.Errno(0) {
			errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
				path, attrname, err)
			return nil, errstr
		}
		return buf[:int(readstr)], ""
	}
	return []byte{}, ""
}

// Set specific named attribute for specific path.
func Setxattr(path string, attrname string, data []byte) (errstr string) {
	var dataval *byte = nil
	datalen := len(data)
	if datalen > 0 {
		dataval = &data[0]
	}
	_, _, err := syscall.Syscall6(syscall.SYS_SETXATTR,
		uintptr(unsafe.Pointer(syscall.StringBytePtr(path))),
		uintptr(unsafe.Pointer(syscall.StringBytePtr(attrname))),
		uintptr(unsafe.Pointer(dataval)),
		uintptr(datalen), uintptr(0), uintptr(0))

	if err != syscall.Errno(0) {
		errstr = fmt.Sprintf("Failed to set extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return errstr
	}
	return ""
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
	return ""
}
