// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"syscall"
)

// Get specific attribute for specified path.
func Getxattr(path string, attrname string) ([]byte, string) {
	// find size.
	size, err := syscall.Getxattr(path, attrname, nil)
	if err != nil {
		errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return nil, errstr
	}
	if size > 0 {
		data := make([]byte, size)
		read, err := syscall.Getxattr(path, attrname, data)
		if err != nil {
			errstr := fmt.Sprintf("Failed to get extended attr for path %s attr %s, err: %v",
				path, attrname, err)
			return nil, errstr
		}
		return data[:read], ""
	}
	return []byte{}, ""
}

// Set specific named attribute for specific path.
func Setxattr(path string, attrname string, data []byte) (errstr string) {
	err := syscall.Setxattr(path, attrname, data, 0)
	if err != nil {
		errstr = fmt.Sprintf("Failed to set extended attr for path %s attr %s, err: %v",
			path, attrname, err)
		return
	}
	return ""
}

// Delete specific named attribute for specific path.
func Deletexattr(path string, attrname string) (errstr string) {
	err := syscall.Removexattr(path, attrname)
	if err != nil {
		errstr = fmt.Sprintf("Failed to remove extended attr for path %s attr %s, err: %v",
			path, attrname, err)
	}
	return ""
}
