// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

////////////
// xattrs //
////////////

// GetXattr gets xattr by name - see also the buffered version below
func GetXattr(fqn, attrName string) ([]byte, error) {
	const maxAttrSize = 4096
	buf := make([]byte, maxAttrSize)
	return GetXattrBuf(fqn, attrName, buf)
}

// GetXattr gets xattr by name via provided buffer
func GetXattrBuf(fqn, attrName string, buf []byte) (b []byte, err error) {
	var n int
	n, err = unix.Getxattr(fqn, attrName, buf)
	if err == nil { // returns ERANGE if len(buf) is not enough
		b = buf[:n]
	}
	return
}

// SetXattr sets xattr name = value
func SetXattr(fqn, attrName string, data []byte) (err error) {
	return unix.Setxattr(fqn, attrName, data, 0)
}

func CopyXattr(srcFQN, dstFQN, attrName string) error {
	b, err := GetXattr(srcFQN, attrName)
	if err != nil {
		if err == syscall.ENODATA {
			return nil
		}
		return err
	}
	if err := SetXattr(dstFQN, attrName, b); err != nil {
		return err
	}
	return nil
}

//////////
// misc //
//////////

func Access(path string) error { return syscall.Access(path, syscall.F_OK) }

// fqn2fsAtStartup is used only at startup to store file systems for each mountpath.
func fqn2fsAtStartup(fqn string) (string, error) {
	getFSCommand := fmt.Sprintf("df -P '%s' | awk 'END{print $1}'", fqn)
	outputBytes, err := exec.Command("sh", "-c", getFSCommand).Output()
	if err != nil || len(outputBytes) == 0 {
		return "", fmt.Errorf("unable to retrieve FS from fspath %s, err: %v", fqn, err)
	}

	return strings.TrimSpace(string(outputBytes)), nil
}
