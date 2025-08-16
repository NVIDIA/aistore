// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"

	"golang.org/x/sys/unix"
)

//
// xattrs
//

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

// removeXattr removes xattr
func removeXattr(fqn, attrName string) error {
	err := unix.Removexattr(fqn, attrName)
	if err != nil && !cos.IsErrXattrNotFound(err) {
		nlog.Errorf("failed to remove %q from %s: %v", attrName, fqn, err)
		return err
	}
	return nil
}

//
// probe max xattr size - one of {4, 16, 64, 128+} KiB
// TODO: consider adding more logic to handle:
// - _good_ errors: E2BIG | ENOSPC => try smaller; keep looping
// - _bad_ errors:  ENOTSUP | EPERM | EACCES => cos.Exit()
//

const (
	maxszBase = ".$maxszprobe"
	maxszName = "user.ais.probe"
)

func ProbeMaxsz(avail MPI) (size int64) {
	debug.Assert(len(avail) > 0)
	var mi *Mountpath
	for _, mi = range avail {
		break
	}
	dir := mi.TempDir(maxszBase)
	if err := cos.CreateDir(dir); err != nil {
		return 0
	}
	fqn := filepath.Join(dir, cos.GenTie())
	fh, err := cos.CreateFile(fqn)
	if err != nil {
		os.RemoveAll(dir)
		return 0
	}
	fh.Close()

	buf, slab := memsys.PageMM().AllocSize(128 * cos.KiB)
	debug.Assert(len(buf) == 128*cos.KiB)
	for _, size = range []int64{128 * cos.KiB, 64 * cos.KiB, 16 * cos.KiB, 4 * cos.KiB} {
		if SetXattr(fqn, maxszName, buf[:size-256]) == nil {
			break
		}
	}
	slab.Free(buf)
	os.RemoveAll(dir)
	return size
}
