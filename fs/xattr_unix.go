// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"

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

func IsXattrExist(fqn, attrName string) bool {
	_, err := unix.Getxattr(fqn, attrName, nil)
	return err == nil // note: not differentiating ENODATA vs other errors
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

// probe max xattr size - one of {4, 16, 64, 128+} KiB
// TODO: consider failing startup upon !(E2BIG | ENOSPC)

const (
	maxszBase   = ".$maxszprobe"
	maxszName   = "user.ais.probe"
	maxszPrompt = "max xattr size"
)

func ProbeMaxsz(avail MPI, tname string) {
	debug.Assert(len(avail) > 0)
	var mi *Mountpath
	for _, mi = range avail {
		break
	}
	size := mi.probexsz()
	switch size {
	case 4 * cos.KiB:
		nlog.Warningln(maxszPrompt, "[", size, mi.String(), "]")
	case 0:
		nlog.Errorln(tname, "failed to probe xattr size: [", 0, mi.String(), "]")
		nlog.Errorln(tname, "check filesystem support for extended attributes!")
	default:
		nlog.Infoln(maxszPrompt, "[", size, mi.String(), "]")
	}
}

func (mi *Mountpath) probexsz() (size int64) {
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
		err := SetXattr(fqn, maxszName, buf[:size-256])
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.E2BIG) {
			nlog.Errorln(maxszPrompt, "failure:", err, "[", size, mi.String(), "]")
		}
	}
	slab.Free(buf)
	os.RemoveAll(dir)
	return size
}
