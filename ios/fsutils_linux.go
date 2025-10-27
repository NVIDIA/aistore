// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"golang.org/x/sys/unix"
)

const maxStackSize = 65536
const dirOpenMode = syscall.O_RDONLY | syscall.O_NOCTTY | syscall.O_NONBLOCK | syscall.O_NOFOLLOW | syscall.O_CLOEXEC | syscall.O_DIRECTORY

// DirSizeOnDisk calculates the total size of a directory on disk, including its subdirectories.
// TODO: We should also calculate and include `xattr` size.
func DirSizeOnDisk(dirPath string, withNonDirPrefix bool) (size uint64, err error) {
	var prefix string
	if withNonDirPrefix {
		dirPath, prefix = filepath.Split(dirPath)
	}

	fd, err := syscall.Open(dirPath, dirOpenMode, 0)
	if err != nil {
		return size, err
	}
	defer syscall.Close(fd)

	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		return size, err
	}

	size, err = dirSizeOnDiskFD(fd, prefix, 0)
	return size + uint64(stat.Size), err
}

// dirSizeOnDiskFD calculates directory size on disk based on the opened
// file descriptor to said directory.
func dirSizeOnDiskFD(fd int, prefix string, stackSize int) (size uint64, err error) {
	if stackSize >= maxStackSize {
		return size, fmt.Errorf("DirSizeOnDisk stack overflow, exceeded maximum size of %d nested directories", maxStackSize)
	}

	buf := make([]byte, 16*cos.KiB)
	for {
		n, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			// syscall.EINTR - interrupted by signal.
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			// syscall.EINVAL - can occur when reading protected directory.
			if errors.Is(err, syscall.EINVAL) {
				return size, nil
			}
			return size, err
		}
		if n <= 0 { // end of directory: normal exit
			return size, nil
		}
		workBuffer := buf[:n] // trim work buffer to number of bytes read

		for len(workBuffer) > 0 {
			var sde syscall.Dirent
			copy((*[unsafe.Sizeof(syscall.Dirent{})]byte)(unsafe.Pointer(&sde))[:], workBuffer)
			workBuffer = workBuffer[sde.Reclen:] // Advance buffer for next iteration through loop.

			// Skip `.` and `..` dirents as well as `inode == 0` (inode marked for deletion).
			if sde.Ino == 0 || nameEqual(&sde.Name, "") || nameEqual(&sde.Name, ".") || nameEqual(&sde.Name, "..") {
				continue
			}
			// If prefix is set we should skip all the names that do not have the prefix.
			if prefix != "" && !nameHasPrefix(&sde.Name, prefix) {
				continue
			}

			// Skip anything except files and directories.
			if sde.Type != syscall.DT_REG && sde.Type != syscall.DT_DIR {
				continue
			}

			// use architecture-specific fstatat syscall
			var stat syscall.Stat_t
			_, _, errno := syscall.Syscall6(fstatatSyscall,
				uintptr(fd), uintptr(unsafe.Pointer(&sde.Name[0])), uintptr(unsafe.Pointer(&stat)), uintptr(unix.AT_SYMLINK_NOFOLLOW), 0, 0)
			if errno != 0 {
				return size, errno
			}
			size += uint64(stat.Size)

			if sde.Type == syscall.DT_DIR {
				fd, _, errno := syscall.Syscall6(syscall.SYS_OPENAT,
					uintptr(fd), uintptr(unsafe.Pointer(&sde.Name[0])), uintptr(dirOpenMode), uintptr(0), 0, 0)
				if errno != 0 {
					// syscall.EPERM - permission denied to open directory.
					if errors.Is(err, syscall.EPERM) {
						continue
					}
					return size, errno
				}
				n, err := dirSizeOnDiskFD(int(fd), "", stackSize+1)
				_ = syscall.Close(int(fd))
				if err != nil {
					return size, err
				}
				size += n
			}
		}
	}
}

func nameEqual(name *[256]int8, s string) bool {
	return nameHasPrefix(name, s) && name[len(s)] == '\x00'
}

func nameHasPrefix(name *[256]int8, s string) bool {
	if len(s) >= 255 { // Not 256 because we know that `name` has NULL character.
		return false
	}
	for i := range len(s) {
		if byte(name[i]) != s[i] {
			return false
		}
	}
	return true
}

func GetFSStats(path string) (blocks, bavail uint64, bsize int64, err error) {
	var statfs unix.Statfs_t
	statfs, err = getFSStats(path)
	if err != nil {
		return
	}
	debug.Assert(statfs.Blocks > 0)
	debug.Assert(statfs.Bsize > 0)
	return statfs.Blocks, statfs.Bavail, statfs.Bsize, nil
}

func GetATime(osfi os.FileInfo) time.Time {
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
	// NOTE: see https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	return atime
}
