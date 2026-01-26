// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"time"

	"golang.org/x/sys/unix"
)

const (
	stxFlags = unix.AT_STATX_DONT_SYNC | unix.AT_SYMLINK_NOFOLLOW
	stxMask  = unix.STATX_MTIME
)

// return mtime time in UTC using statx(2)
func MtimeUTC(path string) (time.Time, error) {
	var stx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, stxFlags, stxMask, &stx); err != nil {
		return time.Time{}, err
	}
	t := time.Unix(stx.Mtime.Sec, int64(stx.Mtime.Nsec))
	return t.UTC(), nil
}

// set atime and mtime via utimensat(2)
// (a faster alternative to os.Chtimes())
func Chtimes(path string, atime, mtime time.Time) error {
	ts := []unix.Timespec{
		unix.NsecToTimespec(atime.UnixNano()),
		unix.NsecToTimespec(mtime.UnixNano()),
	}
	return unix.UtimesNanoAt(unix.AT_FDCWD, path, ts, unix.AT_SYMLINK_NOFOLLOW)
}

// as above but only atime
func ChtimeOnly(path string, atime time.Time) error {
	ts := []unix.Timespec{
		unix.NsecToTimespec(atime.UnixNano()),
		{Nsec: unix.UTIME_OMIT},
	}
	return unix.UtimesNanoAt(unix.AT_FDCWD, path, ts, unix.AT_SYMLINK_NOFOLLOW)
}
