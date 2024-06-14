// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/karrick/godirwalk"
	"golang.org/x/sys/unix"
)

func DirSizeOnDisk(originalDirPath string, withNonDirPrefix bool) (size uint64, err error) {
	dirPath := originalDirPath
	if withNonDirPrefix {
		dirPath, _ = filepath.Split(originalDirPath)
	}
	err = godirwalk.Walk(dirPath, &godirwalk.Options{Callback: func(osPathname string, entry *godirwalk.Dirent) error {
		if !entry.IsDir() && !entry.IsRegular() {
			return nil
		}
		// If prefix is set we should skip all the names that do not have the prefix.
		if withNonDirPrefix && !strings.HasPrefix(osPathname, originalDirPath) {
			return nil
		}
		stat, err := os.Lstat(osPathname)
		if err != nil {
			return err
		}
		size += uint64(stat.Size())
		return nil
	}})
	return
}

func GetFSStats(path string) (blocks, bavail uint64, bsize int64, err error) {
	var fsStats unix.Statfs_t
	fsStats, err = getFSStats(path)
	if err != nil {
		return
	}
	return fsStats.Blocks, fsStats.Bavail, int64(fsStats.Bsize), nil
}

func GetATime(osfi os.FileInfo) time.Time {
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec)
	// NOTE: see https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	return atime
}
