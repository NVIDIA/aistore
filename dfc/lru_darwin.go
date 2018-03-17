// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"syscall"
	"time"

	"github.com/golang/glog"
)

func getToEvict(mpath string, hwm uint32, lwm uint32) (int64, error) {
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(mpath, &statfs); err != nil {
		glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
		return -1, err
	}
	blocks, bavail, bsize := statfs.Blocks, statfs.Bavail, statfs.Bsize
	used := blocks - bavail
	usedpct := used * 100 / blocks
	glog.Infof("Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%", blocks, bavail, usedpct, hwm, lwm)
	if usedpct < uint64(hwm) {
		return 0, nil // 0 to evict
	}
	lwmblocks := blocks * uint64(lwm) / 100
	return int64(used-lwmblocks) * int64(bsize), nil

}

func getAmTimes(osfi os.FileInfo) (time.Time, time.Time, *syscall.Stat_t) {
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(int64(stat.Atimespec.Sec), int64(stat.Atimespec.Nsec))
	// atime controversy, see e.g. https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	mtime := time.Unix(int64(stat.Mtimespec.Sec), int64(stat.Mtimespec.Nsec))
	return atime, mtime, stat
}
