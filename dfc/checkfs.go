/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"container/heap"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
)

type fileinfo struct {
	file string
	stat *syscall.Stat_t
}

var fileList = make([]fileinfo, 0, 256)
var checkfscas int64

// FIXME: mountpath.enabled is never used
func checkfs() {
	if !atomic.CompareAndSwapInt64(&checkfscas, 0, 7890123) {
		glog.Infoln("checkfs is already running")
		return
	}
	mntcnt := len(ctx.mountpaths)
	fschkwg := &sync.WaitGroup{}
	fsmap := make(map[syscall.Fsid]bool, mntcnt)
	glog.Infof("checkfs start, num mp-s %d", mntcnt)
	for _, mountpath := range ctx.mountpaths {
		_, ok := fsmap[mountpath.Fsid]
		if ok {
			glog.Infof("checkfs: duplicate FSID %v, mpath %q", mountpath.Fsid, mountpath.Path)
			continue
		}
		fsmap[mountpath.Fsid] = true
		fschkwg.Add(1)
		go fsscan(mountpath.Path, fschkwg)
	}
	fschkwg.Wait()
	glog.Infoln("checkfs done")
	swapped := atomic.CompareAndSwapInt64(&checkfscas, 7890123, 0)
	assert(swapped)
}

func fsscan(mpath string, fschkwg *sync.WaitGroup) error {
	defer fschkwg.Done()
	hwm := ctx.config.Cache.FSHighWaterMark
	lwm := ctx.config.Cache.FSLowWaterMark

	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(mpath, &statfs); err != nil {
		glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
		return err
	}
	blocks, bavail, bsize := statfs.Blocks, statfs.Bavail, statfs.Bsize
	used := blocks - bavail
	usedpct := used * 100 / blocks
	glog.Infof("Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%", blocks, bavail, usedpct, hwm, lwm)

	if usedpct < uint64(hwm) {
		return nil
	}
	lwmblocks := blocks * uint64(lwm) / 100
	toevict := int64(used-lwmblocks) * bsize
	if glog.V(3) {
		glog.Infof("lwmblocks %d to-evict-bytes %d", lwmblocks, toevict)
	}

	if err := filepath.Walk(mpath, walkfunc); err != nil {
		glog.Errorf("Failed to traverse all files in dir %q, err: %v", mpath, err)
		return err
	}
	if err := doMaxAtimeHeapAndDelete(toevict); err != nil {
		glog.Errorf("Error in creating Heap and Delete for path %q, err: %v", mpath, err)
		return err
	}
	return nil
}

func walkfunc(path string, fi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("walkfunc callback invoked with err: %v", err)
		return err
	}
	// skip system files and directories
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") || fi.Mode().IsDir() {
	} else {
		var obj fileinfo
		obj.file = path
		obj.stat = fi.Sys().(*syscall.Stat_t)
		fileList = append(fileList, obj)
	}
	return nil
}

func doMaxAtimeHeapAndDelete(toevict int64) error {
	h := &PriorityQueue{}
	heap.Init(h)
	now := time.Now()
	dontevictime := now.Add(-ctx.config.Cache.DontEvictTime)
	var (
		bytecnt  int64
		maxatime time.Time
		maxfo    *FileObject
		filecnt  uint64
	)
	defer func() { fileList = fileList[:0] }() // empty filelist upon return
	for _, fo := range fileList {
		filecnt++
		file, stat := fo.file, fo.stat
		atime := time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
		mtime := time.Unix(int64(stat.Mtim.Sec), int64(stat.Mtim.Nsec))
		// atime controversy - see e.g. https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
		usetime := atime
		if mtime.After(atime) {
			usetime = mtime
		}
		// do not evict freshly-updated and most recently accessed
		if usetime.After(dontevictime) {
			if glog.V(3) {
				glog.Infof("1A: skipping %q dontevictime %v usetime %v", file, dontevictime, usetime)
			}
			continue
		}
		item := &FileObject{
			path: file, size: stat.Size, usetime: usetime, index: 0}

		if bytecnt < toevict {
			heap.Push(h, item)
			bytecnt += stat.Size
			if glog.V(3) {
				glog.Infof("1B: bytecnt %d file %q usetime %v", bytecnt, file, usetime)
			}
			continue
		}
		assert(h.Len() > 0)
		maxfo = heap.Pop(h).(*FileObject)
		maxatime = maxfo.usetime
		bytecnt -= maxfo.size
		if glog.V(3) {
			glog.Infof("1C: bytecnt %d heap len %d", bytecnt, h.Len())
		}

		// Push object into heap iff older
		if usetime.Before(maxatime) {
			heap.Push(h, item)
			bytecnt += stat.Size
			if glog.V(3) {
				glog.Infof("1D: bytecnt %d len %d", bytecnt, h.Len())
			}

			// vs maxatime
			maxfo = heap.Pop(h).(*FileObject)
			bytecnt -= maxfo.size
			if glog.V(3) {
				glog.Infof("1E: bytecnt %d len %d", bytecnt, h.Len())
			}
			maxatime = maxfo.usetime
			if glog.V(3) {
				glog.Infof("1F: file %q usetime %v maxfo.path %q maxatime %v",
					file, usetime, maxfo.path, maxatime)
			}
		}
	}
	if glog.V(3) {
		glog.Infof("max-heap len %d bytecnt %d toevict %d filecnt %d",
			h.Len(), bytecnt, toevict, filecnt)
	}
	// evict some files
	var bevicted, fevicted int64
	for h.Len() > 0 && bytecnt > 10 {
		maxfo = heap.Pop(h).(*FileObject)

		// FIXME: error not handled - will fail to reach the target
		if err := os.Remove(maxfo.path); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", maxfo.path, err)
			continue
		}
		bytecnt -= maxfo.size
		if glog.V(3) {
			glog.Infof("1E: curheapsize %d len %d", bytecnt, h.Len())
		}
		bevicted += maxfo.size
		fevicted++
	}
	if ctx.rg != nil { // FIXME: for fsscan_test only
		stats := getstorstats()
		atomic.AddInt64(&stats.bytesevicted, bevicted)
		atomic.AddInt64(&stats.filesevicted, fevicted)
	}
	return nil
}
