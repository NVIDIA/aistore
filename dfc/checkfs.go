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
var critsect = &sync.Mutex{}

func checkfs() {
	critsect.Lock()
	defer critsect.Unlock()
	mntcnt := len(ctx.mntpath)
	fschkwg := &sync.WaitGroup{}
	glog.Infof("checkfs start, num mp-s %d", mntcnt)
	for i := 0; i < mntcnt; i++ {
		fschkwg.Add(1)
		go fsscan(ctx.mntpath[i].Path, fschkwg)
	}
	fschkwg.Wait()
	glog.Infof("checkfs done")
	return
}

func fsscan(mntpath string, fschkwg *sync.WaitGroup) error {
	defer fschkwg.Done()
	hwm := ctx.config.Cache.FSHighWaterMark
	lwm := ctx.config.Cache.FSLowWaterMark

	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(mntpath, &statfs); err != nil {
		glog.Errorf("Failed to statfs mp %q, err: %v", mntpath, err)
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

	if err := filepath.Walk(mntpath, walkfunc); err != nil {
		glog.Errorf("Failed to traverse all files in dir %q, err: %v", mntpath, err)
		return err
	}
	if err := doMaxAtimeHeapAndDelete(toevict); err != nil {
		glog.Errorf("Error in creating Heap and Delete for path %q, err: %v", mntpath, err)
		return err
	}
	return nil
}

func walkfunc(path string, fi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("Failed to walk, err: %v", err)
		return err
	}
	// skip system files and directories
	if strings.HasPrefix(path, ".") || fi.Mode().IsDir() {
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
		item := &FileObject{
			path: file, size: stat.Size, atime: atime, index: 0}

		if bytecnt < toevict {
			heap.Push(h, item)
			bytecnt += stat.Size
			if glog.V(3) {
				glog.Infof("1A: bytecnt %d file %q atime %v", bytecnt, file, atime)
			}
			continue
		}
		assert(h.Len() > 0)
		maxfo = heap.Pop(h).(*FileObject)
		maxatime = maxfo.atime
		bytecnt -= maxfo.size
		if glog.V(3) {
			glog.Infof("1B: bytecnt %d heap len %d", bytecnt, h.Len())
		}

		// Push object into heap iff older
		if atime.Before(maxatime) {
			heap.Push(h, item)
			bytecnt += stat.Size
			if glog.V(3) {
				glog.Infof("1C: bytecnt %d len %d", bytecnt, h.Len())
			}

			// Get atime of max-heap file object
			maxfo = heap.Pop(h).(*FileObject)
			bytecnt -= maxfo.size
			if glog.V(3) {
				glog.Infof("1D: bytecnt %d len %d", bytecnt, h.Len())
			}
			maxatime = maxfo.atime
			if glog.V(3) {
				glog.Infof("1E: file %q atime %v maxfo.path %q maxatime %v",
					file, atime, maxfo.path, maxatime)
			}
		}
	}
	heapelecnt := h.Len()
	if glog.V(3) {
		glog.Infof("max-heap size %d bytecnt %d toevict %d filecnt %d",
			heapelecnt, bytecnt, toevict, filecnt)
	}
	// delete some files
	var bevicted, fevicted int64
	for heapelecnt > 0 && bytecnt > 0 {
		maxfo = heap.Pop(h).(*FileObject)
		heapelecnt--
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
