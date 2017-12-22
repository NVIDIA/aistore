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

var fileList []fileinfo
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
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(mntpath, &fs)
	if err != nil {
		glog.Errorf("Failed to statfs mp %q, err: %v", mntpath, err)
		return err
	}
	// Bfree is not equal to Bavail and Used blocks are (TotalBlks- Availableblks)
	glog.Infof("Total blocks %v free blocks %v avail blocks %v", fs.Blocks, fs.Bfree, fs.Bavail)
	// in terms of block
	used := fs.Blocks - fs.Bavail

	// FS is used less than HighWaterMark, nothing needs to be done.
	if (used * 100 / fs.Blocks) < uint64(hwm) {
		// Do nothing
		glog.Infof("mp %q used %d hwm %d", mntpath, used*100/fs.Blocks, hwm)
		return nil
	}
	desiredusedblks := fs.Blocks * uint64(lwm) / 100
	tobedeletedblks := used - desiredusedblks
	bytestodel := tobedeletedblks * uint64(fs.Bsize)
	glog.Infof("Total blocks %v Used blocks %v  desiredusedblks %v blocks to be freed %v bytes %v",
		fs.Blocks, used, desiredusedblks, tobedeletedblks, bytestodel)

	// if FileSystem's Used block are more than hwm(%), delete files to bring
	// FileSystem's Used block equal to lwm.

	if glog.V(3) {
		glog.Infof("Bytetodelete = %v ", bytestodel)
	}

	fileList = make([]fileinfo, 256)
	fileList = fileList[:0]
	err = filepath.Walk(mntpath, walkfunc)

	if err != nil {
		glog.Errorf("Failed to traverse all files in dir %q, err: %v", mntpath, err)
		return err
	}
	err = doMaxAtimeHeapAndDelete(bytestodel)
	if err != nil {
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
	// Skip special files starting with .
	if strings.HasPrefix(path, ".") || fi.Mode().IsDir() {
		glog.Infof("Skipping path = %s ", path)
	} else {
		var obj fileinfo
		obj.file = path
		obj.stat = fi.Sys().(*syscall.Stat_t)
		fileList = append(fileList, obj)
	}
	return nil
}

func doMaxAtimeHeapAndDelete(bytestodel uint64) error {

	h := &PriorityQueue{}
	heap.Init(h)

	var evictCurrBytes, evictDesiredBytes int64
	evictDesiredBytes = int64(bytestodel)
	var maxatime time.Time
	var maxfo *FileObject
	var filecnt uint64
	for _, fo := range fileList {
		filecnt++
		file := fo.file
		stat := fo.stat
		atime := time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
		item := &FileObject{
			path: file, size: stat.Size, atime: atime, index: 0}

		if evictCurrBytes < evictDesiredBytes {
			heap.Push(h, item)
			evictCurrBytes += stat.Size
			if glog.V(3) {
				glog.Infof("1A: evictCurrBytes %v currentpath %s atime %v",
					evictCurrBytes, file, atime)
			}
			continue
		}
		// Print error if no heap entry and break
		// TODO ASSERT
		if h.Len() == 0 {
			glog.Errorf("evictCurrBytes: %v evictDesiredBytes: %v ", evictCurrBytes, evictDesiredBytes)
			break
		}

		// Find Maxheap element for comparision with next set of incoming file object.
		maxfo = heap.Pop(h).(*FileObject)
		maxatime = maxfo.atime
		evictCurrBytes -= maxfo.size
		if glog.V(3) {
			glog.Infof("1B: curheapsize %v len %v", evictCurrBytes, h.Len())
		}

		// Push object into heap only if current fileobject's atime is lower than Maxheap element.
		if atime.Before(maxatime) {
			heap.Push(h, item)
			evictCurrBytes += stat.Size

			if glog.V(3) {
				glog.Infof("1C: curheapsize %d len %d", evictCurrBytes, h.Len())
			}

			// Get atime of Maxheap fileobject
			maxfo = heap.Pop(h).(*FileObject)
			evictCurrBytes -= maxfo.size
			if glog.V(3) {
				glog.Infof("1D: curheapsize %d len %d", evictCurrBytes, h.Len())
			}
			maxatime = maxfo.atime
			if glog.V(3) {
				glog.Infof("1C: current path %q atime %v maxfo.path %q maxatime %v",
					file, atime, maxfo.path, maxatime)
			}
		}
	}

	heapelecnt := h.Len()
	if glog.V(3) {
		glog.Infof("max-heap size %d evictCurrBytes %d evictDesiredBytes %d filecnt %d",
			heapelecnt, evictCurrBytes, evictDesiredBytes, filecnt)
	}
	for heapelecnt > 0 && evictCurrBytes > 0 {
		maxfo = heap.Pop(h).(*FileObject)
		evictCurrBytes -= maxfo.size
		if glog.V(3) {
			glog.Infof("1E: curheapsize %d len %d", evictCurrBytes, h.Len())
		}
		heapelecnt--
		err := os.Remove(maxfo.path)
		// FIXME: may fail to reach the "desired" target
		if err != nil {
			glog.Errorf("Failed to delete file %q, err: %v", maxfo.path, err)
			continue
		}
		atomic.AddInt64(&stats.bytesevicted, maxfo.size)
		atomic.AddInt64(&stats.filesevicted, 1)
	}
	// delete fileList
	fileList = fileList[:0]

	return nil
}
