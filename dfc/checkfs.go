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
	glog.Infof("fsscan mp %q", mntpath)
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(mntpath, &fs)
	if err != nil {
		glog.Errorf("Failed to statfs mp %q, err: %v", mntpath, err)
		return err
	}
	glog.Infof("Used blocks %d free blocks %d", fs.Blocks, fs.Bfree)
	// in terms of block
	used := fs.Blocks - fs.Bfree
	hwm := ctx.config.Cache.FSHighWaterMark
	lwm := ctx.config.Cache.FSLowWaterMark

	// FS is used less than HighWaterMark, nothing needs to be done.
	if (used * 100 / fs.Blocks) < uint64(hwm) {
		// Do nothing
		glog.Infof("mp %q used %d% hwm %d", mntpath, used*100/fs.Blocks, hwm)
		return nil
	}

	// if FileSystem's Used block are more than hwm(%), delete files to bring
	// FileSystem's Used block equal to lwm.
	desiredblks := fs.Blocks * uint64(lwm) / 100
	tobedeletedblks := used - desiredblks
	bytestodel := tobedeletedblks * uint64(fs.Bsize)
	if glog.V(4) {
		glog.Infof("Used blocks %d blocks to be freed %d bytes %d",
			fs.Blocks, desiredblks, tobedeletedblks, bytestodel)
	}
	fileList := []string{}

	_ = filepath.Walk(mntpath, func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, path)
		return nil
	})
	if err != nil {
		glog.Fatalf("Failed to traverse all files in dir %q, err: %v", mntpath, err)
		return err
	}
	h := &PriorityQueue{}
	heap.Init(h)

	var evictCurrBytes, evictDesiredBytes int64
	evictDesiredBytes = int64(bytestodel)
	var maxatime time.Time
	var maxfo *FileObject
	var filecnt uint64
	for _, file := range fileList {
		filecnt++

		// Skip special files starting with .
		if strings.HasPrefix(file, ".") {
			continue
		}
		fi, err := os.Stat(file)
		if err != nil {
			glog.Errorf("Failed to do stat on %s error = %v \n", file, err)
			continue
		}
		switch mode := fi.Mode(); {
		case mode.IsRegular():
			// do file stuff
			stat := fi.Sys().(*syscall.Stat_t)
			atime := time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
			item := &FileObject{
				path: file, size: stat.Size, atime: atime, index: 0}

			// Heapsize refers to total size of objects into heap.
			// Insert into heap until evictDesiredBytes
			if evictCurrBytes < evictDesiredBytes {
				heap.Push(h, item)
				evictCurrBytes += stat.Size
				if glog.V(4) {
					glog.Infof("1A: evictCurrBytes %v currentpath %s atime %v",
						evictCurrBytes, file, atime)
				}
				break
			}
			// Find Maxheap element for comparision with next set of incoming file object.
			maxfo = heap.Pop(h).(*FileObject)
			maxatime = maxfo.atime
			evictCurrBytes -= maxfo.size
			if glog.V(4) {
				glog.Infof("1B: curheapsize %v len %v", evictCurrBytes, h.Len())
			}

			// Push object into heap only if current fileobject's atime is lower than Maxheap element.
			if atime.Before(maxatime) {
				heap.Push(h, item)
				evictCurrBytes += stat.Size

				if glog.V(4) {
					glog.Infof("1C: curheapsize %d len %d", evictCurrBytes, h.Len())
				}

				// Get atime of Maxheap fileobject
				maxfo = heap.Pop(h).(*FileObject)
				evictCurrBytes -= maxfo.size
				if glog.V(4) {
					glog.Infof("1D: curheapsize %d len %d", evictCurrBytes, h.Len())
				}
				maxatime = maxfo.atime
				if glog.V(4) {
					glog.Infof("1C: current path %q atime %v maxfo.path %q maxatime %v",
						file, atime, maxfo.path, maxatime)
				}
			}

		case mode.IsDir():
			if glog.V(4) {
				glog.Infof("%q is a directory, skipping", file)
			}
			continue
		default:
			continue
		}

	}
	heapelecnt := h.Len()
	if glog.V(4) {
		glog.Infof("max-heap size %d evictCurrBytes %d evictDesiredBytes %d filecnt %d",
			heapelecnt, evictCurrBytes, evictDesiredBytes, filecnt)
	}
	for heapelecnt > 0 && evictCurrBytes > 0 {
		maxfo = heap.Pop(h).(*FileObject)
		evictCurrBytes -= maxfo.size
		if glog.V(4) {
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
	return nil
}
