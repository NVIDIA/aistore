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

// types
type fileinfo struct {
	fqn     string
	usetime time.Time
	size    int64
	index   int
}

type lructx struct {
	cursize int64
	totsize int64
	newest  time.Time
}

type maxheap []*fileinfo

// vars
var maxheapmap = make(map[string]*maxheap)
var lructxmap = make(map[string]*lructx)
var lrucas int64

// FIXME: mountpath.enabled is never used
func all_LRU() {
	if !atomic.CompareAndSwapInt64(&lrucas, 0, 7890123) {
		glog.Infoln("all_LRU is already running")
		return
	}
	mntcnt := len(ctx.mountpaths)
	fschkwg := &sync.WaitGroup{}
	fsmap := make(map[syscall.Fsid]bool, mntcnt)
	glog.Infof("all_LRU start, num mp-s %d", mntcnt)
	for _, mountpath := range ctx.mountpaths {
		_, ok := fsmap[mountpath.Fsid]
		if ok {
			glog.Infof("all_LRU: duplicate FSID %v, mpath %q", mountpath.Fsid, mountpath.Path)
			continue
		}
		fsmap[mountpath.Fsid] = true
		fschkwg.Add(1)
		go one_LRU(mountpath.Path, fschkwg)
	}
	fschkwg.Wait()
	glog.Infoln("all_LRU done")
	swapped := atomic.CompareAndSwapInt64(&lrucas, 7890123, 0)
	assert(swapped)
}

func one_LRU(mpath string, fschkwg *sync.WaitGroup) error {
	defer fschkwg.Done()
	hwm := ctx.config.Cache.FSHighWaterMark
	lwm := ctx.config.Cache.FSLowWaterMark

	h := &maxheap{}
	heap.Init(h)
	maxheapmap[mpath] = h

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

	// init LRU context
	lructxmap[mpath] = &lructx{totsize: toevict}
	defer func() { maxheapmap[mpath], lructxmap[mpath] = nil, nil }() // GC

	if err := filepath.Walk(mpath, walkfunc); err != nil {
		glog.Errorf("Failed to traverse mpath %q, err: %v", mpath, err)
		return err
	}

	if err := do_LRU(toevict, mpath); err != nil {
		glog.Errorf("Error do_LRU mpath %q, err: %v", mpath, err)
		return err
	}
	return nil
}

func walkfunc(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("walkfunc callback invoked with err: %v", err)
		return err
	}
	// skip system files and directories
	if strings.HasPrefix(osfi.Name(), ".") || osfi.Mode().IsDir() {
		return nil
	}
	stat := osfi.Sys().(*syscall.Stat_t)
	atime := time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
	mtime := time.Unix(int64(stat.Mtim.Sec), int64(stat.Mtim.Nsec))
	// atime controversy, see e.g. https://en.wikipedia.org/wiki/Stat_(system_call)#Criticism_of_atime
	usetime := atime
	if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-ctx.config.Cache.DontEvictTime)
	if usetime.After(dontevictime) {
		return nil
	}
	var (
		h *maxheap
		c *lructx
	)
	for mpath, hh := range maxheapmap {
		rel, err := filepath.Rel(mpath, fqn)
		if err == nil && !strings.HasPrefix(rel, "../") {
			h = hh
			c = lructxmap[mpath]
			break
		}
	}
	assert(h != nil && c != nil)
	// partial optimization:
	// 	do nothing if the heap's cursize >= totsize &&
	// 	the file is more recent then the the heap's newest
	// full optimization (tbd) entails compacting the heap when its cursize >> totsize
	if c.cursize >= c.totsize && usetime.After(c.newest) {
		return nil
	}
	// push and update the context
	fi := &fileinfo{
		fqn:     fqn,
		usetime: usetime,
		size:    stat.Size,
	}
	heap.Push(h, fi)
	c.cursize += fi.size
	if usetime.After(c.newest) {
		c.newest = usetime
	}
	return nil
}

func do_LRU(toevict int64, mpath string) error {
	h := maxheapmap[mpath]
	var (
		fevicted, bevicted int64
		cnt                int
	)
	for h.Len() > 0 && toevict > 10 {
		fi := heap.Pop(h).(*fileinfo)
		if err := os.Remove(fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		toevict -= fi.size
		bevicted += fi.size
		fevicted++
		cnt++
		if cnt >= 10 { // check the space every so often to avoid overshooting
			cnt = 0
			statfs := syscall.Statfs_t{}
			if err := syscall.Statfs(mpath, &statfs); err == nil {
				u := (statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks
				if u <= uint64(ctx.config.Cache.FSLowWaterMark)+1 {
					break
				}
			}
		}
	}
	if ctx.rg != nil { // FIXME: for *_test only
		stats := getstorstats()
		statsAdd(&stats.Bytesevicted, bevicted)
		statsAdd(&stats.Filesevicted, fevicted)
	}
	// final check
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(mpath, &statfs); err == nil {
		u := (statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks
		if u > uint64(ctx.config.Cache.FSLowWaterMark)+1 {
			glog.Errorf("Failed to reach lwm %d for mpath %q: used %d%% rem-toevict %d",
				ctx.config.Cache.FSLowWaterMark, mpath, u, toevict)
		}
	}
	return nil
}

//===========================================================================
//
// max-heap
//
//===========================================================================
func (mh maxheap) Len() int { return len(mh) }

func (mh maxheap) Less(i, j int) bool {
	return mh[i].usetime.Before(mh[j].usetime)
}

func (mh maxheap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].index = i
	mh[j].index = j
}

func (mh *maxheap) Push(x interface{}) {
	n := len(*mh)
	fi := x.(*fileinfo)
	fi.index = n
	*mh = append(*mh, fi)
}

func (mh *maxheap) Pop() interface{} {
	old := *mh
	n := len(old)
	fi := old[n-1]
	fi.index = -1
	*mh = old[0 : n-1]
	return fi
}
