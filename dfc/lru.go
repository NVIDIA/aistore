// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

type maxheap []*fileinfo

type lructx struct {
	cursize int64
	totsize int64
	newest  time.Time
	xlru    *xactLRU
	h       *maxheap
	t       *targetrunner
}

// FIXME: mountpath.enabled is never used
func (t *targetrunner) runLRU() {
	// FIXME: if LRU config has changed we need to force new LRU transaction
	xlru := t.xactinp.renewLRU(t)
	if xlru == nil {
		return
	}
	fschkwg := &sync.WaitGroup{}

	glog.Infof("LRU: %s started: dont-evict-time %v", xlru.tostring(), ctx.config.LRUConfig.DontEvictTime)
	for mpath := range ctx.mountpaths {
		fschkwg.Add(1)
		go t.oneLRU(mpath+"/"+ctx.config.LocalBuckets, fschkwg, xlru)
	}
	fschkwg.Wait()
	for mpath := range ctx.mountpaths {
		fschkwg.Add(1)
		go t.oneLRU(mpath+"/"+ctx.config.CloudBuckets, fschkwg, xlru)
	}
	fschkwg.Wait()

	// final check
	rr := getstorstatsrunner()
	rr.updateCapacity()

	for mpath := range ctx.mountpaths {
		fscapacity := rr.Capacity[mpath]
		if fscapacity.Usedpct > ctx.config.LRUConfig.LowWM+1 {
			glog.Warningf("LRU mpath %s: failed to reach lwm %d%% (used %d%%)", mpath, ctx.config.LRUConfig.LowWM, fscapacity.Usedpct)
		}
	}
	xlru.etime = time.Now()
	glog.Infoln(xlru.tostring())
	t.xactinp.del(xlru.id)
}

// TODO: local-buckets-first LRU policy
func (t *targetrunner) oneLRU(bucketdir string, fschkwg *sync.WaitGroup, xlru *xactLRU) {
	defer fschkwg.Done()
	h := &maxheap{}
	heap.Init(h)

	toevict, err := get_toevict(bucketdir, ctx.config.LRUConfig.HighWM, ctx.config.LRUConfig.LowWM)
	if err != nil {
		return
	}
	glog.Infof("LRU %s: to evict %.2f MB", bucketdir, float64(toevict)/1000/1000)

	// init LRU context
	lctx := &lructx{totsize: toevict, xlru: xlru, h: h, t: t}

	if err = filepath.Walk(bucketdir, lctx.lruwalkfn); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %q traversal: %s", bucketdir, s)
		} else {
			glog.Errorf("Failed to traverse %q, err: %v", bucketdir, err)
		}
		return
	}
	if err := t.doLRU(toevict, bucketdir, lctx); err != nil {
		glog.Errorf("doLRU %q, err: %v", bucketdir, err)
	}
}

// the walking callback is execited by the LRU xaction
// (notice the receiver)
func (lctx *lructx) lruwalkfn(fqn string, osfi os.FileInfo, err error) error {
	xlru, h := lctx.xlru, lctx.h
	if err != nil {
		glog.Errorf("walkfunc callback invoked with err: %v", err)
		return err
	}
	// skip system files and directories
	if strings.HasPrefix(osfi.Name(), ".") || osfi.Mode().IsDir() {
		return nil
	}
	_, err = os.Stat(fqn)
	if os.IsNotExist(err) {
		glog.Infof("Warning (LRU race?): %s does not exist", fqn)
		glog.Flush()
		return nil
	}
	// abort?
	select {
	case <-xlru.abrt:
		s := fmt.Sprintf("%s aborted, exiting lruwalkfn", xlru.tostring())
		glog.Infoln(s)
		glog.Flush()
		return errors.New(s)
	case <-time.After(time.Millisecond):
		break
	}
	if xlru.finished() {
		return fmt.Errorf("%s aborted - exiting lruwalkfn", xlru.tostring())
	}

	atime, mtime, stat := get_amtimes(osfi)
	usetime := atime
	if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-ctx.config.LRUConfig.DontEvictTime)
	if usetime.After(dontevictime) {
		if glog.V(3) {
			glog.Infof("DEBUG: not evicting %s (usetime %v, dontevictime %v)", fqn, usetime, dontevictime)
		}
		return nil
	}
	// partial optimization:
	// 	do nothing if the heap's cursize >= totsize &&
	// 	the file is more recent then the the heap's newest
	// full optimization (tbd) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && usetime.After(lctx.newest) {
		if glog.V(3) {
			glog.Infof("DEBUG: use-time-after (usetime=%v, newest=%v) %s", usetime, lctx.newest, fqn)
		}
		return nil
	}
	// push and update the context
	fi := &fileinfo{
		fqn:     fqn,
		usetime: usetime,
		size:    stat.Size,
	}
	heap.Push(h, fi)
	lctx.cursize += fi.size
	if usetime.After(lctx.newest) {
		lctx.newest = usetime
	}
	return nil
}

func (t *targetrunner) doLRU(toevict int64, bucketdir string, lctx *lructx) error {
	h := lctx.h
	var (
		fevicted, bevicted int64
	)
	for h.Len() > 0 && toevict > 10 {
		fi := heap.Pop(h).(*fileinfo)
		if err := t.lrufilRemove("lru", fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		if glog.V(3) {
			glog.Infof("LRU %s: removed %q", bucketdir, fi.fqn)
		}
		toevict -= fi.size
		bevicted += fi.size
		fevicted++
	}
	if ctx.rg != nil { // FIXME: for *_test only
		stats := getstorstats()
		stats.add("bytesevicted", bevicted)
		stats.add("filesevicted", fevicted)
	}
	return nil
}

func (t *targetrunner) lrufilRemove(prefix, fqn string) error {
	bucket, objname, ok := t.fqn2bckobj(fqn)
	if !ok {
		glog.Errorf("Cannot convert (%q => bucket %s, object %s) - fspath config changed?", fqn, bucket, objname)
		glog.Errorf("Evicting %q anyway...", fqn)
		if err := os.Remove(fqn); err != nil {
			return err
		}
		glog.Infof("%s: removed %q", prefix, fqn)
		return nil
	}
	uname := bucket + objname
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("lru: removed %q", fqn)
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
