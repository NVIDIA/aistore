/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package lru provides atime-based least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orhaned workfiles.
package lru

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/ios"
	"github.com/NVIDIA/dfcpub/stats"
)

// contextual LRU "jogger" traverses a given (cloud/ or local/) subdirectory to
// evict or delete selected objects and workfiles

func (lctx *lructx) jog(wg *sync.WaitGroup) {
	defer wg.Done()

	lctx.heap = &fileInfoMinHeap{}
	heap.Init(lctx.heap)
	if err := lctx.evictSize(); err != nil {
		return
	}
	if lctx.totsize < minevict {
		glog.Infof("%s: below threshold, nothing to do", lctx.bucketdir)
		return
	}
	glog.Infof("%s: evicting %s", lctx.bucketdir, cmn.B2S(lctx.totsize, 2))

	if err := filepath.Walk(lctx.bucketdir, lctx.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", lctx.bucketdir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", lctx.bucketdir, err)
		}
		return
	}
	if err := lctx.evict(); err != nil {
		glog.Errorf("%s: failed to evict, err: %v", lctx.bucketdir, err)
	}
}

func (lctx *lructx) walk(fqn string, osfi os.FileInfo, err error) error {
	var (
		evictOK, isOld bool
		xlru, h        = lctx.ini.Xlru, lctx.heap
	)
	if err != nil {
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if evictOK, isOld = lctx.ctxResolver.PermToEvict(fqn); !evictOK && !isOld {
		return nil
	}
	lctx.throttler.Sleep()

	_, err = os.Stat(fqn)
	if os.IsNotExist(err) {
		glog.Infof("Warning (race?): %s "+cmn.DoesNotExist, fqn)
		glog.Flush()
		return nil
	}
	// abort?
	select {
	case <-xlru.ChanAbort():
		s := fmt.Sprintf("%s aborted, exiting", xlru)
		glog.Infoln(s)
		glog.Flush()
		return errors.New(s)
	case <-time.After(time.Millisecond):
		break
	}
	if xlru.Finished() {
		return fmt.Errorf("%s aborted, exiting", xlru)
	}

	atime, mtime, stat := ios.GetAmTimes(osfi)
	if isOld {
		fi := &fileInfo{fqn: fqn, size: stat.Size}
		lctx.oldwork = append(lctx.oldwork, fi) // TODO: upper-limit to avoid OOM; see Push as well
		return nil
	}

	// object eviction: access time
	usetime := atime

	atimeResponse := <-lctx.ini.Ratime.Atime(fqn, lctx.atimeRespCh)
	accessTime, ok := atimeResponse.AccessTime, atimeResponse.Ok
	if ok {
		usetime = accessTime
	} else if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-cmn.GCO.Get().LRU.DontEvictTime)
	if usetime.After(dontevictime) {
		if glog.V(4) {
			glog.Infof("%s: not evicting (usetime %v, dontevictime %v)", fqn, usetime, dontevictime)
		}
		return nil
	}

	// cleanup after rebalance
	_, _, _, err = cluster.ResolveFQN(fqn, lctx.ini.Bmdowner)
	if err != nil {
		glog.Infof("%s: is misplaced, err: %v", fqn, err)
		fi := &fileInfo{fqn: fqn, size: stat.Size}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// partial optimization:
	// do nothing if the heap's cursize >= totsize &&
	// the file is more recent then the the heap's newest
	// full optimization (TODO) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && usetime.After(lctx.newest) {
		if glog.V(4) {
			glog.Infof("%s: use-time-after (usetime=%v, newest=%v)", fqn, usetime, lctx.newest)
		}
		return nil
	}
	// push and update the context
	fi := &fileInfo{fqn: fqn, usetime: usetime, size: stat.Size}
	heap.Push(h, fi)
	lctx.cursize += fi.size
	if usetime.After(lctx.newest) {
		lctx.newest = usetime
	}
	return nil
}

func (lctx *lructx) evict() error {
	var (
		fevicted, bevicted int64
		h                  = lctx.heap
	)
	for _, fi := range lctx.oldwork {
		if lctx.ini.Targetif.IsRebalancing() {
			_, _, _, err := cluster.ResolveFQN(fi.fqn, lctx.ini.Bmdowner)
			// keep a copy of a rebalanced file while rebalance is running
			if movable := lctx.ctxResolver.PermToMove(fi.fqn); movable && err != nil {
				continue
			}
		}
		if err := os.Remove(fi.fqn); err != nil {
			glog.Warningf("LRU: failed to GC %q", fi.fqn)
			continue
		}
		lctx.totsize -= fi.size
		glog.Infof("LRU: GC-ed %q", fi.fqn)
	}
	for h.Len() > 0 && lctx.totsize > 0 {
		fi := heap.Pop(h).(*fileInfo)
		if err := lctx.evictFQN(fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		lctx.totsize -= fi.size
		bevicted += fi.size
		fevicted++
	}
	lctx.ini.Statsif.Add(stats.LruEvictSize, bevicted)
	lctx.ini.Statsif.Add(stats.LruEvictCount, fevicted)
	return nil
}

// evictFQN evicts a given file
func (lctx *lructx) evictFQN(fqn string) error {
	_, bucket, objname, err := cluster.ResolveFQN(fqn, lctx.ini.Bmdowner)
	if err != nil {
		glog.Errorf("Evicting %q with error: %v", fqn, err)
		if e := os.Remove(fqn); e != nil {
			return fmt.Errorf("nested error: %v and %v", err, e)
		}
		glog.Infof("LRU: removed %q", fqn)
		return nil
	}
	uname := cluster.Uname(bucket, objname)
	lctx.ini.Namelocker.Lock(uname, true)
	defer lctx.ini.Namelocker.Unlock(uname, true)

	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("LRU: evicted %s/%s", bucket, objname)
	return nil
}

func (lctx *lructx) evictSize() (err error) {
	config := cmn.GCO.Get()
	hwm, lwm := config.LRU.HighWM, config.LRU.LowWM
	blocks, bavail, bsize, err := ios.GetFSStats(lctx.bucketdir)
	if err != nil {
		return err
	}
	used := blocks - bavail
	usedpct := used * 100 / blocks
	if glog.V(4) {
		glog.Infof("%s: Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%",
			lctx.bucketdir, blocks, bavail, usedpct, hwm, lwm)
	}
	if usedpct < uint64(hwm) {
		return
	}
	lwmblocks := blocks * uint64(lwm) / 100
	lctx.totsize = int64(used-lwmblocks) * bsize
	return
}

//=======================================================================
//
// fileInfoMinHeap keeps fileInfo sorted by access time with oldest
// on top of the heap.
//
//=======================================================================
func (h fileInfoMinHeap) Len() int { return len(h) }

func (h fileInfoMinHeap) Less(i, j int) bool {
	return h[i].usetime.Before(h[j].usetime)
}

func (h fileInfoMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *fileInfoMinHeap) Push(x interface{}) {
	*h = append(*h, x.(*fileInfo))
}

func (h *fileInfoMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	fi := old[n-1]
	*h = old[0 : n-1]
	return fi
}
