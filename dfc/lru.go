// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

type fileInfo struct {
	fqn     string
	usetime time.Time
	size    int64
}

type fileInfoMinHeap []*fileInfo

type lructx struct {
	cursize                    int64
	totsize                    int64
	newest                     time.Time
	xlru                       *xactLRU
	heap                       *fileInfoMinHeap
	oldwork                    []*fileInfo
	t                          *targetrunner
	currentThrottleSleepInMS   float32
	previousDiskUtilPercentage float32
	fsUsedPercentage           uint64
	lastDiskUtilCheckTime      time.Time
	lastFSCapacityCheckTime    time.Time
}

const (
	InitialThrottleSleepDurationInMS = 1
	FSCapacityCheckDuation           = 1 * time.Second
)

func (t *targetrunner) runLRU() {
	// FIXME: if LRU config has changed we need to force new LRU transaction
	xlru := t.xactinp.renewLRU(t)
	if xlru == nil {
		return
	}
	fschkwg := &sync.WaitGroup{}

	glog.Infof("LRU: %s started: dont-evict-time %v", xlru.tostring(), ctx.config.LRU.DontEvictTime)
	for mpath := range ctx.mountpaths.Available {
		fschkwg.Add(1)
		go t.oneLRU(makePathLocal(mpath), fschkwg, xlru)
	}
	fschkwg.Wait()
	for mpath := range ctx.mountpaths.Available {
		fschkwg.Add(1)
		go t.oneLRU(makePathCloud(mpath), fschkwg, xlru)
	}
	fschkwg.Wait()

	// DEBUG
	if glog.V(4) {
		rr := getstorstatsrunner()
		rr.Lock()
		rr.updateCapacity()
		for mpath := range ctx.mountpaths.Available {
			fscapacity := rr.Capacity[mpath]
			if fscapacity.Usedpct > ctx.config.LRU.LowWM+1 {
				glog.Warningf("LRU mpath %s: failed to reach lwm %d%% (used %d%%)",
					mpath, ctx.config.LRU.LowWM, fscapacity.Usedpct)
			}
		}
		rr.Unlock()
	}

	xlru.etime = time.Now()
	glog.Infoln(xlru.tostring())
	t.xactinp.del(xlru.id)
}

// TODO: local-buckets-first LRU policy
func (t *targetrunner) oneLRU(bucketdir string, fschkwg *sync.WaitGroup, xlru *xactLRU) {
	defer fschkwg.Done()
	h := &fileInfoMinHeap{}
	heap.Init(h)

	toevict, err := getToEvict(bucketdir, ctx.config.LRU.HighWM, ctx.config.LRU.LowWM)
	if err != nil {
		return
	}
	glog.Infof("Initiating LRU for directory: %s. Need to evict: %.2f MB."+
		" [It is possible that less data gets evicted because of `dont_evict_time` setting in the config.]",
		bucketdir, float64(toevict)/MiB)

	// init LRU context
	var oldwork []*fileInfo
	lctx := &lructx{
		totsize: toevict,
		xlru:    xlru,
		heap:    h,
		oldwork: oldwork,
		t:       t,
	}

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
	sleepDuration, isThrottleRequired := lctx.getSleepDurationForThrottle(fqn,
		FSCapacityCheckDuation, getiostatrunner())
	glog.Infof("Fetched sleep duration for throttle for FQN [%s]. "+
		"Duration: [%v]. IsThrottleRequired: [%v]", fqn, sleepDuration, isThrottleRequired)
	if isThrottleRequired {
		time.Sleep(sleepDuration)
	}

	if err != nil {
		glog.Errorf("walkfunc callback invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	var (
		iswork, isold bool
		xlru, h       = lctx.xlru, lctx.heap
	)
	if iswork, isold = lctx.t.isworkfile(fqn); iswork {
		if !isold {
			return nil
		}
	}
	_, err = os.Stat(fqn)
	if os.IsNotExist(err) {
		glog.Infof("Warning (LRU race?): %s "+doesnotexist, fqn)
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

	atime, mtime, stat := getAmTimes(osfi)
	if isold {
		fi := &fileInfo{
			fqn:  fqn,
			size: stat.Size,
		}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// object eviction: access time
	usetime := atime

	aTimeRunner := getatimerunner()
	aTimeRunner.atime(fqn)
	accessTimeResponse := <-aTimeRunner.chSendAtime
	if accessTimeResponse.ok {
		usetime = accessTimeResponse.accessTime
	} else if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-ctx.config.LRU.DontEvictTime)
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
	fi := &fileInfo{
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
	h := lctx.heap
	var (
		fevicted, bevicted int64
	)
	for _, fi := range lctx.oldwork {
		if err := os.Remove(fi.fqn); err != nil {
			glog.Warningf("LRU: failed to GC %q", fi.fqn)
			continue
		}
		toevict -= fi.size
		glog.Infof("LRU: GC-ed %q", fi.fqn)
	}
	for h.Len() > 0 && toevict > 0 {
		fi := heap.Pop(h).(*fileInfo)
		if err := t.lruEvict(fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		toevict -= fi.size
		bevicted += fi.size
		fevicted++
	}
	t.statsif.add("bytesevicted", bevicted)
	t.statsif.add("filesevicted", fevicted)
	return nil
}

func (t *targetrunner) lruEvict(fqn string) error {
	bucket, objname, errstr := t.fqn2bckobj(fqn)
	if errstr != "" {
		glog.Errorln(errstr)
		glog.Errorf("Evicting %q anyway...", fqn)
		if err := os.Remove(fqn); err != nil {
			return err
		}
		glog.Infof("LRU: removed %q", fqn)
		return nil
	}
	uname := uniquename(bucket, objname)
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("LRU: evicted %s/%s", bucket, objname)
	return nil
}

// fileInfoMinHeap keeps fileInfo sorted by access time with oldest on top of the heap.
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

func (lctx *lructx) getSleepDurationForThrottle(fqn string,
	fsCapacityCheckDuration time.Duration, throttleChecker ThrottleChecker) (
	throttleDuration time.Duration, isThrottleRequired bool) {
	mountPath := getMountPathFromFilePath(fqn)
	if mountPath == "" {
		glog.Errorf("Unable to retrieve mount path from file path [%s]. Won't throttle.", fqn)
		return
	}

	if time.Now().After(lctx.lastFSCapacityCheckTime.Add(fsCapacityCheckDuration)) {
		usedFSPercentage, ok := throttleChecker.getFSUsedPercentage(fqn)
		if !ok {
			glog.Errorf("Unable to retrieve FS used capacity from mount path [%s]. Won't throttle.", mountPath)
			return
		}
		lctx.fsUsedPercentage = usedFSPercentage
		lctx.lastFSCapacityCheckTime = time.Now()
	}

	if lctx.fsUsedPercentage > uint64(ctx.config.LRU.HighWM) {
		return
	}

	currentDiskUtilPercentage := lctx.previousDiskUtilPercentage
	// Fetch disk utilization only after StatsTime has elapsed since
	// `iostatrunner` updates disk utilization only after StatsTime duration has
	// elapsed.
	if time.Now().After(lctx.lastDiskUtilCheckTime.Add(ctx.config.Periodic.StatsTime)) {
		diskUtilPercentage, ok := throttleChecker.getDiskUtilizationFromPath(fqn)
		if !ok {
			glog.Error("Unable to retrieve FS disk utilization. Won't throttle.")
			return
		}
		lctx.lastDiskUtilCheckTime = time.Now()
		if diskUtilPercentage < float32(ctx.config.Xaction.DiskUtilLowWM) {
			return
		}
		currentDiskUtilPercentage = diskUtilPercentage
	}

	if currentDiskUtilPercentage <= 1.05*lctx.previousDiskUtilPercentage &&
		currentDiskUtilPercentage >= 0.95*lctx.previousDiskUtilPercentage {
		glog.Infof("Current and previous utilization are same. [%d]. "+
			"Not updating throttling duration [%d].",
			currentDiskUtilPercentage, lctx.currentThrottleSleepInMS)
	} else if currentDiskUtilPercentage > float32(ctx.config.Xaction.DiskUtilHighWM) {
		// To avoid precision errors
		if lctx.currentThrottleSleepInMS < 0.001 {
			lctx.currentThrottleSleepInMS = InitialThrottleSleepDurationInMS
		} else {
			lctx.currentThrottleSleepInMS *= 2
			if lctx.currentThrottleSleepInMS > 1000 {
				lctx.currentThrottleSleepInMS = 1000
			}
		}
	} else {
		// To avoid precision errors
		if lctx.currentThrottleSleepInMS < 0.001 {
			lctx.currentThrottleSleepInMS = InitialThrottleSleepDurationInMS
		}
		multiplier := (currentDiskUtilPercentage - float32(ctx.config.Xaction.DiskUtilLowWM)) /
			float32(ctx.config.Xaction.DiskUtilHighWM-ctx.config.Xaction.DiskUtilLowWM)
		lctx.currentThrottleSleepInMS = lctx.currentThrottleSleepInMS + lctx.currentThrottleSleepInMS*multiplier
	}

	lctx.previousDiskUtilPercentage = currentDiskUtilPercentage
	// time.Duration truncates float to int. Convert to Microseconds and then back to handle float values.
	return time.Duration(lctx.currentThrottleSleepInMS*1000) * time.Microsecond, true
}
